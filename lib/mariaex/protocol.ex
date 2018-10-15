defmodule Mariaex.Protocol do
  @moduledoc false

  alias Mariaex.Cache
  alias Mariaex.LruCache
  alias Mariaex.Query
  alias Mariaex.Cursor
  alias Mariaex.Column
  import Mariaex.Messages
  import Mariaex.ProtocolHelper

  use DBConnection
  use Bitwise

  @timeout 5000
  @cache_size 100
  @max_rows 500
  @nonposix_errors [:closed, :timeout]

  @maxpacketbytes 50000000
  @mysql_native_password "mysql_native_password"
  @mysql_old_password :mysql_old_password

  @client_long_password     0x00000001
  @client_found_rows        0x00000002
  @client_long_flag         0x00000004
  @client_connect_with_db   0x00000008
  @client_local_files       0x00000080
  @client_protocol_41       0x00000200
  @client_ssl               0x00000800
  @client_transactions      0x00002000
  @client_secure_connection 0x00008000
  @client_multi_statements  0x00010000
  @client_multi_results     0x00020000
  @client_ps_multi_results  0x00040000
  @client_deprecate_eof     0x01000000

  @server_status_in_trans      0x0001
  @server_more_results_exists  0x0008
  @server_status_cursor_exists 0x0040
  @server_status_last_row_sent 0x0080

  @cursor_type_no_cursor 0x00
  @cursor_type_read_only 0x01

  @capabilities @client_long_password     ||| @client_found_rows        ||| @client_long_flag |||
                @client_local_files       ||| @client_protocol_41       ||| @client_transactions |||
                @client_secure_connection ||| @client_multi_statements  ||| @client_multi_results |||
                @client_ps_multi_results  ||| @client_deprecate_eof

  defstruct sock: nil,
            state: nil,
            state_data: nil,
            deprecated_eof: false,
            binary_as: nil,
            connection_id: nil,
            opts: [],
            catch_eof: false,
            buffer: "",
            timeout: 0,
            lru_cache: nil,
            cache: nil,
            cursors: %{},
            seqnum: 0,
            datetime: :structs,
            json_library: Poison,
            transaction_status: :idle,
            ssl_conn_state: :undefined  #  :undefined | :not_used | :ssl_handshake | :connected

  @doc """
  DBConnection callback
  """

  def connect(opts) do
    opts         = default_opts(opts)
    sock_type    = opts[:sock_type] |> Atom.to_string |> String.capitalize()
    sock_mod     = Module.concat(Mariaex.Connection, sock_type)
    {host, port} =
      case Keyword.fetch(opts, :socket) do
        {:ok, socket} ->
          {{:local, socket}, 0}
        :error ->
          {parse_host(opts[:hostname]), opts[:port]}
      end
    connect_opts = [host, port, opts[:socket_options], opts[:timeout]]
    binary_as    = opts[:binary_as] || :field_type_var_string
    datetime     = opts[:datetime] || :structs
    json_library = Application.get_env(:mariaex, :json_library, Poison)

    case apply(sock_mod, :connect, connect_opts) do
      {:ok, sock} ->
        s = %__MODULE__{binary_as: binary_as,
                        state: :handshake,
                        ssl_conn_state: set_initial_ssl_conn_state(opts),
                        connection_id: self(),
                        sock: {sock_mod, sock},
                        cache: reset_cache(),
                        lru_cache: reset_lru_cache(opts[:cache_size]),
                        timeout: opts[:timeout],
                        datetime: datetime,
                        json_library: json_library,
                        opts: opts}
        handshake_recv(s, %{opts: opts})
      {:error, reason} ->
        {:error, %Mariaex.Error{message: "tcp connect: #{reason}"}}
    end
  end

  def reset_cache do
    cache = Cache.new()
    case Process.put(:cache, cache) do
      nil -> nil
      tab -> :ets.delete(tab)
    end
    cache

  end

  def reset_lru_cache(cache_size) do
    lru_cache = LruCache.new(cache_size)
    case Process.put(:lru_cache, lru_cache) do
      nil -> nil
      {_size, tab} -> :ets.delete(tab)
    end
    lru_cache
  end


  defp default_opts(opts) do
    opts
    |> Keyword.put_new(:username, System.get_env("MDBUSER") || System.get_env("USER"))
    |> Keyword.put_new(:password, System.get_env("MDBPASSWORD"))
    |> Keyword.put_new(:hostname, System.get_env("MDBHOST") || "localhost")
    |> Keyword.put_new(:port, System.get_env("MDBPORT") || 3306)
    |> Keyword.put_new(:timeout, @timeout)
    |> Keyword.put_new(:cache_size, @cache_size)
    |> Keyword.put_new(:sock_type, :tcp)
    |> Keyword.put_new(:socket_options, [])
    |> Keyword.update!(:port, &normalize_port/1)
  end

  defp parse_host(host) do
    host = if is_binary(host), do: String.to_charlist(host), else: host

    case :inet.parse_strict_address(host) do
      {:ok, address} ->
        address
      _ ->
        host
    end
  end

  defp set_initial_ssl_conn_state(opts) do
    if opts[:ssl] && has_ssl_opts?(opts[:ssl_opts]) do
      :ssl_handshake
    else
      :not_used
    end
  end

  defp has_ssl_opts?(nil), do: false
  defp has_ssl_opts?([]), do: false
  defp has_ssl_opts?(ssl_opts) when is_list(ssl_opts), do: true

  defp normalize_port(port) when is_binary(port), do: String.to_integer(port)
  defp normalize_port(port) when is_integer(port), do: port

  defp handshake_recv(state, request) do
    case msg_recv(state) do
      {:ok, packet, state} ->
        case handle_handshake(packet, request, state) do
          {:error, error} ->
            do_disconnect(state, error, "") |> connected()
          other -> other
        end
      {:error, reason} ->
        {sock_mod, _} = state.sock
        Mariaex.Protocol.do_disconnect(state, {sock_mod.tag, "recv", reason, ""}) |> connected()
    end
  end

  defp connected({:disconnect, error, state}) do
    disconnect(error, state)
    {:error, error}
  end
  defp connected(other), do: other

  # request to communicate over an SSL connection
  defp handle_handshake(packet(seqnum: seqnum) = packet, opts, %{ssl_conn_state: :ssl_handshake} = s) do
    # Create and send an SSL request packet per the spec:
    # https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::SSLRequest
    msg = ssl_connection_request(capability_flags: ssl_capabilities(opts), max_size: @maxpacketbytes, character_set: 8)
    msg_send(msg, s, new_seqnum = seqnum + 1)
    case upgrade_to_ssl(s, opts) do
      {:ok, new_state} ->
        # move along to the actual handshake; now over SSL/TLS
        handle_handshake(packet(packet, seqnum: new_seqnum), opts, new_state)
      {:error, error} ->
        {:error, error}
    end
  end
  defp handle_handshake(packet(seqnum: seqnum,
                               msg: handshake(capability_flags_1: flag1,
                                              capability_flags_2: flag2,
                                              plugin: plugin) = handshake) = _packet,  %{opts: opts}, s) do
    <<flag :: size(32)>> = <<flag2 :: size(16), flag1 :: size(16)>>
    deprecated_eof = (flag &&& @client_deprecate_eof) == @client_deprecate_eof
    handshake(auth_plugin_data1: salt1, auth_plugin_data2: salt2) = handshake
    scramble = case password = opts[:password] do
      nil -> ""
      ""  -> ""
      _   -> password(plugin, password, <<salt1 :: binary, salt2 :: binary>>)
    end
    {database, capabilities} = capabilities(opts)
    msg = handshake_resp(username: :unicode.characters_to_binary(opts[:username]), password: scramble,
                         database: database, capability_flags: capabilities,
                         max_size: @maxpacketbytes, character_set: 8)
    msg_send(msg, s, seqnum + 1)
    handshake_recv(%{s | state: :handshake_send, deprecated_eof: deprecated_eof}, nil)
  end
  defp handle_handshake(packet(msg: ok_resp(affected_rows: _affected_rows, last_insert_id: _last_insert_id) = _packet), nil, state) do
    statement = "SET CHARACTER SET " <> (state.opts[:charset] || "utf8")
    query = %Query{type: :text, statement: statement}
    case send_text_query(state, statement) |> text_query_recv([], query) do
      {:error, error, _} ->
        {:error, error}
      {:ok, _, _, state} ->
        activate(state, state.buffer) |> connected()
    end
  end
  defp handle_handshake(packet, query, state) do
    {:error, error, _} = handle_error(packet, query, state)
    {:error, error}
  end

  defp upgrade_to_ssl(%{sock: {_sock_mod, sock}} = s, %{opts: opts}) do
    ssl_opts = opts[:ssl_opts]
    case :ssl.connect(sock, ssl_opts, opts[:timeout]) do
      {:ok, ssl_sock} ->
        # switch to the ssl connection module
        # set the socket
        # move ssl_conn_state to :connected
        {:ok, %{s | sock: {Mariaex.Connection.Ssl, ssl_sock}, ssl_conn_state: :connected}}
      {:error, reason} ->
        {:error, %Mariaex.Error{message: "failed to upgraded socket: #{inspect reason}"}}
    end
  end

  defp capabilities(opts) do
    case opts[:skip_database] do
      true -> {"", @capabilities}
      _    -> {opts[:database], @capabilities ||| @client_connect_with_db}
    end
  end

  defp ssl_capabilities(%{opts: opts}) do
    case opts[:skip_database] do
      true -> @capabilities ||| @client_ssl
      _    -> @capabilities ||| @client_connect_with_db ||| @client_ssl
    end
  end

  @doc """
  DBConnection callback
  """
  def disconnect(_, state = %{sock: {sock_mod, sock}}) do
    msg_send(text_cmd(command: com_quit(), statement: ""), state, 0)
    case msg_recv(state) do
      {:ok, packet(msg: ok_resp()), _state} ->
        sock_mod.close(sock)
      {:ok, packet(msg: _), _state} ->
        sock_mod.close(sock)
      {:error, _} ->
        sock_mod.close(sock)
    end
    _ = sock_mod.recv_active(sock, 0, "")
    :ok
  end

  @doc """
  DBConnection callback
  """
  def checkout(%{buffer: :active_once, sock: {sock_mod, sock}} = s) do
    case setopts(s, [active: :false], :active_once) do
      :ok                       -> sock_mod.recv_active(sock, 0, "") |> handle_recv_buffer(s)
      {:disconnect, _, _} = dis -> dis
    end
  end

  defp handle_recv_buffer({:ok, buffer}, s) do
    {:ok, %{s | buffer: buffer}}
  end
  defp handle_recv_buffer({:disconnect, description}, s) do
    do_disconnect(s, description)
  end

  @doc """
  DBConnection callback
  """
  def checkin(%{buffer: buffer} = s) when is_binary(buffer) do
    activate(s, buffer)
  end

  ## Fake [active: once] if buffer not empty
  defp activate(s, <<>>) do
    case setopts(s, [active: :once], <<>>) do
      :ok  -> {:ok, %{s | buffer: :active_once, state: :running}}
      other -> other
    end
  end
  defp activate(%{sock: {mod, sock}} = s, buffer) do
    msg = mod.fake_message(sock, buffer)
    send(self(), msg)
    {:ok, %{s | buffer: :active_once, state: :running}}
  end

  defp setopts(%{sock: {mod, sock}} = s, opts, buffer) do
    case mod.setopts(sock, opts) do
      :ok ->
        :ok
      {:error, reason} ->
        do_disconnect(s, {mod, "setopts", reason, buffer})
    end
  end

  @doc """
  DBConnection callback
  """
  def handle_prepare(%Query{type: nil} = query, opts, s) do
    case handle_prepare(%Query{query | type: :binary}, opts, s) do
      {:error,  %Mariaex.Error{mariadb: %{code: 1295}}, s} ->
        {:ok, %Query{query | type: :text, ref: make_ref(), num_params: 0}, s}
      other ->
        other
    end
  end
  def handle_prepare(%Query{type: :binary} = query, opts, %{binary_as: binary_as} = s) do
    case prepare_lookup(%Query{query | binary_as: binary_as}, s) do
      {:prepared, query} ->
        {:ok, query, s}
      {:prepare, query} ->
        prepare(opts, query, s)
      {:close_prepare, id, query} ->
        close_prepare(id, opts, query, s)
    end
  end
  def handle_prepare(%Query{type: _} = query, _, s) do
    error = ArgumentError.exception("query #{inspect query} is already prepared")
    {:error, error, s}
  end

  defp prepare_lookup(%Query{name: "", statement: statement} = query, %{lru_cache: cache}) do
    case LruCache.lookup(cache, statement) || LruCache.garbage_collect(cache) do
      {_id, ref, num_params} ->
        {:prepared, %{query | ref: ref, num_params: num_params}}
      id when is_integer(id) ->
        {:close_prepare, id, query}
      nil ->
        {:prepare, query}
    end
  end
  defp prepare_lookup(%Query{name: name} = query, %{cache: cache}) do
    case Cache.take(cache, name) do
      id when is_integer(id) ->
        {:close_prepare, id, query}
      nil ->
        {:prepare, query}
    end
  end

  defp prepare(opts, %Query{statement: statement} = query, s) do
    msg_send(text_cmd(command: com_stmt_prepare(), statement: statement), s, 0)
    prepare_recv(%{s | state: :prepare_send}, opts, query)
  end

  defp close_prepare(id, opts, %Query{statement: statement} = query, s) do
    msgs = [stmt_close(command: com_stmt_close(), statement_id: id),
            text_cmd(command: com_stmt_prepare(), statement: statement)]
    msg_send(msgs, s, 0)
    prepare_recv(s, opts, query)
  end

  defp prepare_recv(state, opts, query) do
    case prepare_recv(state, opts) do
      {:prepared, id, num_params, flags, state} ->
        {:ok, prepare_insert(id, num_params, query, state), clean_state(state, flags)}
      {:ok, packet, state} ->
        handle_error(packet, query, state)
      {:error, reason} ->
        recv_error(reason, state)
    end
  end

  defp prepare_recv(state, opts) do
    state = %{state | state: :prepare_send}
    with {:ok, packet(msg: stmt_prepare_ok(statement_id: id, num_columns: num_cols, num_params: num_params)), state} <- msg_recv(state, opts),
         {:eof, _, state} <- skip_definitions(state, opts, num_params),
         {:eof, flags, state} <- skip_definitions(state, opts, num_cols) do
      {:prepared, id, num_params, flags, state}
    end
  end

  defp skip_definitions(state, _opts, 0), do: {:eof, nil, state}
  defp skip_definitions(state, opts, count) do
    do_skip_definitions(%{state | state: :column_definitions}, opts, count)
  end

  defp do_skip_definitions(state, opts, rem) when rem > 0 do
    case msg_recv(state, opts) do
      {:ok, packet(msg: column_definition_41()), state} ->
        do_skip_definitions(state, opts, rem-1)
      other ->
        other
    end
  end
  defp do_skip_definitions(%{deprecated_eof: true} = state, _opts, 0) do
    {:eof, nil, state}
  end
  defp do_skip_definitions(%{deprecated_eof: false} = state, opts, 0) do
    case msg_recv(state, opts) do
      {:ok, packet(msg: eof_resp(status_flags: flags)), state} ->
        {:eof, flags, state}
      other ->
        other
    end
  end

  defp prepare_insert(id, num_params, %Query{name: "", statement: statement, ref: ref} = query, %{lru_cache: cache}) do
    ref = ref || make_ref()
    true = LruCache.insert_new(cache, statement, id, ref, num_params)
    %{query | ref: ref, num_params: num_params}
  end
  defp prepare_insert(id, num_params, %Query{name: name, ref: ref} = query, %{cache: cache}) do
    ref= ref || make_ref()
    true = Cache.insert_new(cache, name, id, ref)
    %{query | ref: ref, num_params: num_params}
  end

  @doc """
  DBConnection callback
  """
  def handle_execute(%Query{type: :text, statement: statement} = query, [], opts, state) do
    send_text_query(state, statement) |> text_query_recv(opts, query)
  end
  def handle_execute(%Query{type: :binary} = query, params, opts, state) do
    case execute_lookup(query, state) do
      {:execute, id, query} ->
        execute(id, query, params, state, opts)
      {:prepare_execute, query} ->
        prepare_execute(&prepare(opts, query, &1), params, state, opts)
      {:close_prepare_execute, id, query} ->
        prepare_execute(&close_prepare(id, opts, query, &1), params, state, opts)
    end
  end

  defp execute_lookup(%Query{name: ""} = query, %{lru_cache: cache}) do
    %Query{statement: statement, ref: ref} = query
    case LruCache.lookup(cache, statement) || LruCache.garbage_collect(cache) do
      {id, ^ref, _} ->
        {:execute, id, query}
      {id, _, _} ->
        LruCache.delete(cache, statement)
        {:close_prepare_execute, id, query}
      id when is_integer(id) ->
        {:close_prepare_execute, id, query}
      nil ->
        {:prepare_execute, query}
    end
  end
  defp execute_lookup(%Query{name: name, ref: ref} = query, %{cache: cache}) do
    case Cache.lookup(cache, name) do
      {id, ^ref} ->
        {:execute, id, query}
      {id, _} ->
        Cache.delete(cache, name)
        {:close_prepare_execute, id, query}
      nil ->
        {:prepare_execute, query}
    end
  end

  defp execute(id, query, params, state, opts) do
    msg_send(stmt_execute(command: com_stmt_execute(), parameters: params, statement_id: id, flags: @cursor_type_no_cursor, iteration_count: 1), state, 0)
    binary_query_recv(state, opts, query)
  end

  defp prepare_execute(prepare, params, state, opts) do
    case prepare.(state) do
      {:ok, query, state} ->
        id = prepare_execute_lookup(query, state)
        execute(id, query, params, state, opts)
      {err, _, _} = error when err in [:error, :disconnect] ->
        error
    end
  end

  defp prepare_execute_lookup(%Query{name: "", statement: statement}, %{lru_cache: cache}) do
    LruCache.id(cache, statement)
  end
  defp prepare_execute_lookup(%Query{name: name}, %{cache: cache}) do
    Cache.id(cache, name)
  end

  defp text_query_recv(state, opts, query) do
    case text_query_recv(state, opts) do
      {:resultset, columns, rows, flags, state} ->
        result = %Mariaex.Result{rows: rows, connection_id: state.connection_id}
        {:ok, query, {result, columns}, clean_state(state, flags)}
      {:ok, packet(msg: ok_resp()) = packet, state} ->
        {:ok, result, state} = handle_ok_packet(packet, query, state)
        {:ok, query, result, state}
      {:ok, packet, state} ->
        handle_error(packet, query, state)
      {:error, reason} ->
        recv_error(reason, state)
    end
  end

  defp text_query_recv(state, opts) do
    state = %{state | state: :column_count}
    with {:ok, packet(msg: column_count(column_count: num_cols)), state} <- msg_recv(state, opts),
         {:eof, columns, _, state} <- columns_recv(state, opts, num_cols),
         {:eof, rows, flags, state} <- text_rows_recv(state, columns) do
      {:resultset, columns, rows, flags, state}
    end
  end

  defp text_rows_recv(%{buffer: buffer} = state, columns) do
    fields = Mariaex.RowParser.decode_text_init(columns)
    case text_row_decode(%{state | buffer: :text_rows}, fields, [], buffer) do
      {:ok, packet(msg: eof_resp(status_flags: flags)), rows, state} ->
        {:eof, rows, flags, state}
      {:ok, packet, _, state} ->
        {:ok, packet, state}
      other ->
        other
    end
  end

  defp text_row_decode(%{datetime: datetime, json_library: json_library} = s, fields, rows, buffer) do
    case decode_text_rows(buffer, fields, rows, datetime, json_library) do
      {:ok, packet, rows, rest} ->
        {:ok, packet, rows, %{s | buffer: rest}}
      {:more, rows, rest} ->
        text_row_recv(s, fields, rows, rest)
    end
  end

  defp text_row_recv(s, fields, rows, buffer) do
    %{sock: {sock_mod, sock}, timeout: timeout} = s
    case sock_mod.recv(sock, 0, timeout) do
      {:ok, data} when buffer == "" ->
        text_row_decode(s, fields, rows, data)
      {:ok, data} ->
        text_row_decode(s, fields, rows, buffer <> data)
      {:error, _} = error ->
        error
    end
  end

  defp handle_error(packet(msg: error_resp(error_code: code, error_message: message)), query, state) do
    abort_statement(state, query, code, message)
  end

  defp binary_query_recv(state, opts, query) do
    case binary_query_recv(state, opts) do
      {:resultset, columns, bin_rows, flags, state} ->
        binary_query_resultset(state, opts, query, columns, bin_rows, flags)
      {:ok, packet(msg: ok_resp()) = packet, state} ->
        {:ok, result, state} = handle_ok_packet(packet, query, state)
        {:ok, query, result, state}
      {:ok, packet, state} ->
        handle_error(packet, query, state)
      {:error, reason} ->
        recv_error(reason, state)
    end
  end

  defp binary_query_recv(state, opts) do
    state = %{state | state: :column_count}
    with {:ok, packet(msg: column_count(column_count: num_cols)), state} <- msg_recv(state, opts),
         {:eof, columns, _, state} <- columns_recv(state, opts, num_cols),
         {:eof, rows, flags, state} <- bin_rows_recv(state, columns) do
      {:resultset, columns, rows, flags, state}
    end
  end

  defp columns_recv(state, opts, num_cols) do
    columns_recv(%{state | state: :column_definitions}, opts, num_cols, [])
  end

  defp columns_recv(state, opts, rem, columns) when rem > 0 do
    case msg_recv(state, opts) do
      {:ok, packet(msg: column_definition_41(type: type, name: name, flags: flags, table: table)), state} ->
        column = %Column{name: name, table: table, type: type, flags: flags}
        columns_recv(state, opts, rem-1, [column | columns])
      other ->
        other
    end
  end
  defp columns_recv(%{deprecated_eof: true} = state, _opts, 0, columns) do
    {:eof, Enum.reverse(columns), 0, state}
  end
  defp columns_recv(%{deprecated_eof: false} = state, opts, 0, columns) do
    case msg_recv(state, opts) do
      {:ok, packet(msg: eof_resp(status_flags: flags)), state} ->
        {:eof, Enum.reverse(columns), flags, state}
      other ->
        other
    end
  end

  defp bin_rows_recv(%{buffer: buffer} = state, columns) do
    {fields, nullbin_size} = Mariaex.RowParser.decode_init(columns)
    case binary_row_decode(%{state | buffer: :bin_rows}, fields, nullbin_size, [], buffer) do
      {:ok, packet(msg: eof_resp(status_flags: flags)), rows, state} ->
        {:eof, rows, flags, state}
      {:ok, packet, _, state} ->
        {:ok, packet, state}
      other ->
        other
    end
  end

  defp binary_query_resultset(state, opts, query, columns, rows, flags) do
    cond do
      (flags &&& @server_more_results_exists) == @server_more_results_exists ->
        binary_query_more(state, opts, query, columns, rows)
      true ->
        result = %Mariaex.Result{rows: rows, connection_id: state.connection_id}
        {:ok, query, {result, columns}, clean_state(state, flags)}
    end
  end

  defp binary_query_more(state, opts, query, columns, rows) do
    case msg_recv(state, opts) do
      {:ok, packet(msg: ok_resp(affected_rows: affected_rows, last_insert_id: last_insert_id, status_flags: flags)), state} ->
        result = %Mariaex.Result{rows: rows, num_rows: affected_rows,
          last_insert_id: last_insert_id, connection_id: state.connection_id}
        {:ok, query, {result, columns}, clean_state(state, flags)}
      {:ok, packet, state} ->
        handle_error(packet, query, state)
      {:error, reason} ->
        recv_error(reason, state)
    end
  end

  defp binary_row_decode(%{datetime: datetime, json_library: json_library} = s, fields, nullbin_size, rows, buffer) do
    case decode_bin_rows(buffer, fields, nullbin_size, rows, datetime, json_library) do
      {:ok, packet, rows, rest} ->
        {:ok, packet, rows, %{s | buffer: rest}}
      {:more, rows, rest} ->
        binary_row_recv(s, fields, nullbin_size, rows, rest)
    end
  end

  defp binary_row_recv(s, fields, nullbin_size, rows, buffer) do
    %{sock: {sock_mod, sock}, timeout: timeout} = s
    case sock_mod.recv(sock, 0, timeout) do
      {:ok, data} when buffer == "" ->
        binary_row_decode(s, fields, nullbin_size, rows, data)
      {:ok, data} ->
        binary_row_decode(s, fields, nullbin_size, rows, buffer <> data)
      {:error, _} = error ->
        error
    end
  end

  defp handle_ok_packet(packet(msg: ok_resp(affected_rows: affected_rows, last_insert_id: last_insert_id, status_flags: flags)), _query, s) do
    result = %Mariaex.Result{columns: [], rows: nil, num_rows: affected_rows,
      last_insert_id: last_insert_id, connection_id: s.connection_id}
    {:ok, {result, nil}, clean_state(s, flags)}
  end

  defp clean_state(state, flags) do
    status = transaction_status(state, flags)
    state = %{state | state: :running, state_data: nil, transaction_status: status}
    case status do
      :idle ->
        clean_cursors(state)
      :transaction ->
        state
    end
  end

  defp transaction_status(_, flags) when is_integer(flags) do
    case flags &&& @server_status_in_trans do
      @server_status_in_trans ->
        :transaction
      0 ->
        :idle
    end
  end
  defp transaction_status(%{transaction_status: status}, nil) do
    status
  end

  defp clean_cursors(%{cursors: cursors} = state) do
    for {_ref, {_status, id, _info}} <- cursors, is_integer(id) do
      msg_send(stmt_close(command: com_stmt_close(), statement_id: id), state, 0)
    end
    %{state | cursors: %{}}
  end

  @doc """
  DBConnection callback
  """
  def handle_close(%Query{type: :text}, _, s) do
    {:ok, nil, s}
  end
  def handle_close(%Query{type: :binary} = query, _, s) do
    case close_lookup(query, s) do
      {:close, id} ->
        msg_send(stmt_close(command: com_stmt_close(), statement_id: id), s, 0)
        {:ok, nil, s}
      :closed ->
        {:ok, nil, s}
    end
  end

  defp close_lookup(%Query{name: "", statement: statement}, %{lru_cache: cache}) do
    case LruCache.take(cache, statement) do
      id when is_integer(id) ->
        {:close, id}
      nil ->
        :closed
    end
  end
  defp close_lookup(%Query{name: name}, %{cache: cache}) do
    case Cache.take(cache, name) do
      id when is_integer(id) ->
        {:close, id}
      nil ->
        :closed
    end
  end

  def handle_declare(query, params, opts, state) do
    case declare_lookup(query, state) do
      {:declare, id} ->
        cursor = %Cursor{statement_id: id, ref: make_ref()}
        declare(cursor, query, params, state)
      {:prepare_declare, query} ->
        prepare_declare(&prepare(opts, query, &1), params, state)
      {:close_prepare_declare, id, query} ->
        prepare_declare(&close_prepare(id, opts, query, &1), params, state)
      {:text, _} ->
        cursor = %Cursor{statement_id: :text, ref: make_ref()}
        declare(cursor, query, params, state)
    end
  end

  defp declare_lookup(%Query{type: :text} = query, _), do: {:text, query}
  defp declare_lookup(%Query{name: "", statement: statement} = query, %{lru_cache: cache}) do
    case LruCache.take(cache, statement) do
      id when is_integer(id) ->
        {:declare, id}
      nil ->
        {:prepare_declare, query}
    end
  end
  defp declare_lookup(%Query{name: name, ref: ref} = query, %{cache: cache}) do
    case Cache.lookup(cache, name) do
      {id, ^ref} ->
        Cache.delete(cache, name)
        {:declare, id}
      {id, _} ->
        Cache.delete(cache, name)
        {:close_prepare_declare, id, query}
      nil ->
        {:prepare_declare, query}
    end
  end

  defp declare(%Cursor{ref: ref, statement_id: id} = cursor, query, params, state) do
    state = put_in(state.cursors[ref], {:first, id, params})
    # close cursor if idle
    {:ok, query, cursor, clean_state(state, nil)}
  end

  defp prepare_declare(prepare, params, state) do
    case prepare.(state) do
      {:ok, query, state} ->
        id = prepare_declare_lookup(query, state)
        cursor = %Cursor{statement_id: id, ref: make_ref()}
        declare(cursor, query, params, state)
      {err, _, _} = error when err in [:error, :disconnect] ->
        error
    end
  end

  defp prepare_declare_lookup(%Query{name: "", statement: statement}, %{lru_cache: cache}) do
    LruCache.take(cache, statement)
  end
  defp prepare_declare_lookup(%Query{name: name}, %{cache: cache}) do
    Cache.take(cache, name)
  end

  def handle_fetch(query, cursor, opts, state) do
    %Cursor{ref: ref, statement_id: id} = cursor
    %{cursors: cursors} = state
    case cursors do
      %{^ref => {:first, _, params}} ->
        first(query, cursor, params, opts, state) |> fetch_result(ref, id)
      %{^ref => {:cont, _, columns}} ->
        next(query, cursor, columns, opts, state) |> fetch_result(ref, id)
      %{^ref => {:halt, _, columns}} ->
        # cursor finished, empty result
        result = %Mariaex.Result{rows: [], num_rows: 0}
        {:halt, {result, columns}, state}
      %{} ->
        msg = "could not find active cursor: #{inspect cursor}"
        {:error, Mariaex.Error.exception(msg), state}
    end
  end

  defp fetch_result({:cont, {_, columns} = res, state}, ref, id) do
    {:cont, res, put_in(state.cursors[ref], {:cont, id, columns})}
  end
  defp fetch_result({:halt, {_, columns} = res, state}, ref, id) do
    {:halt, res, put_in(state.cursors[ref], {:halt, id, columns})}
  end
  defp fetch_result({:error, _, _} = error, _ref, _id) do
    error
  end
  defp fetch_result({:disconnect, _, _} = disconnect, _ref, _id) do
    disconnect
  end

  defp first(query, %Cursor{statement_id: :text}, params, opts, state) do
    case handle_execute(query, params, opts, state) do
      {:ok, _, result, state} ->
        {:halt, result, state}
      other ->
        other
    end
  end
  defp first(query, %Cursor{statement_id: id}, params, opts, state) do
    msg_send(stmt_execute(command: com_stmt_execute(), parameters: params, statement_id: id, flags: @cursor_type_read_only, iteration_count: 1), state, 0)
    binary_first_recv(state, opts, query)
  end

  defp binary_first_recv(state, opts, query) do
    case binary_first_recv(state, opts) do
      {:eof, columns, flags, state} ->
        binary_first_resultset(state, opts, query, columns, [], flags)
      {:resultset, columns, rows, flags, state} ->
        binary_first_resultset(state, opts, query, columns, rows, flags)
      {:ok, packet(msg: ok_resp()) = packet, state} ->
        {:ok, result, state} = handle_ok_packet(packet, query, state)
        {:halt, result, state}
      {:ok, packet, state} ->
        handle_error(packet, query, state)
      {:error, reason} ->
        recv_error(reason, state)
    end
  end

  defp binary_first_recv(state, opts) do
    state = %{state | state: :column_count}
    with {:ok, packet(msg: column_count(column_count: num_cols)), state} <- msg_recv(state, opts),
         {:eof, columns, flags, state} when (flags &&& @server_status_cursor_exists) == 0 <- columns_recv(state, opts, num_cols),
         {:eof, rows, flags, state} <- bin_rows_recv(state, columns) do
      {:resultset, columns, rows, flags, state}
    end
  end

  defp binary_first_resultset(state, opts, query, columns, rows, flags) do
    cond do
      (flags &&& @server_more_results_exists) == @server_more_results_exists ->
        binary_first_more(state, opts, query, columns, rows)
      (flags &&& @server_status_cursor_exists) == @server_status_cursor_exists ->
        result = %Mariaex.Result{rows: rows, connection_id: state.connection_id}
        {:cont, {result, columns}, clean_state(state, flags)}
      true ->
        result = %Mariaex.Result{rows: rows, connection_id: state.connection_id}
        {:halt, {result, columns}, clean_state(state, flags)}
    end
  end

  defp binary_first_more(state, opts, query, columns, rows) do
    case binary_query_more(state, opts, query, columns, rows) do
      {:ok, _query, res, state} ->
        {:halt, res, state}
      other ->
        other
    end
  end

  defp next(query, %Cursor{statement_id: id}, columns, opts, state) do
    max_rows = Keyword.get(opts, :max_rows, @max_rows)
    msg_send(stmt_fetch(command: com_stmt_fetch(), statement_id: id, num_rows: max_rows), state, 0)
    binary_next_recv(state, query, columns)
  end

  defp binary_next_recv(state, query, columns) do
    case bin_rows_recv(state, columns) do
      {:eof, rows, flags, state} ->
        binary_next_resultset(state, columns, rows, flags)
      {:ok, packet, state} ->
        handle_error(packet, query, state)
      {:error, reason} ->
        recv_error(reason, state)
    end
  end

  defp binary_next_resultset(state, columns, rows, flags) do
    cond do
      (flags &&& @server_status_last_row_sent) == @server_status_last_row_sent ->
        result = %Mariaex.Result{rows: rows, connection_id: state.connection_id}
        {:halt, {result, columns}, clean_state(state, flags)}
      (flags &&& @server_status_cursor_exists) == @server_status_cursor_exists ->
        result = %Mariaex.Result{rows: rows, connection_id: state.connection_id}
        {:cont, {result, columns}, clean_state(state, flags)}
    end
  end

  def handle_deallocate(query, cursor, _, state) do
    %Cursor{ref: ref, statement_id: id} = cursor
    case pop_in(state.cursors[ref]) do
      {nil, state} ->
        {:ok, nil, state}
      {_exists, state} when id == :text ->
        {:ok, nil, state}
      {_exists, state} ->
        deallocate(id, query, state)
    end
  end

  defp deallocate(id, query, state) do
    case deallocate_insert(id, query, state) do
      {:reset, reset_id, query} ->
        msg = stmt_reset(command: com_stmt_reset(), statement_id: reset_id)
        deallocate_send(msg, query, state)
      {:close_reset, close_id, reset_id, query} ->
        msgs = [stmt_close(command: com_stmt_close(), statement_id: close_id),
                stmt_reset(command: com_stmt_reset(), statement_id: reset_id)]
        deallocate_send(msgs, query, state)
      {:close, close_id} ->
        msg_send(stmt_close(command: com_stmt_close(), statement_id: close_id), state, 0)
        {:ok, nil, state}
    end
  end

  defp deallocate_insert(id, %Query{name: "", statement: statement, ref: ref, num_params: num_params} = query, %{lru_cache: cache}) do
    case LruCache.insert_new(cache, statement, id, ref, num_params) do
      true ->
        case LruCache.garbage_collect(cache) do
          close_id when is_integer(close_id) ->
            {:close_reset, close_id, id, query}
          nil ->
            {:reset, id, query}
        end
      false ->
        {:close, id}
    end
  end
  defp deallocate_insert(id, %Query{name: name, ref: ref} = query, %{cache: cache}) do
    case Cache.insert_new(cache, name, id, ref) do
      true ->
        {:reset, id, query}
      false ->
        {:close, id}
    end
  end

  defp deallocate_send(msg, query, state) do
    msg_send(msg, state, 0)
    handle_deallocate_recv(state, query)
  end

  def_handle :handle_deallocate_recv, :handle_deallocate_query
  defp handle_deallocate_query(packet(msg: ok_resp()) = packet, query, s), do: handle_ok_packet(packet, query, s)
  defp handle_deallocate_query(packet, query, s), do: handle_error(packet, query, s)

  @doc """
  DBConnection callback
  """
  def handle_begin(opts, %{transaction_status: status} = s) do
    case Keyword.get(opts, :mode, :transaction) do
      :transaction when status == :idle ->
        handle_transaction("BEGIN", s, opts)
      :savepoint when status == :transaction ->
        handle_transaction("SAVEPOINT mariaex_savepoint", s, opts)
      mode when mode in [:transaction, :savepoint] ->
        {status, s}
    end
  end

  @doc """
  DBConnection callback
  """
  def handle_commit(opts, %{transaction_status: status} = s) do
    case Keyword.get(opts, :mode, :transaction) do
      :transaction when status == :transaction ->
        handle_transaction("COMMIT", s, opts)
      :savepoint when status == :transaction ->
        handle_transaction("RELEASE SAVEPOINT mariaex_savepoint", s, opts)
      mode when mode in [:transaction, :savepoint] ->
        {status, s}
    end
  end

  @doc """
  DBConnection callback
  """
  def handle_rollback(opts, %{transaction_status: status} = s) do
    case Keyword.get(opts, :mode, :transaction) do
      :transaction when status == :transaction ->
        handle_transaction("ROLLBACK", s, opts)
      :savepoint when status == :transaction ->
        rollback_release =
          "ROLLBACK TO SAVEPOINT mariaex_savepoint; RELEASE SAVEPOINT mariaex_savepoint"
        handle_transaction(rollback_release, s, opts)
      mode when mode in [:transaction, :savepoint] ->
        {status, s}
    end
  end

  @doc """
  DBConnection callback
  """
  def handle_status(_, %{transaction_status: status} = state) do
    {status, state}
  end

  defp handle_transaction(statement, state, opts) do
    state
    |> send_text_query(statement)
    |> transaction_recv(opts)
  end

  defp transaction_recv(state, opts) do
    case msg_recv(state, opts) do
      {:ok, packet(msg: ok_resp(status_flags: flags)), state}
          when (flags &&& @server_more_results_exists) == @server_more_results_exists ->
        # rollback/release has multiple results
        transaction_recv(state, opts)
      {:ok, packet(msg: ok_resp(status_flags: flags)), state} ->
        result = %Mariaex.Result{columns: [], rows: nil, num_rows: 0,
          last_insert_id: 0}
        {:ok, result, clean_state(state, flags)}
      {:ok, packet(msg: error_resp(error_code: code, error_message: message)), state} ->
        err = %Mariaex.Error{mariadb: %{code: code, message: message}}
        # connection in bad state and unlikely to recover
        {:disconnect, err, state}
      {:error, reason} ->
        recv_error(reason, state)
    end
  end

  defp recv_error(reason, %{sock: {sock_mod, _}} = state) do
    do_disconnect(state, {sock_mod.tag, "recv", reason, ""})
  end

  @doc """
  Do disconnect
  """
  def do_disconnect(s, {tag, action, reason, buffer}) do
    msg = "#{tag} #{action}: #{format_error(tag, reason)}"
    {:disconnect, DBConnection.ConnectionError.exception(msg), %{s | buffer: buffer}}
  end

  defp format_error(_, reason) when reason in @nonposix_errors do
    Atom.to_string(reason)
  end
  defp format_error(:tcp, reason) do
    "#{:inet.format_error(reason)} - #{inspect(reason)}"
  end
  defp format_error(:ssl, reason) do
    "#{:ssl.format_error(reason)} - #{inspect(reason)}"
  end

  defp do_disconnect(%{connection_id: connection_id} = state, %Mariaex.Error{} = err, buffer) do
    {:disconnect, %{err | connection_id: connection_id}, %{state | buffer: buffer}}
  end

  def dispatch(packet(msg: :mysql_old_password), state = %{opts: opts, handshake: handshake}) do
    if opts[:insecure_auth] do
      password = opts[:password]
      %{salt: {salt1, salt2}, seqnum: seqnum} = handshake
      password = password(@mysql_old_password, password, <<salt1 :: binary, salt2 :: binary>>)
      # TODO: rethink seqnum handling
      msg_send(old_password(password: password), state, seqnum + 3)
      state
    else
      {:error, %Mariaex.Error{message: "MySQL server is requesting the old and insecure pre-4.1 auth mechanism. " <>
                                       "Upgrade the user password or use the `insecure_auth: true` option."}}
    end
  end

  defp password(@mysql_native_password <> _, password, salt), do: mysql_native_password(password, salt)
  defp password("", password, salt),                  do: mysql_native_password(password, salt)
  defp password(@mysql_old_password, password, salt), do: mysql_old_password(password, salt)

  defp mysql_native_password(password, salt) do
    stage1 = :crypto.hash(:sha, password)
    stage2 = :crypto.hash(:sha, stage1)
    :crypto.hash_init(:sha)
    |> :crypto.hash_update(salt)
    |> :crypto.hash_update(stage2)
    |> :crypto.hash_final
    |> bxor_binary(stage1)
  end

  defp bxor_binary(b1, b2) do
    (for {e1, e2} <- List.zip([:erlang.binary_to_list(b1), :erlang.binary_to_list(b2)]), do: e1 ^^^ e2) |> :erlang.list_to_binary
  end

  def mysql_old_password(password, salt) do
    {p1, p2} = hash(password)
    {s1, s2} = hash(salt)
    seed1 = bxor(p1, s1)
    seed2 = bxor(p2, s2)
    list = rnd(9, seed1, seed2)
    {l, [extra]} = Enum.split(list, 8)
    l |> Enum.map(&bxor(&1, extra - 64)) |> to_string
  end

  defp hash(bin) when is_binary(bin), do: bin |> to_charlist |> hash
  defp hash(s), do: hash(s, 1345345333, 305419889, 7)
  defp hash([c | s], n1, n2, add) do
    n1 = bxor(n1, (((band(n1, 63) + add) * c + n1 * 256)))
    n2 = n2 + (bxor(n2 * 256, n1))
    add = add + c
    hash(s, n1, n2, add)
  end
  defp hash([], n1, n2, _add) do
    mask = bsl(1, 31) - 1
    {band(n1, mask), band(n2, mask)}
  end

  defp rnd(n, seed1, seed2) do
    mod = bsl(1, 30) - 1
    rnd(n, [], rem(seed1, mod), rem(seed2, mod))
  end
  defp rnd(0, list, _, _) do
    Enum.reverse(list)
  end
  defp rnd(n, list, seed1, seed2) do
    mod = bsl(1, 30) - 1
    seed1 = rem((seed1 * 3 + seed2), mod)
    seed2 = rem((seed1 + seed2 + 33), mod)
    float = (seed1 / mod) * 31
    val = trunc(float) + 64
    rnd(n - 1, [val | list], seed1, seed2)
  end

  defp msg_send(msg, %{sock: {sock_mod, sock}}, seqnum), do: msg_send(msg, {sock_mod, sock}, seqnum)
  defp msg_send(msgs, {sock_mod, sock}, seqnum) when is_list(msgs) do
    binaries = Enum.reduce(msgs, [], &[&2 | encode(&1, seqnum)])
    sock_mod.send(sock, binaries)
  end

  defp msg_send(msg, {sock_mod, sock}, seqnum) do
    data = encode(msg, seqnum)
    sock_mod.send(sock, data)
  end

  defp msg_recv(%__MODULE__{sock: sock_info, buffer: buffer}=state, opts \\ []) do
    msg_recv(sock_info, state, opts[:timeout] || state.timeout, buffer)
  end

  defp msg_recv(sock, state, timeout, buffer) do
    case msg_decode(buffer, state) do
      {:ok, _packet, _new_state}=success ->
        success

      {:more, more} ->
        msg_recv(sock, state, timeout, buffer, more)

      {:error, _}=err ->
        err
    end
  end

  defp msg_recv({sock_mod, sock}=s, state, timeout, buffer, more) do

    case sock_mod.recv(sock, more, timeout) do
      {:ok, data} when byte_size(data) < more ->
        msg_recv(s, state, timeout, [buffer | data], more - byte_size(data))

      {:ok, data} when is_binary(buffer) ->
        msg_recv(s, state, timeout, buffer <> data)

      {:ok, data} when is_list(buffer) ->
        msg_recv(s, state, timeout, IO.iodata_to_binary([buffer | data]))

      {:error, _} = err ->
          err
    end
  end


  def msg_decode(<< len :: size(24)-little-integer, _seqnum :: size(8)-integer, message :: binary>>=header, state) when byte_size(message) >= len do

    {packet, rest} = decode(header, state.state)
    {:ok, packet, %{state | buffer: rest}}
  end

  def msg_decode(_buffer, _state) do
    {:more, 0}
  end

  def_handle :ping_recv, :ping_handle

  @doc """
  DBConnection callback
  """
  def ping(%{buffer: buffer} = state) when is_binary(buffer) do
    msg_send(text_cmd(command: com_ping(), statement: ""), state, 0)
    ping_recv(state, :ping)
  end
  def ping(state) do
    case checkout(state) do
      {:ok, state} ->
        msg_send(text_cmd(command: com_ping(), statement: ""), state, 0)
        {:ok, state} = ping_recv(state, :ping)
        checkin(state)
      disconnect ->
        disconnect
    end
  end

  defp ping_handle(packet(msg: ok_resp()), :ping, %{buffer: buffer} = state) when is_binary(buffer) do
    {:ok, state}
  end

  defp ping_handle(error = packet(msg: error_resp()), :ping, %{buffer: buffer} = state) when is_binary(buffer) do
    {:disconnect, error,  state}
  end

  defp send_text_query(s, statement) do
    msg_send(text_cmd(command: com_query(), statement: statement), s, 0)
    %{s | state: :column_count}
  end

  defp abort_statement(s, query, code, message) do
    abort_statement(s, query, %Mariaex.Error{
      mariadb: %{code: code, message: message},
      connection_id: s.connection_id
    })
  end
  defp abort_statement(s, query, error = %Mariaex.Error{}) do
    case query do
      %Query{} ->
        {:ok, nil, s} = handle_close(query, [], s)
        {:error, error, clean_state(s, nil)}
      nil ->
        {:error, error, clean_state(s, nil)}
    end
  end
end
