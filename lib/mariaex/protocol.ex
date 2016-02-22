defmodule Mariaex.Protocol do
  @moduledoc false

  alias Mariaex.Connection
  alias Mariaex.Cache
  import Mariaex.Messages

  use Bitwise

  @timeout 5000
  @keepalive_interval 60000
  @keepalive_timeout @timeout
  @cache_size 100

  @maxpacketbytes 50000000
  @mysql_native_password "mysql_native_password"
  @mysql_old_password :mysql_old_password

  @client_long_password     0x00000001
  @client_found_rows        0x00000002
  @client_long_flag         0x00000004
  @client_connect_with_db   0x00000008
  @client_local_files       0x00000080
  @client_protocol_41       0x00000200
  @client_transactions      0x00002000
  @client_secure_connection 0x00008000
  @client_multi_statements  0x00010000
  @client_multi_results     0x00020000
  @client_deprecate_eof     0x01000000

  @capabilities @client_long_password     ||| @client_found_rows        ||| @client_long_flag |||
                @client_local_files       ||| @client_protocol_41       ||| @client_transactions |||
                @client_secure_connection ||| @client_multi_statements  ||| @client_multi_results |||
                @client_deprecate_eof

  def init(opts) do
    sock_mod   = Keyword.fetch!(opts, :sock_mod)
    host       = Keyword.fetch!(opts, :hostname)
    host       = if is_binary(host), do: String.to_char_list(host), else: host
    port       = opts[:port] || 3306
    timeout    = opts[:timeout] || @timeout
    cache_size = opts[:cache_size] || @cache_size
    keepalive  = {opts[:keepalive_interval] || @keepalive_interval,
                  opts[:keepalive_timeout]  || @keepalive_timeout}
    s = %{sock: nil, tail: "", state: nil, substate: nil, state_data: nil, parameters: %{},
          catch_eof: false, protocol57: false,
          backend_key: nil, sock_mod: sock_mod, seqnum: 0, rows: [], statement: nil, results: [],
          parameter_types: [], types: [], queue: :queue.new, opts: opts, statement_id: nil, handshake: nil,
          keepalive: keepalive, keepalive_send: nil, last_answer: nil, cache: Mariaex.Cache.new(cache_size)}
    case sock_mod.connect(host, port, opts[:socket_options] || [], timeout) do
      {:ok, sock} ->
        s = %{s | state: :handshake, sock: {sock_mod, sock}}
        connect(s, timeout)
      {:error, reason} ->
        {:stop, %Mariaex.Error{message: "tcp connect: #{reason}"}}
    end
  end

  defp connect(state = %{opts: opts, sock: {sock_mod, sock}}, timeout) do
    case passive_receive(state, timeout) do
      {:error, error} ->
        {:stop, error}
      {:ok, state} ->
        statement = "SET CHARACTER SET " <> (opts[:charset] || "utf8")
        case send_text_query(state, statement) |> passive_receive(timeout) do
          {:error, error} ->
            {:stop, error}
          {:ok, state} ->
            sock_mod.next(sock)
            {:ok, state}
        end
    end
  end

  defp passive_receive(s = %{state: state, substate: substate}, timeout) do
    case msg_recv(s.sock, substate || state, timeout) do
      {:ok, packet} ->
        case dispatch(packet, s) do
          {:error, error} ->
            {:error, error}
          state = %{state: new_state} ->
            if new_state == :running do
              {:ok, state}
            else
              passive_receive(state, timeout)
            end
        end
      {:error, error} ->
        {:error, error}
    end
  end

  defp capabilities(opts) do
    case opts[:skip_database] do
      true -> {"", @capabilities}
      _    -> {opts[:database], @capabilities ||| @client_connect_with_db}
    end
  end

  def dispatch(packet(msg: ok_resp()), state = %{state: :ping}) do
    Connection.pong(state) |> put_in([:state], :running)
  end

  def dispatch(packet(seqnum: seqnum, msg: handshake(server_version: server_version, plugin: plugin) = handshake) = _packet, %{state: :handshake} = s) do
    ## It is a little hack here. Because MySQL before 5.7.5 (at least, I need to asume this or test it with versions 5.7.X, where X < 5),
    ## but all points in documentation to changes shows, that changes done in 5.7.5, but haven't tested it further.
    ## In a phase of geting binary protocol resultset ( https://dev.mysql.com/doc/internals/en/binary-protocol-resultset.html )
    ## we get in versions before 5.7.X eof packet after last ColumnDefinition and one for the ending of query.
    ## That means, without differentiation of MySQL versions, we can't know, if eof after last column definition
    ## is resulting eof after result set (which can be none) or simple information, that now results will be coming.
    ## Due to this, we need to difference server version.
    protocol57 = get_3_digits_version(server_version) |> Version.match?("~> 5.7.5")
    handshake(auth_plugin_data1: salt1, auth_plugin_data2: salt2) = handshake
    authorization(plugin, %{s | protocol57: protocol57, handshake: %{salt: {salt1, salt2}, seqnum: seqnum}})
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

  def dispatch(packet(msg: error_resp(error_code: code, error_message: message)), state = %{state: s})
   when s in [:handshake_send, :query_send, :prepare_send, :prepare_send_2, :execute_send] do
    abort_statement(state, code, message)
  end

  def dispatch(packet(msg: column_definition_41() = msg), s = %{types: acc, substate: :column_definitions}) do
    column_definition_41(type: type, name: name) = msg
    %{ s | types: [{name, type} | acc] } |> count_down()
  end

  def dispatch(packet(msg: eof_resp() = _msg), s = %{state: state, substate: :column_definitions}) do
    case state do
      :execute_send ->
        %{ s | state: :rows, substate: :bin_rows, catch_eof: not s.protocol57 }
      _ ->
        s
    end
  end

  def dispatch(packet(msg: ok_resp(affected_rows: affected_rows, last_insert_id: last_insert_id)), state = %{statement: statement, state: s})
   when s in [:handshake_send, :query_send, :execute_send] do
    command = get_command(statement)
    rows = if (command in [:create, :insert, :replace, :update, :delete, :begin, :commit, :rollback]) do nil else [] end
    result = {:ok, %Mariaex.Result{command: command, columns: [], rows: rows, num_rows: affected_rows, last_insert_id: last_insert_id, decoder: :done}}
    {_, state} = Connection.reply(result, state)
    cleanup_state(state)
  end

  def dispatch(packet(msg: stmt_prepare_ok(statement_id: id, num_columns: columns, num_params: params)), state = %{state: :prepare_send}) do
    statedata = {columns, params}
    switch_state(%{state | substate: :column_definitions, state_data: statedata, statement_id: id })
  end

  def dispatch(packet(msg: column_count(column_count: count)), state = %{state: s}) when s in [:query_send, :execute_send] do
    %{ state | substate: :column_definitions, types: [], state_data: {0, count} }
  end

  def dispatch(packet(msg: bin_row(row: row)), state = %{state: :rows, rows: acc}) do
    %{state | rows: [row | acc]}
  end

  def dispatch(packet(msg: msg), state = %{statement: statement, catch_eof: catch_eof, state: :rows})
   when elem(msg, 0) in [:ok_resp, :eof_resp] do
    cmd = get_command(statement)
    case cmd do
      :call when (elem(msg, 0) == :eof_resp) ->
        %{state | state: :call_last_ok, substate: nil}
      _ ->
        case elem(msg, 0) do
          :eof_resp when catch_eof -> %{state | catch_eof: false}
          _                        -> result(state, cmd)
        end
    end
  end

  def dispatch(packet(msg: ok_resp()), state = %{state: :call_last_ok}) do
    result(state, :call)
  end

  def dispatch(packet(msg: eof_resp()), state) do
    state
  end

  defp count_down(s = %{state_data: {columns, params}}) when params > 1,
    do: %{s | state_data: {columns, params - 1}}
  defp count_down(s = %{state_data: {columns, 1}, types: definitions}),
    do: %{s | parameter_types: Enum.reverse(definitions), state_data: {columns, 0}} |> switch_state
  defp count_down(s = %{state_data: {columns, 0}}) when columns > 1,
    do: %{s | state_data: {columns - 1, 0}}
  defp count_down(s = %{state_data: {1, 0}}),
    do: %{s | state_data: {0, 0}} |> switch_state

  defp switch_state(s = %{state: state, state_data: state_data, substate: :column_definitions}) do
    case state do
      :prepare_send ->
        case state_data do
          {0, 0} ->
            send_execute_new(s)
          _ ->
            s
        end
      :execute_send ->
        %{ s | state: :rows, substate: :bin_rows, catch_eof: not s.protocol57 }
    end
  end

  defp result(state = %{types: types, rows: rows}, cmd) do
    result = %Mariaex.Result{command: cmd,
                             rows: rows,
                             decoder: types}
    {_, state} = Connection.reply({:ok, result}, state)
    cleanup_state(state)
  end

  defp cleanup_state(state) do
    %{state | state: :running,
              statement: nil,
              substate: nil,
              statement_id: nil,
              rows: [],
              parameter_types: [],
              types: []}
  end

  defp authorization(plugin, %{handshake: %{seqnum: seqnum, salt: {salt1, salt2}}, opts: opts} = s) do
    password = opts[:password]
    scramble = case password do
      nil -> ""
      ""  -> ""
      _   -> password(plugin, password, <<salt1 :: binary, salt2 :: binary>>)
    end
    {database, capabilities} = capabilities(opts)
    msg = handshake_resp(username: :unicode.characters_to_binary(opts[:username]), password: scramble,
                         database: database, capability_flags: capabilities,
                         max_size: @maxpacketbytes, character_set: 8)
    msg_send(msg, s, seqnum + 1)
    %{ s | state: :handshake_send }
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

  defp hash(bin) when is_binary(bin), do: bin |> to_char_list |> hash
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

  defp msg_recv({sock_mod, sock}, decode_state, timeout) do
    case sock_mod.recv(sock, 4, timeout) do
      {:ok, << len :: size(24)-little-integer, _seqnum :: size(8)-integer >> = header} ->
        case sock_mod.recv(sock, len, timeout) do
          {:ok, packet_body} ->
            {packet, ""} = decode(header <> packet_body, decode_state)
            {:ok, packet}
          {:error, _} = error ->
            error
        end
      {:error, _} = error ->
        error
    end
  end

  def ping(s) do
    msg_send(text_cmd(command: com_ping, statement: ""), s, 0)
    %{s | state: :ping}
  end

  def send_query(statement, params, s) do
    command = get_command(statement)
    case command in [:insert, :select, :update, :delete, :replace, :show, :call, :describe] do
      true ->
        case Cache.lookup(s.cache, statement) do
          {id, parameter_types} ->
            Cache.update(s.cache, statement, {id, parameter_types})
            send_execute(%{ s | statement_id: id, statement: statement, parameters: params,
                                parameter_types: parameter_types, state: :prepare_send, rows: []})
          nil ->
            msg_send(text_cmd(command: com_stmt_prepare, statement: statement), s, 0)
            %{s | statement: statement, parameters: params, parameter_types: [], types: [], state: :prepare_send, rows: []}
        end
      false when params == [] ->
        send_text_query(s, statement)
      false ->
        {_, s} = Connection.reply({:error, %Mariaex.Error{message: "unsupported query"}}, s)
        cleanup_state(s)
    end
  end

  defp send_text_query(s, statement) do
    msg_send(text_cmd(command: com_query, statement: statement), s, 0)
    %{s | statement: statement, parameters: [], types: [], state: :query_send, substate: :column_count, rows: []}
  end

  defp send_execute_new(s = %{statement: statement, statement_id: id, parameter_types: parameter_types, cache: cache, sock: sock}) do
    Cache.insert(cache, statement, {id, parameter_types}, &close_statement(&1, &2, sock))
    send_execute(s)
  end

  defp send_execute(s = %{statement_id: id, parameters: parameters, parameter_types: parameter_types}) do
    if length(parameters) == length(parameter_types) do
      parameters = Enum.zip(parameter_types, parameters)
      try do
        msg_send(stmt_execute(command: com_stmt_execute, parameters: parameters, statement_id: id), s, 0)
        %{ s | state: :execute_send, substate: :column_count }
      catch
        :throw, :encoder_error ->
          abort_statement(s, "query has invalid parameters")
      end
    else
      abort_statement(s, "query has invalid number of parameters")
    end
  end

  defp abort_statement(s, code, message) do
    abort_statement(s, %Mariaex.Error{mariadb: %{code: code, message: message}})
  end
  defp abort_statement(s, error = %Mariaex.Error{}) do
    case Connection.reply({:error, error}, s) do
      {true, s}  -> close_statement(s)
      {false, _s} -> {:error, error}
    end
  end
  defp abort_statement(s, error_msg) do
    abort_statement(s, %Mariaex.Error{message: error_msg})
  end

  def close_statement(_statement, {id, _}, sock) do
    msg_send(stmt_close(command: com_stmt_close, statement_id: id), sock, 0)
  end

  def close_statement(s = %{statement_id: nil}) do
    cleanup_state(s)
  end
  def close_statement(s = %{statement: statement, sock: sock, cache: cache}) do
    Cache.delete(cache, statement, &close_statement(&1, &2, sock))
    cleanup_state(s)
  end

  defp get_command(statement) when is_binary(statement) do
    statement |> :binary.split([" ", "\n"]) |> hd |> String.downcase |> String.to_atom
  end
  defp get_command(nil), do: nil

  defp get_3_digits_version(server_version) do
    server_version
    |> String.split("-", parts: 2)
    |> hd
    |> String.split(".")
    |> Enum.slice(0,3)
    |> Enum.join(".")
  end
end
