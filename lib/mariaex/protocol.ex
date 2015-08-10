defmodule Mariaex.Protocol do
  @moduledoc false

  alias Mariaex.Connection
  alias Mariaex.Cache
  import Mariaex.Messages

  use Bitwise, only_operators: true

  @maxpacketbytes 50000000
  @mysql_native_password "mysql_native_password"

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

  defp capabilities(opts) do
    case opts[:skip_database] do
      true -> {"", @capabilities}
      _    -> {opts[:database], @capabilities ||| @client_connect_with_db}
    end
  end

  def dispatch(packet(msg: ok_resp()), state = %{state: :ping}) do
    Connection.pong(state) |> put_in([:state], :running)
  end

  def dispatch(packet(seqnum: seqnum, msg: handshake(plugin: plugin) = handshake) = _packet, %{state: :handshake, opts: opts} = s) do
    handshake(auth_plugin_data1: salt1, auth_plugin_data2: salt2) = handshake
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

  def dispatch(packet(msg: error_resp(error_code: code, error_message: message)), state = %{state: s})
   when s in [:handshake_send, :query_send, :prepare_send, :prepare_send_2, :execute_send] do
    abort_statement(state, code, message)
  end

  def dispatch(packet(msg: column_definition_41() = msg), state = %{types: acc, substate: :column_definitions}) do
    column_definition_41(type: type, name: name) = msg
    %{ state | types: [{name, type} | acc] }
  end

  def dispatch(packet(msg: eof_resp() = _msg), s = %{types: definitions, state: state, substate: :column_definitions}) do
    case state do
      :prepare_send ->
        case s.state_data do
          {true, true} ->
            %{ s | state: :prepare_send_2, parameter_types: Enum.reverse(definitions) }
          {true, false} ->
            send_execute_new(%{s | parameter_types: Enum.reverse(definitions)})
          {false, true} ->
            send_execute_new(s)
        end
      :prepare_send_2 ->
        send_execute_new(s)
      :execute_send ->
        %{ s | state: :rows, substate: :bin_rows, types: Enum.reverse(definitions) }
    end
  end

  def dispatch(packet(msg: ok_resp(affected_rows: affected_rows, last_insert_id: last_insert_id)), state = %{statement: statement, state: s})
   when s in [:handshake_send, :query_send, :execute_send] do
    command = get_command(statement)
    rows = if (command in [:create, :insert, :update, :delete, :begin, :commit, :rollback]) do nil else [] end
    result = {:ok, %Mariaex.Result{command: command, columns: [], rows: rows, num_rows: affected_rows, last_insert_id: last_insert_id}}
    {_, state} = Connection.reply(result, state)
    %{ state | state: :running, substate: nil, statement_id: nil}
  end

  def dispatch(packet(msg: ok_resp(affected_rows: affected_rows, last_insert_id: last_insert_id)), state = %{statement: statement, state: s})
   when s in [:handshake_send, :query_send, :execute_send] do
    command = get_command(statement)
    rows = if (command in [:create, :insert, :update, :delete, :begin, :commit, :rollback]) do nil else [] end
    result = {:ok, %Mariaex.Result{command: command, columns: [], rows: rows, num_rows: affected_rows, last_insert_id: last_insert_id}}
    {_, state} = Connection.reply(result, state)
    %{ state | state: :running, substate: nil, statement_id: nil}
  end

  def dispatch(packet(msg: stmt_prepare_ok(statement_id: id, num_columns: columns, num_params: params)), state = %{state: :prepare_send}) do
    statedata = {params > 0, columns > 0}
    case statedata do
      {false, false} ->
        send_execute(%{ state | statement_id: id})
      _ ->
        %{ state | substate: :column_definitions, state_data: {params > 0, columns > 0}, statement_id: id }
    end
  end

  def dispatch(packet(msg: column_count(column_count: _count)), state = %{state: s}) when s in [:query_send, :execute_send] do
    %{ state | substate: :column_definitions, types: [] }
  end

  def dispatch(packet(msg: bin_row(row: row)), state = %{state: :rows, types: definitions, rows: acc}) do
    %{state | rows: [decode_bin_rows(row, definitions) | acc]}
  end

  def dispatch(packet(msg: msg), state = %{statement: statement, state: :rows})
   when elem(msg, 0) in [:ok_resp, :eof_resp] do

    cmd = get_command(statement)
    case cmd do
      :call when elem(msg, 0) == :eof_resp ->
        %{state | state: :call_last_ok, substate: nil}
      _ ->
        result(state, cmd)
    end
  end

  def dispatch(packet(msg: ok_resp()), state = %{state: :call_last_ok}) do
    result(state, :call)
  end

  defp result(state = %{types: types, rows: rows}, cmd) do
    result = %Mariaex.Result{command: cmd,
                             columns: (for {type, _} <- types, do: type),
                             rows: Enum.reverse(rows),
                             num_rows: length(rows)}
    {_, state} = Connection.reply({:ok, result}, state)
    %{ state | state: :running, substate: nil, statement_id: nil}
  end

  defp password(@mysql_native_password <> _, password, salt), do: mysql_native_password(password, salt)
  defp password("", password, salt), do: mysql_native_password(password, salt)

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

  defp msg_send(msg, %{sock: {sock_mod, sock}}, seqnum), do: msg_send(msg, {sock_mod, sock}, seqnum)

  defp msg_send(msgs, {sock_mod, sock}, seqnum) when is_list(msgs) do
    binaries = Enum.reduce(msgs, [], &[&2 | encode(&1, seqnum)])
    sock_mod.send(sock, binaries)
  end

  defp msg_send(msg, {sock_mod, sock}, seqnum) do
    data = encode(msg, seqnum)
    sock_mod.send(sock, data)
  end

  def ping(s) do
    msg_send(text_cmd(command: com_ping, statement: ""), s, 0)
    %{s | state: :ping}
  end

  def send_query(statement, params, s) do
    command = get_command(statement)
    case command in [:insert, :select, :update, :delete, :show, :call] do
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
        msg_send(text_cmd(command: com_query, statement: statement), s, 0)
        %{s | statement: statement, parameters: [], types: [], state: :query_send, substate: :column_count, rows: []}
      false ->
        {_, s} = Connection.reply({:error, %Mariaex.Error{message: "unsupported query"}}, s)
        %{ s | state: :running, substate: nil}
    end
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
    {_, s} = Connection.reply({:error, error}, s)
    close_statement(s)
  end
  defp abort_statement(s, error_msg) do
    abort_statement(s, %Mariaex.Error{message: error_msg})
  end

  def close_statement(_statement, {id, _}, sock) do
    msg_send(stmt_close(command: com_stmt_close, statement_id: id), sock, 0)
  end

  def close_statement(s = %{statement_id: nil}) do
    %{ s | state: :running, substate: nil}
  end
  def close_statement(s = %{statement: statement, sock: sock, cache: cache}) do
    Cache.delete(cache, statement, &close_statement(&1, &2, sock))
    %{ s | state: :running, substate: nil, statement_id: nil}
  end

  defp get_command(statement) when is_binary(statement) do
    statement |> :binary.split([" ", "\n"]) |> hd |> String.downcase |> String.to_atom
  end
  defp get_command(nil), do: nil
end
