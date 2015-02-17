defmodule Mariaex.Protocol do
  @moduledoc false

  alias Mariaex.Connection
  #alias Mariaex.Types
  import Mariaex.Messages
  use Bitwise, only_operators: true

  @mysql_native_password "mysql_native_password"

  @maxpacketbytes 50000000
  @long_password           0x00000001
  @long_flag               0x00000004
  @client_connect_with_db  0x00000008
  @client_local_file       0x00000080
  @protocol_41             0x00000200
  @transactions            0x00002000
  @secure_connection       0x00008000
  @client_multi_statements 0x00010000
  @client_multi_results    0x00020000
  @client_deprecate_eof    0x01000000
  @capabilities [@long_password, @long_flag, @client_local_file, @transactions,
                 @client_connect_with_db, @client_multi_statements, @client_multi_results,
                 @protocol_41, @secure_connection, @client_deprecate_eof]

  def dispatch(packet(seqnum: seqnum, msg: handshake(plugin: plugin) = handshake) = _packet, %{state: :handshake, opts: opts} = s) do
    handshake(auth_plugin_data1: salt1, auth_plugin_data2: salt2) = handshake
    password = opts[:password]
    scramble = case password do
      nil -> ""
      _   -> password(plugin, password, <<salt1 :: binary, salt2 :: binary>>)
    end
    capabilities = Enum.reduce(@capabilities, 0, &(&1 ||| &2))
    msg = handshake_resp(username: :unicode.characters_to_binary(opts[:username]), password: scramble,
                         database: opts[:database], capability_flags: capabilities,
                         max_size: @maxpacketbytes, character_set: 8)
    msg_send(msg, s, seqnum + 1)
    %{ s | state: :handshake_send }
  end

  def dispatch(packet(msg: error_resp(error_code: code, error_message: message)), state = %{state: s})
   when s in [:handshake_send, :query_send, :prepare_send, :prepare_send_2, :execute_send] do
    error = %Mariaex.Error{mariadb: %{code: code, message: message}}
    {_, state} = Connection.reply({:error, error}, state)
    %{ state | state: :running, substate: nil }
  end

  def dispatch(packet(msg: column_definition_41() = msg), state = %{types: acc, substate: :column_definitions}) do
    column_definition_41(type: type, name: name) = msg
    %{ state | types: [{name, type} | acc] }
  end

  def dispatch(packet(msg: eof_resp() = _msg), s = %{types: definitions, state: state, substate: :column_definitions}) do
    case state do
      :query_send ->
        %{ s | state: :rows, substate: nil, types: Enum.reverse(definitions) }
      :prepare_send ->
        case s.state_data do
          {true, true} ->
            %{ s | state: :prepare_send_2, parameter_types: Enum.reverse(definitions) }
          {true, false} ->
            send_execute(%{s | parameter_types: Enum.reverse(definitions)})
          {false, true} ->
            send_execute(s)
        end
      :prepare_send_2 ->
        send_execute(s)
      :execute_send ->
        %{ s | state: :rows, substate: :bin_rows, types: Enum.reverse(definitions) }
    end
  end

  def dispatch(packet(msg: ok_resp(affected_rows: affected_rows)), state = %{statement: statement, state: s})
   when s in [:handshake_send, :query_send, :execute_send] do
    command = get_command(statement)
    rows = if (command in [:create, :insert, :begin, :commit, :rollback]) do nil else [] end
    result = {:ok, %Mariaex.Result{command: command, columns: [], rows: rows, num_rows: affected_rows}}
    {_, state} = Connection.reply(result, state)
    %{ state | state: :running, substate: nil }
  end

  def dispatch(packet(msg: stmt_prepare_ok(statement_id: id, num_columns: columns, num_params: params)), state = %{state: :prepare_send}) do
    %{ state | substate: :column_definitions, state_data: {params > 0, columns > 0}, types: [], statement_id: id }
  end

  def dispatch(packet(msg: column_count(column_count: _count)), state = %{state: s}) when s in [:query_send, :execute_send] do
    %{ state | substate: :column_definitions, types: [] }
  end

  def dispatch(packet(msg: row(row: row)), state = %{state: :rows, types: definitions, rows: acc}) do
    %{state | rows: [decode_type_row(row, definitions, []) | acc]}
  end

  def dispatch(packet(msg: bin_row(row: row)), state = %{state: :rows, types: definitions, rows: acc}) do
    %{state | rows: [decode_bin_rows(row, definitions) | acc]}
  end

  def dispatch(packet(msg: msg), state = %{statement: statement, state: :rows, types: types, rows: rows})
   when elem(msg, 0) in [:ok_resp, :eof_resp] do
    result = %Mariaex.Result{command: get_command(statement),
                             columns: (for {type, _} <- types, do: type),
                             rows: Enum.reverse(rows),
                             num_rows: length(rows)}
    {_, state} = Connection.reply({:ok, result}, state)
    %{ state | state: :running, substate: nil }
  end

  defp password(@mysql_native_password, password, salt) do
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

  def send_query(statement, [], s) do
    msg_send(text_cmd(command: com_query, statement: statement), s, 0)
    %{s | statement: statement, parameters: [], state: :query_send, substate: :column_count, rows: []}
  end

  def send_query(statement, params, s) do
    msg_send(text_cmd(command: com_stmt_prepare, statement: statement), s, 0)
    %{s | statement: statement, parameters: params, state: :prepare_send, rows: []}
  end

  defp send_execute(s = %{statement_id: id, parameters: parameters, parameter_types: types}) do
    parameters = Enum.zip(types, parameters)
    msg_send(stmt_execute(command: com_stmt_execute, parameters: parameters, statement_id: id), s, 0)
    %{ s | state: :execute_send, substate: :column_count }
  end

  defp get_command(statement) when is_binary(statement) do
    statement |> String.split(" ", parts: 2) |> hd |> String.downcase |> String.to_atom
  end
  defp get_command(nil), do: nil

end
