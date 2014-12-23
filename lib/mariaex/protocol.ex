defmodule Mariaex.Protocol do
  @moduledoc false

  alias Mariaex.Connection
  #alias Mariaex.Types
  import Mariaex.Messages
  use Bitwise, only_operators: true
  import Record, only: [is_record: 2]

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

  def dispatch(packet(seqnum: seqnum, msg: handshake(plugin: plugin) = handshake) = packet, %{state: :handshake, opts: opts} = s) do
    handshake(auth_plugin_data1: salt1, auth_plugin_data2: salt2) = handshake
    password = opts[:password]
    scramble = case password do
      nil -> ""
      _   -> password(plugin, password, <<salt1 :: binary, salt2 :: binary>>)
    end
    capabilities = Enum.reduce(@capabilities, 0, &(&1 ||| &2))
    msg = handshake_resp(user: :unicode.characters_to_binary(opts[:user]), password: scramble,
                         database: opts[:database], capability_flags: capabilities,
                         max_size: @maxpacketbytes, character_set: 8)
    msg_send(msg, s, seqnum + 1)
    %{ s | state: :handshake_send }
  end

  def dispatch(packet(msg: msg), state = %{queue: queue, state: :handshake_send}) do
    {_, state} = Connection.reply(:ok, state)
    %{ state | state: :running }
  end

  def dispatch(packet(msg: error_resp(error_code: code, error_message: message)), state = %{queue: queue, statement: statement, state: :query_send}) do
    error = %Mariaex.Error{mariadb: %{code: code, message: message}}
    {_, state} = Connection.reply({:error, error}, state)
    %{ state | state: :running }
  end

  def dispatch(packet(msg: ok_resp(affected_rows: affected_rows)), state = %{queue: queue, statement: statement, state: :query_send}) do
    result = case affected_rows do
               0 -> :ok
               _ -> {:ok, %Mariaex.Result{command: get_command(statement), columns: [], rows: [], num_rows: affected_rows}}
             end
    {_, state} = Connection.reply(result, state)
    %{ state | state: :running }
  end

  def dispatch(packet(msg: column_count(column_count: _count)), state = %{state: :query_send}) do
    %{ state | state: :column_definitions, types: [] }
  end

  def dispatch(packet(msg: column_definition_41() = msg), state = %{types: acc, state: :column_definitions}) do
    column_definition_41(type: type, name: name) = msg
    %{ state | types: [{name, type} | acc] }
  end

  def dispatch(packet(msg: eof_resp() = msg), state = %{types: definitions, state: :column_definitions}) do
    %{ state | state: :rows, types: Enum.reverse(definitions) }
  end

  def dispatch(packet(msg: row(row: row)), state = %{state: :rows, types: definitions, rows: acc}) do
    %{state | rows: [decode_type_row(row, definitions, []) | acc]}
  end

  def dispatch(packet(msg: msg), state = %{statement: statement, state: :rows, types: types, rows: rows})
   when elem(msg, 0) in [:ok_resp, :eof_resp] do
    result = %Mariaex.Result{command: get_command(statement),
                             columns: (for {type, _} <- types, do: type),
                             rows: Enum.reverse(rows),
                             num_rows: length(rows)}
    {_, state} = Connection.reply({:ok, result}, state)
    %{ state | state: :running }
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

  defp bxor_binary(b1, b2), do: (for {e1, e2} <- List.zip([to_char_list(b1), to_char_list(b2)]), do: e1 ^^^ e2) |> to_string

  defp msg_send(msg, %{sock: {sock_mod, sock}}, seqnum), do: msg_send(msg, {sock_mod, sock}, seqnum)

  defp msg_send(msgs, {sock_mod, sock}, seqnum) when is_list(msgs) do
    binaries = Enum.reduce(msgs, [], &[&2 | encode(&1, seqnum)])
    sock_mod.send(sock, binaries)
  end

  defp msg_send(msg, {sock_mod, sock}, seqnum) do
    data = encode(msg, seqnum)
    sock_mod.send(sock, data)
  end

  def send_query(statement, s) do
    msg_send(text_cmd(command: com_query, statement: statement), s, 0)
    %{s | statement: statement, state: :query_send }
  end

  defp get_command(statement) when is_binary(statement) do
    statement |> String.split(" ", parts: 2) |> hd |> String.downcase |> String.to_atom
  end

end
