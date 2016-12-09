defmodule Mariaex.Messages do
  @moduledoc false

  import Record, only: [defrecord: 2]
  use Mariaex.Coder
  require Decimal

  # @protocol_vsn_major 3
  # @protocol_vsn_minor 0

  defrecord :packet, [:size, :seqnum, :msg, :body]


  @auth_types [ ok: 0, kerberos: 2, cleartext: 3, md5: 5, scm: 6, gss: 7,
                sspi: 9, gss_cont: 8 ]
  _ = @auth_types # Drop warning for now

  @error_fields [ severity: ?S, code: ?C, message: ?M, detail: ?D, hint: ?H,
                  position: ?P, internal_position: ?p, internal_query: ?q,
                  where: ?W, schema: ?s, table: ?t, column: ?c, data_type: ?d,
                  constraint: ?n, file: ?F, line: ?L, routine: ?R ]
  _ = @error_fields # Drop warning for now

  @commands [ com_sleep: 0x00, com_quit: 0x01, com_init_bd: 0x02,
              com_query: 0x03, com_field_list: 0x04, com_create_db: 0x05,
              com_drop_db: 0x06, com_refresh: 0x07, com_shutdown: 0x08,
              com_statistics: 0x09, com_process_info: 0x0a, com_connect: 0x0b,
              com_process_kill: 0x0c, com_debug: 0x0d, com_ping: 0x0e,
              com_time: 0x0f, com_delayed_inser: 0x10, com_change_use: 0x11,
              com_binlog_dump: 0x12, com_table_dump: 0x13, com_connect_out: 0x14,
              com_register_slave: 0x15, com_stmt_prepare: 0x16, com_stmt_execute: 0x17,
              com_stmt_send_long_data: 0x18, com_stmt_close: 0x19, com_stmt_reset: 0x1a,
              com_set_option: 0x1b, com_stmt_fetch: 0x1c, com_daemon: 0x1d,
              com_binlog_dump_gtid: 0x1e, com_reset_connection: 0x1f]

  for {command, number} <- @commands do
    defmacro unquote(command)(), do: unquote(number)
  end

  @types [float:
           [field_type_float: 0x04,
            field_type_double: 0x05],
          decimal:
           [field_type_decimal: 0x00,
            field_type_newdecimal: 0xf6],
          integer:
           [field_type_tiny: 0x01,
            field_type_short: 0x02,
            field_type_long: 0x03,
            field_type_int24: 0x09,
            field_type_year: 0x0d,
            field_type_longlong: 0x08],
          timestamp:
           [field_type_timestamp: 0x07,
            field_type_datetime: 0x0c],
          date:
           [field_type_date: 0x0a],
          time:
           [field_type_time: 0x0b],
          bit:
           [field_type_bit: 0x10],
          string:
           [field_type_varchar: 0x0f,
            field_type_tiny_blob: 0xf9,
            field_type_medium_blob: 0xfa,
            field_type_long_blob: 0xfb,
            field_type_blob: 0xfc,
            field_type_var_string: 0xfd,
            field_type_string: 0xfe],
          null:
           [field_type_null: 0x06]
         ]

  def __type__(:decode, _type, nil), do: nil

  for {_type, list} <- @types,
      {name, id}    <- list   do
    def __type__(:id, unquote(name)), do: unquote(id)
  end
  for {type, list}  <- @types,
      {name, id}    <- list   do
    def __type__(:type, unquote(id)), do: {unquote(type), unquote(name)}
  end

  defcoder :handshake do
    protocol_version 1
    server_version :string
    connection_id 4
    auth_plugin_data1 :string
    capability_flags_1 2
    character_set 1
    status_flags 2
    capability_flags_2 2
    # length_auth_plugin_data and the following ten bytes in the spec are rolled into the following field
    auth_plugin_data2 {__MODULE__, :auth_plugin_data2}
    plugin :string_eof
  end

  defcoder :handshake_resp do
    capability_flags 4
    max_size 4
    character_set 1
    _ 23
    username :string
    password :length_string
    database :string
  end

  defcoder :ssl_connection_request do
    capability_flags 4
    max_size 4
    character_set 1
    _ 23
  end

  defcoder :old_password do
    password :string
  end

  defcoder :ok_resp do
    header 1
    affected_rows :length_encoded_integer
    last_insert_id :length_encoded_integer
    status_flags 2
    warnings 2
    message :string_eof
  end

  defcoder :eof_resp do
    header 1
    warnings 2
    status_flags 2
    ## Since version MySQL of 5.7.X, MySQL sends bigger eof messages without any documentation, what is
    ## added.
    message :string_eof
  end

  defcoder :error_resp do
    header 1
    error_code 2
    sql_state_marker 1, :string
    sql_state 5, :string
    error_message :string_eof
  end

  defcoder :text_cmd do
    command 1
    statement :string_eof
  end

  defcoder :stmt_prepare_ok do
    status 1
    statement_id 4
    num_columns 2
    num_params 2
    _ 1
    warning_count 2
  end

  defcoder :stmt_execute do
    command 1
    statement_id 4
    flags 1
    iteration_count 4
    parameters :string_eof
  end

  defcoder :stmt_fetch do
    command 1
    statement_id 4
    num_rows 4
  end

  defcoder :stmt_close do
    command 1
    statement_id 4
  end

  defcoder :stmt_reset do
    command 1
    statement_id 4
  end

  defcoder :column_count do
    column_count :length_encoded_integer
  end

  defcoder :bin_row do
    row :string_eof
  end

  defcoder :text_row do
    row :string_eof
  end

  defcoder :column_definition_41 do
    catalog   :length_encoded_string
    schema    :length_encoded_string
    table     :length_encoded_string
    org_table :length_encoded_string
    name      :length_encoded_string
    org_name  :length_encoded_string
    length_of_fixed :length_encoded_integer
    character_set 2
    column_length 4
    type 1
    flags 2
    decimals 1
    _ 2
  end

  # Encoding

  def encode(msg, seqnum) do
    body = encode_msg(msg)
    <<(byte_size(body)) :: 24-little, seqnum :: 8, body :: binary>>
  end

  defp encode_msg(rec),
    do: __encode__(rec)

  # Decoding

  def decode(<< len :: size(24)-little-integer, seqnum :: size(8)-integer, body :: size(len)-binary, rest :: binary>>, state) do
    msg = decode_msg(body, state)
    {packet(size: len, seqnum: seqnum, msg: msg, body: body), rest}
  end
  def decode(rest, _state),
    do: {nil, rest}

  def decode_bin_rows(<< len :: size(24)-little-integer, seqnum :: size(8)-integer, body :: size(len)-binary, rest :: binary>>,
                      fields, nullbin_size, rows) do
    case body do
      <<0 :: 8, nullbin::size(nullbin_size)-little-unit(8), values :: binary>> ->
        row = Mariaex.RowParser.decode_bin_rows(values, fields, nullbin)
        decode_bin_rows(rest, fields, nullbin_size, [row | rows])
      body ->
        msg = decode_msg(body, :bin_rows)
        {:ok, packet(size: len, seqnum: seqnum, msg: msg, body: body), rows, rest}
    end
  end
  def decode_bin_rows(<<rest :: binary>>, _fields, _nullbin_size, rows) do
    {:more, rows, rest}
  end

  def decode_text_rows(<< len :: size(24)-little-integer, seqnum :: size(8)-integer, body :: size(len)-binary, rest :: binary>>,
                      fields, rows) do
    case body do
      << 254 :: 8, _ :: binary >> = body when byte_size(body) < 9 ->
        msg = decode_msg(body, :text_rows)
        {:ok, packet(size: len, seqnum: seqnum, msg: msg, body: body), rows, rest}
      body ->
        row = Mariaex.RowParser.decode_text_rows(body, fields)
        decode_text_rows(rest, fields, [row | rows])
    end
  end
  def decode_text_rows(<<rest :: binary>>, _fields, rows) do
    {:more, rows, rest}
  end

  defp decode_msg(<< 255 :: 8, _ :: binary >> = body, _),                          do: __decode__(:error_resp, body)
  defp decode_msg(body, :handshake),                                               do: __decode__(:handshake, body)
  defp decode_msg(<< 0 :: 8, _ :: binary >> = body, :bin_rows),                    do: __decode__(:bin_row, body)
  defp decode_msg(<< 0 :: 8, _ :: binary >> = body, :prepare_send),                do: __decode__(:stmt_prepare_ok, body)
  defp decode_msg(<< 0 :: 8, _ :: binary >> = body, _),                            do: __decode__(:ok_resp, body)
  defp decode_msg(<< 254 >> = _body, _),                                           do: :mysql_old_password
  defp decode_msg(<< 254 :: 8, _ :: binary >> = body, _) when byte_size(body) < 9, do: __decode__(:eof_resp, body)
  defp decode_msg(<< _ :: binary >> = body, :text_rows),                           do: __decode__(:text_row, body)
  defp decode_msg(body, :column_count),                                            do: __decode__(:column_count, body)
  defp decode_msg(body, :column_definitions),                                      do: __decode__(:column_definition_41, body)

  @doc """
  Decode auth plugin data2, which need some additional context checking.
  """
  def auth_plugin_data2(<< length_auth_plugin_data :: 8, _ :: 80, next :: binary >>) do
    length = max(13, length_auth_plugin_data - 8)
    length_nul_terminated = length - 1
    << null_terminated? :: size(length)-binary, next :: binary >> = next
    auth_plugin_data2 = case null_terminated? do
                          << contents :: size(length_nul_terminated)-binary, 0 :: 8 >> -> contents
                          contents -> contents
                        end
    {String.strip(auth_plugin_data2, 0), next}
  end
end
