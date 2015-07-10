defmodule Mariaex.Messages do
  @moduledoc false

  import Record, only: [defrecord: 2]
  import Mariaex.Coder.Utils
  use Mariaex.Coder
  require Decimal

  @protocol_vsn_major 3
  @protocol_vsn_minor 0

  defrecord :packet, [:size, :seqnum, :msg, :body]


  @auth_types [ ok: 0, kerberos: 2, cleartext: 3, md5: 5, scm: 6, gss: 7,
                sspi: 9, gss_cont: 8 ]

  @error_fields [ severity: ?S, code: ?C, message: ?M, detail: ?D, hint: ?H,
                  position: ?P, internal_position: ?p, internal_query: ?q,
                  where: ?W, schema: ?s, table: ?t, column: ?c, data_type: ?d,
                  constraint: ?n, file: ?F, line: ?L, routine: ?R ]

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
    length_auth_plugin_data 1
    _ 10
    auth_plugin_data2 :string #max(13, length_auth_plugin_data - 8), :string
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

  defcoder :stmt_close do
    command 1
    statement_id 4
  end

  defcoder :column_count do
    column_count :length_encoded_integer
  end

  defcoder :bin_row do
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

  defp encode_msg(rec = stmt_execute(parameters: params, )),
    do: stmt_execute(rec, parameters: parameters_to_binary(params), flags: 0, iteration_count: 1) |> __encode__()
  defp encode_msg(rec),
    do: __encode__(rec)

  defp parameters_to_binary([]), do: <<>>
  defp parameters_to_binary(params) do
    set = {<<>>, <<>>, <<>>}
    {nullbits, typesbin, valuesbin} = Enum.reduce(params, set, fn(p, acc) -> encode_params(p, acc) end)
    << null_map_to_mysql(nullbits, <<>>) :: binary, 1 :: 8, typesbin :: binary, valuesbin :: binary >>
  end

  defp encode_params({_, param}, {nullbits, typesbin, valuesbin}) do
    {nullbit, type, value} = encode_param(param)
    {<< nullbits :: bitstring, nullbit :: 1>>,
     << typesbin :: binary, __type__(:id, type) :: 16-little >>,
     << valuesbin :: binary, value :: binary >>}
  end

  defp encode_param(nil),
    do: {1, :field_type_null, ""}
  defp encode_param(bin) when is_binary(bin),
    do: {0, :field_type_blob, << to_length_encoded_integer(byte_size(bin)) :: binary, bin :: binary >>}
  defp encode_param(int) when is_integer(int),
    do: {0, :field_type_longlong, << int :: 64-little >>}
  defp encode_param(float) when is_float(float),
    do: {0, :field_type_double, << float :: 64-little-float >>}
  defp encode_param(true),
    do: {0, :field_type_tiny, << 01 >>}
  defp encode_param(false),
    do: {0, :field_type_tiny, << 00 >>}
  defp encode_param(%Decimal{} = value) do
    bin = Decimal.to_string(value, :normal)
    {0, :field_type_newdecimal, << to_length_encoded_integer(byte_size(bin)) :: binary, bin :: binary >>}
  end
  defp encode_param({year, month, day}),
    do: {0, :field_type_date, << 4::8-little, year::16-little, month::8-little, day::8-little>>}
  defp encode_param({hour, min, sec, 0}),
    do: {0, :field_type_time, << 8 :: 8-little, 0 :: 8-little, 0 :: 32-little, hour :: 8-little, min :: 8-little, sec :: 8-little >>}
  defp encode_param({hour, min, sec, msec}),
    do: {0, :field_type_time, << 12 :: 8-little, 0 :: 8-little, 0 :: 32-little, hour :: 8-little, min :: 8-little, sec :: 8-little, msec :: 32-little>>}
  defp encode_param({{year, month, day}, {hour, min, sec, 0}}),
    do: {0, :field_type_datetime, << 7::8-little, year::16-little, month::8-little, day::8-little, hour::8-little, min::8-little, sec::8-little>>}
  defp encode_param({{year, month, day}, {hour, min, sec, msec}}),
    do: {0, :field_type_datetime, <<11::8-little, year::16-little, month::8-little, day::8-little, hour::8-little, min::8-little, sec::8-little, msec::32-little>>}
  defp encode_param(_else),
    do: throw(:encoder_error)

  defp null_map_to_mysql(<<byte :: 1-bytes, rest :: bits>>, acc) do
    null_map_to_mysql(rest, << acc :: bytes, reverse_bits(byte, "") :: bytes >>)
  end
  defp null_map_to_mysql(bits, acc) do
    padding = rem(8 - bit_size(bits), 8)
    << acc :: binary, 0 :: size(padding), reverse_bits(bits, "") :: bits >>
  end

  defp reverse_bits(<<>>, acc),
    do: acc
  defp reverse_bits(<<h::1, t::bits>>, acc),
    do: reverse_bits(t, <<h::1, acc::bits>>)

  # Decoding

  def decode(<< len :: size(24)-little-integer, seqnum :: size(8)-integer, body :: size(len)-binary, rest :: binary>>, state),
    do: {packet(size: len, seqnum: seqnum, msg: decode_msg(body, state), body: body), rest}
  def decode(rest, _state),
    do: {nil, rest}

  defp decode_msg(body, :handshake),                                               do: __decode__(:handshake, body)
  defp decode_msg(<< 0 :: 8, _ :: binary >> = body, :bin_rows),                    do: __decode__(:bin_row, body)
  defp decode_msg(<< 0 :: 8, _ :: binary >> = body, :prepare_send),                do: __decode__(:stmt_prepare_ok, body)
  defp decode_msg(<< 0 :: 8, _ :: binary >> = body, _),                            do: __decode__(:ok_resp, body)
  defp decode_msg(<< 254 :: 8, _ :: binary >> = body, _) when byte_size(body) < 9, do: __decode__(:eof_resp, body)
  defp decode_msg(<< 255 :: 8, _ :: binary >> = body, _),                          do: __decode__(:error_resp, body)
  defp decode_msg(body, :column_count),                                            do: __decode__(:column_count, body)
  defp decode_msg(body, :column_definitions),                                      do: __decode__(:column_definition_41, body)

  def decode_bin_rows(packet, fields) do
    nullbin_size = div(length(fields) + 7 + 2, 8)
    << 0 :: 8, nullbin :: size(nullbin_size)-binary, rest :: binary >> = packet
    nullbin = null_map_from_mysql(nullbin)
    decode_bin_rows(rest, fields, nullbin, [])
  end
  def decode_bin_rows(<<>>, [], _, acc) do
    Enum.reverse(acc)
  end
  def decode_bin_rows(packet, [_ | fields], << 1 :: 1, nullrest :: bits >>, acc) do
    decode_bin_rows(packet, fields, nullrest, [nil | acc])
  end
  def decode_bin_rows(packet, [{_name, type} | fields], << 0 :: 1, nullrest :: bits >>, acc) do
    {value, next} = handle_decode_bin_rows(__type__(:type, type), packet)
    decode_bin_rows(next, fields, nullrest, [value | acc])
  end

  defp handle_decode_bin_rows({:string, _mysql_type}, packet),              do: length_encoded_string(packet)
  defp handle_decode_bin_rows({:integer, :field_type_tiny}, packet),        do: parse_int_packet(packet, 8)
  defp handle_decode_bin_rows({:integer, :field_type_short}, packet),       do: parse_int_packet(packet, 16)
  defp handle_decode_bin_rows({:integer, :field_type_int24}, packet),       do: parse_int_packet(packet, 32)
  defp handle_decode_bin_rows({:integer, :field_type_long}, packet),        do: parse_int_packet(packet, 32)
  defp handle_decode_bin_rows({:integer, :field_type_longlong}, packet),    do: parse_int_packet(packet, 64)
  defp handle_decode_bin_rows({:integer, :field_type_year}, packet),        do: parse_int_packet(packet, 16)
  defp handle_decode_bin_rows({:time, :field_type_time}, packet),           do: parse_time_packet(packet)
  defp handle_decode_bin_rows({:date, :field_type_date}, packet),           do: parse_date_packet(packet)
  defp handle_decode_bin_rows({:timestamp, :field_type_datetime}, packet),  do: parse_datetime_packet(packet)
  defp handle_decode_bin_rows({:timestamp, :field_type_timestamp}, packet), do: parse_datetime_packet(packet)
  defp handle_decode_bin_rows({:decimal, :field_type_newdecimal}, packet),  do: parse_decimal_packet(packet)
  defp handle_decode_bin_rows({:float, :field_type_float}, packet),         do: parse_float_packet(packet, 32)
  defp handle_decode_bin_rows({:float, :field_type_double}, packet),        do: parse_float_packet(packet, 64)

  defp parse_float_packet(packet, size) do
    << value :: size(size)-float-little, rest :: binary >> = packet
    {value, rest}
  end

  defp parse_int_packet(packet, size) do
    << value :: size(size)-little-signed, rest :: binary >> = packet
    {value, rest}
  end

  defp parse_decimal_packet(packet) do
    << length,  raw_value :: size(length)-little-binary, rest :: binary >> = packet
    value = Decimal.new(raw_value)
    {value, rest}
  end

  defp parse_time_packet(packet) do
    case packet do
      << 0 :: 8-little, rest :: binary >> ->
        {{0, 0, 0, 0}, rest}
      << 8 :: 8-little, _ :: 8-little, _ :: 32-little, hour :: 8-little, min :: 8-little, sec :: 8-little, rest :: binary >> ->
        {{hour, min, sec, 0}, rest}
     << 12::8, _ :: 32-little, _ :: 8-little, hour :: 8-little, min :: 8-little, sec :: 8-little, msec :: 32-little, rest :: binary >> ->
        {{hour, min, sec, msec}, rest}
    end
  end

  defp parse_date_packet(packet) do
    case packet do
      << 0 :: 8-little, rest :: binary >> ->
        {{0, 0, 0}, rest}
      << 4 :: 8-little, year :: 16-little, month :: 8-little, day :: 8-little, rest :: binary >> ->
        {{year, month, day}, rest}
    end
  end

  defp parse_datetime_packet(packet) do
    case packet do
      << 0 :: 8-little, rest :: binary >> ->
        {{{0, 0, 0}, {0, 0, 0, 0}}, rest}
      << 4 :: 8-little, year :: 16-little, month :: 8-little, day :: 8-little, rest :: binary >> ->
        {{{year, month, day}, {0, 0, 0, 0}}, rest}
      << 7 :: 8-little, year :: 16-little, month :: 8-little, day :: 8-little, hour :: 8-little, min :: 8-little, sec :: 8-little, rest :: binary >> ->
        {{{year, month, day}, {hour, min, sec, 0}}, rest}
      << 11 :: 8-little, year :: 16-little, month :: 8-little, day :: 8-little, hour :: 8-little, min :: 8-little, sec :: 8-little, msec :: 32-little, rest :: binary >> ->
        {{{year, month, day}, {hour, min, sec, msec}}, rest}
    end
  end

  defp null_map_from_mysql(nullbin) do
    << f :: 1, e :: 1, d :: 1, c :: 1, b :: 1, a ::1, _ :: 2, rest :: binary >> = nullbin
    reversebin = for << x :: 8-bits <- rest >>, into: <<>> do
      << i :: 1, j :: 1, k :: 1, l :: 1, m :: 1, n :: 1, o :: 1, p :: 1 >> = x
      << p :: 1, o :: 1, n :: 1, m :: 1, l :: 1, k :: 1, j :: 1, i :: 1 >>
    end
    << a :: 1, b :: 1, c :: 1, d :: 1, e :: 1, f :: 1, reversebin :: binary >>
  end
end
