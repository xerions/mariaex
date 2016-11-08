defmodule Mariaex.Query do
  @moduledoc """
  Query struct returned from a successfully prepared query. Its fields are:

    * `name` - The name of the prepared statement;
    * `statement` - The prepared statement;
    * `param_formats` - List of formats for each parameters encoded to;
    * `encoders` - List of anonymous functions to encode each parameter;
    * `columns` - The column names;
    * `result_formats` - List of formats for each column is decoded from;
    * `decoders` - List of anonymous functions to decode each column;
    * `types` - The type server table to fetch the type information from;
  """

  defstruct name: "",
            reserved?: false,
            binary_as: nil,
            type: nil,
            statement: "",
            statement_id: nil,
            parameter_types: [],
            types: [],
            connection_ref: nil
end

defimpl DBConnection.Query, for: Mariaex.Query do
  @moduledoc """
  Implementation of `DBConnection.Query` protocol.
  """
  use Bitwise
  import Mariaex.Coder.Utils
  alias Mariaex.Messages
  alias Mariaex.Column

  @doc """
  Parse a query.

  This function is called to parse a query term before it is prepared.
  """
  def parse(%{name: name, statement: statement} = query, _) do
    %{query | name: IO.iodata_to_binary(name), statement: IO.iodata_to_binary(statement)}
  end

  @doc """
  Describe a query.

  This function is called to describe a query after it is prepared.
  """
  def describe(query, _res) do
    query
  end

  @doc """
  Encode parameters using a query.

  This function is called to encode a query before it is executed.
  """
  def encode(%Mariaex.Query{types: nil} = query, _params, _opts) do
    raise ArgumentError, "query #{inspect query} has not been prepared"
  end
  def encode(%Mariaex.Query{type: :binary, parameter_types: parameter_types, binary_as: binary_as} = query, params, _opts) do
    if length(params) == length(parameter_types) do
      parameter_types |> Enum.zip(params) |> parameters_to_binary(binary_as)
    else
      raise ArgumentError, "parameters must be of length #{length params} for query #{inspect query}"
    end
  end
  def encode(%Mariaex.Query{type: :text}, params, _opts) do
    params
  end

  defp parameters_to_binary([], _binary_as), do: <<>>
  defp parameters_to_binary(params, binary_as) do
    set = {<<>>, <<>>, <<>>}
    {nullbits, typesbin, valuesbin} = Enum.reduce(params, set, fn(p, acc) -> encode_params(p, acc, binary_as) end)
    << null_map_to_mysql(nullbits, <<>>) :: binary, 1 :: 8, typesbin :: binary, valuesbin :: binary >>
  end

  defp encode_params({_, param}, {nullbits, typesbin, valuesbin}, binary_as) do
    {nullbit, type, value} = encode_param(param, binary_as)

    types_part = case type do
      :field_type_longlong ->
        # Set the unsigned byte if value > 2^63 (bigint's max signed value).
        if param > 9_223_372_036_854_775_807 do
          << typesbin :: binary, 0x8008 :: 16-little >>
        else
          << typesbin :: binary, 0x08 :: 16-little >>
        end
      _ ->
        << typesbin :: binary, Messages.__type__(:id, type) :: 16-little >>
    end

    {
      << nullbits :: bitstring, nullbit :: 1>>,
      types_part,
      << valuesbin :: binary, value :: binary >>
    }
  end

  defp encode_param(nil, _binary_as),
    do: {1, :field_type_null, ""}
  defp encode_param(bin, binary_as) when is_binary(bin),
    do: {0, binary_as, << to_length_encoded_integer(byte_size(bin)) :: binary, bin :: binary >>}
  defp encode_param(int, _binary_as) when is_integer(int),
    do: {0, :field_type_longlong, << int :: 64-little >>}
  defp encode_param(float, _binary_as) when is_float(float),
    do: {0, :field_type_double, << float :: 64-little-float >>}
  defp encode_param(true, _binary_as),
    do: {0, :field_type_tiny, << 01 >>}
  defp encode_param(false, _binary_as),
    do: {0, :field_type_tiny, << 00 >>}
  defp encode_param(%Decimal{} = value, _binary_as) do
    bin = Decimal.to_string(value, :normal)
    {0, :field_type_newdecimal, << to_length_encoded_integer(byte_size(bin)) :: binary, bin :: binary >>}
  end
  defp encode_param({year, month, day}, _binary_as),
    do: {0, :field_type_date, << 4::8-little, year::16-little, month::8-little, day::8-little>>}
  defp encode_param({hour, min, sec, 0}, _binary_as),
    do: {0, :field_type_time, << 8 :: 8-little, 0 :: 8-little, 0 :: 32-little, hour :: 8-little, min :: 8-little, sec :: 8-little >>}
  defp encode_param({hour, min, sec, msec}, _binary_as),
    do: {0, :field_type_time, << 12 :: 8-little, 0 :: 8-little, 0 :: 32-little, hour :: 8-little, min :: 8-little, sec :: 8-little, msec :: 32-little>>}
  defp encode_param({{year, month, day}, {hour, min, sec, 0}}, _binary_as),
    do: {0, :field_type_datetime, << 7::8-little, year::16-little, month::8-little, day::8-little, hour::8-little, min::8-little, sec::8-little>>}
  defp encode_param({{year, month, day}, {hour, min, sec, msec}}, _binary_as),
    do: {0, :field_type_datetime, <<11::8-little, year::16-little, month::8-little, day::8-little, hour::8-little, min::8-little, sec::8-little, msec::32-little>>}
  defp encode_param(other, _binary_as),
    do: raise ArgumentError, "query has invalid parameter #{inspect other}"

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

  @commands_without_rows [:create, :insert, :replace, :update, :delete, :set,
                          :alter, :rename, :drop, :begin, :commit, :rollback,
                          :savepoint, :execute, :prepare]

  @unsigned_flag 0x20

  def decode(_, %{rows: nil} = res, _), do: res
  def decode(%Mariaex.Query{statement: statement, type: query_type} = query, res_list, opts) when is_list(res_list) do
    res_list
    |> Enum.map(fn result -> decode(query, result, opts) end)
    |> case do # if only 1 result, destructure list
      [x] -> x
      xs -> xs
    end
  end
  def decode(%Mariaex.Query{statement: statement, type: query_type}, {res, types}, opts) do
    command = Mariaex.Protocol.get_command(statement)
    if command in @commands_without_rows do
      %Mariaex.Result{res | command: command, rows: nil}
    else
      mapper = opts[:decode_mapper] || fn x -> x end
      %Mariaex.Result{rows: rows} = res
      types = Enum.reverse(types)
      decoded = do_decode(rows, types, query_type, mapper)
      include_table_name = opts[:include_table_name]
      columns = for %Column{} = column <- types, do: column_name(column, include_table_name)
      %Mariaex.Result{res | command: command,
                            rows: decoded,
                            columns: columns,
                            num_rows: length(decoded)}
    end
  end

  ## helpers

  defp column_name(%Column{name: name, table: table}, true), do: "#{table}.#{name}"
  defp column_name(%Column{name: name}, _), do: name

  def do_decode(rows, types, query_type, mapper \\ fn x -> x end) do
    decode_func = case query_type do
                    :binary -> &decode_bin_rows/2
                    :text -> &decode_text_rows/2
                  end
    rows |> Enum.reduce([], &([(decode_func.(&1, types) |> mapper.()) | &2]))
  end

  def decode_text_rows(unparsed, fields) do
    decode_text_rows(unparsed, [], fields)
  end
  def decode_text_rows(<<>>, row, _) do
    Enum.reverse(row)
  end
  def decode_text_rows(unparsed, row, [this_type | next_types] = types) do
    {raw, next} = Mariaex.Coder.Utils.length_encoded_string(unparsed)
    value = handle_decode_text_rows(Messages.__type__(:type, this_type.type), raw)
    decode_text_rows(next, [value | row], next_types)
  end

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
  def decode_bin_rows(packet, [%Column{type: type, flags: flags} | fields], << 0 :: 1, nullrest :: bits >>, acc) do
    {value, next} = handle_decode_bin_rows(Messages.__type__(:type, type), packet, flags)
    decode_bin_rows(next, fields, nullrest, [value | acc])
  end

  defp handle_decode_text_rows({:string, _mysql_type}, binary), do: binary
  defp handle_decode_text_rows({:integer, _}, binary) do
    {int, ""} = Integer.parse(binary)
    int
  end
  defp handle_decode_text_rows({:float, _}, binary) do
    {float, ""} = Float.parse(binary)
    float
  end
  defp handle_decode_text_rows({:decimal, :field_type_newdecimal}, binary) do
    handle_decode_text_rows({:float, :field_type_newdecimal}, binary)
  end
  defp handle_decode_text_rows({:bit, _}, binary), do: binary
  defp handle_decode_text_rows({:time, :field_type_time}, binary) do
    {:ok, time} = time_from_iso8601(binary)
    time
  end
  defp handle_decode_text_rows({:date, :field_type_date}, binary) do
    {:ok, date} = date_from_iso8601(binary)
    date
  end
  defp handle_decode_text_rows({:timestamp, _}, binary) do
    [date_iso, time_iso] = String.split(binary, ["T", " "])
    {:ok, date} = date_from_iso8601(date_iso)
    {:ok, time} = time_from_iso8601(time_iso)
    {date, time}
  end

  def date_from_iso8601(<<year::4-bytes, ?-, month::2-bytes, ?-, day::2-bytes>>) do
    {year, ""} = Integer.parse(year)
    {month, ""} = Integer.parse(month)
    {day, ""} = Integer.parse(day)
    {:ok, {year, month, day}}
  end

  def time_from_iso8601(<<hour::2-bytes, ?:, min::2-bytes, ?:, sec::2-bytes>>) do
    {hour, ""} = Integer.parse(hour)
    {min, ""} = Integer.parse(min)
    {sec, ""} = Integer.parse(sec)
    {:ok, {hour, min, sec, 0}}
  end



  defp handle_decode_bin_rows({:string, _mysql_type}, packet, _),              do: length_encoded_string(packet)
  defp handle_decode_bin_rows({:integer, :field_type_tiny}, packet, flags),        do: parse_int_packet(packet, 8, flags)
  defp handle_decode_bin_rows({:integer, :field_type_short}, packet, flags),       do: parse_int_packet(packet, 16, flags)
  defp handle_decode_bin_rows({:integer, :field_type_int24}, packet, flags),       do: parse_int_packet(packet, 32, flags)
  defp handle_decode_bin_rows({:integer, :field_type_long}, packet, flags),        do: parse_int_packet(packet, 32, flags)
  defp handle_decode_bin_rows({:integer, :field_type_longlong}, packet, flags),    do: parse_int_packet(packet, 64, flags)
  defp handle_decode_bin_rows({:integer, :field_type_year}, packet, flags),        do: parse_int_packet(packet, 16, flags)
  defp handle_decode_bin_rows({:time, :field_type_time}, packet, _),           do: parse_time_packet(packet)
  defp handle_decode_bin_rows({:date, :field_type_date}, packet, _),           do: parse_date_packet(packet)
  defp handle_decode_bin_rows({:timestamp, :field_type_datetime}, packet, _),  do: parse_datetime_packet(packet)
  defp handle_decode_bin_rows({:timestamp, :field_type_timestamp}, packet, _), do: parse_datetime_packet(packet)
  defp handle_decode_bin_rows({:decimal, :field_type_newdecimal}, packet, _),  do: parse_decimal_packet(packet)
  defp handle_decode_bin_rows({:float, :field_type_float}, packet, _),         do: parse_float_packet(packet, 32)
  defp handle_decode_bin_rows({:float, :field_type_double}, packet, _),        do: parse_float_packet(packet, 64)
  defp handle_decode_bin_rows({:bit, :field_type_bit}, packet, _),             do: parse_bit_packet(packet)

  defp parse_float_packet(packet, size) do
    << value :: size(size)-float-little, rest :: binary >> = packet
    {value, rest}
  end

  defp parse_int_packet(packet, size, flags) when (@unsigned_flag &&& flags) == @unsigned_flag do
    << value :: size(size)-little-unsigned, rest :: binary >> = packet
    {value, rest}
  end

  defp parse_int_packet(packet, size, _flags) do
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

  defp parse_bit_packet(packet) do
    {bitstring, rest} = length_encoded_string(packet)
    ## TODO: implement right decoding of bit string, if somebody will need it.
    {bitstring, rest}
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

defimpl String.Chars, for: Mariaex.Query do
  def to_string(%Mariaex.Query{statement: statement}) do
    IO.iodata_to_binary(statement)
  end
end
