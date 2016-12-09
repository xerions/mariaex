defmodule Mariaex.RowParser do
  @moduledoc """
  Parse a row of the MySQL protocol

  This parser makes extensive use of binary pattern matching and recursion to take advantage
  of Erlang's optimizer that will not create sub binaries when called recusively.
  """
  use Bitwise
  alias Mariaex.Column
  alias Mariaex.Messages

  @unsigned_flag 0x20

  def decode_init(columns) do
    fields =
      for %Column{type: type, flags: flags} <- columns do
        Messages.__type__(:type, type)
        |> type_to_atom(flags)
      end
    {fields, div(length(fields) + 7 + 2, 8)}
  end

  def decode_bin_rows(row, fields, nullint) do
    decode_bin_rows(row, fields, nullint >>> 2, [])
  end

  ## Helpers

  defp type_to_atom({:integer, :field_type_tiny}, flags) when (@unsigned_flag &&& flags) == @unsigned_flag do
    :uint8
  end

  defp type_to_atom({:integer, :field_type_tiny}, _) do
    :int8
  end

  defp type_to_atom({:integer, :field_type_short}, flags) when (@unsigned_flag &&& flags) == @unsigned_flag do
    :uint16
  end

  defp type_to_atom({:integer, :field_type_short}, _) do
    :int16
  end

  defp type_to_atom({:integer, :field_type_int24}, flags) when (@unsigned_flag &&& flags) == @unsigned_flag do
    :uint32
  end

  defp type_to_atom({:integer, :field_type_int24}, _) do
    :int32
  end

  defp type_to_atom({:integer, :field_type_long}, flags) when (@unsigned_flag &&& flags) == @unsigned_flag do
    :uint32
  end

  defp type_to_atom({:integer, :field_type_long}, _) do
    :int32
  end

  defp type_to_atom({:integer, :field_type_longlong}, flags) when (@unsigned_flag &&& flags) == @unsigned_flag do
    :uint64
  end

  defp type_to_atom({:integer, :field_type_longlong}, _) do
    :int64
  end

  defp type_to_atom({:string, _mysql_type}, _),              do: :string
  defp type_to_atom({:integer, :field_type_year}, _),        do: :uint16
  defp type_to_atom({:time, :field_type_time}, _),           do: :time
  defp type_to_atom({:date, :field_type_date}, _),           do: :date
  defp type_to_atom({:timestamp, :field_type_datetime}, _),  do: :datetime
  defp type_to_atom({:timestamp, :field_type_timestamp}, _), do: :datetime
  defp type_to_atom({:decimal, :field_type_newdecimal}, _),  do: :decimal
  defp type_to_atom({:float, :field_type_float}, _),         do: :float32
  defp type_to_atom({:float, :field_type_double}, _),        do: :float64
  defp type_to_atom({:bit, :field_type_bit}, _),             do: :bit
  defp type_to_atom({:null, :field_type_null}, _),           do: nil

  defp decode_bin_rows(<<rest::bits>>, [_ | fields], nullint, acc) when (nullint &&& 1) === 1 do
    decode_bin_rows(rest, fields, nullint >>> 1, [nil | acc])
  end

  defp decode_bin_rows(<<rest::bits>>, [:string | fields], null_bitfield,  acc) do
    decode_string(rest, fields, null_bitfield >>> 1, acc)
  end

  defp decode_bin_rows(<<rest::bits>>, [:uint8 | fields], null_bitfield, acc) do
    decode_uint8(rest, fields, null_bitfield >>> 1, acc)
  end

  defp decode_bin_rows(<<rest::bits>>, [:int8 | fields], null_bitfield, acc) do
    decode_int8(rest, fields, null_bitfield >>> 1, acc)
  end

  defp decode_bin_rows(<<rest::bits>>, [:uint16 | fields], null_bitfield, acc) do
    decode_uint16(rest, fields, null_bitfield >>> 1, acc)
  end

  defp decode_bin_rows(<<rest::bits>>, [:int16 | fields], null_bitfield, acc) do
    decode_int16(rest, fields, null_bitfield >>> 1, acc)
  end

  defp decode_bin_rows(<<rest::bits>>, [:uint32 | fields], null_bitfield, acc) do
    decode_uint32(rest, fields, null_bitfield >>> 1, acc)
  end

  defp decode_bin_rows(<<rest::bits>>, [:int32 | fields], null_bitfield, acc) do
    decode_int32(rest, fields, null_bitfield >>> 1, acc)
  end

  defp decode_bin_rows(<<rest::bits>>, [:uint64 | fields], null_bitfield, acc) do
    decode_uint64(rest, fields, null_bitfield >>> 1, acc)
  end

  defp decode_bin_rows(<<rest::bits>>, [:int64 | fields], null_bitfield, acc) do
    decode_int64(rest, fields, null_bitfield >>> 1, acc)
  end

  defp decode_bin_rows(<<rest::bits>>, [:time | fields], null_bitfield, acc) do
    decode_time(rest, fields, null_bitfield >>> 1, acc)
  end

  defp decode_bin_rows(<<rest::bits>>, [:date | fields], null_bitfield, acc) do
    decode_date(rest, fields, null_bitfield >>> 1, acc)
  end

  defp decode_bin_rows(<<rest::bits>>, [:datetime | fields], null_bitfield, acc) do
    decode_datetime(rest, fields, null_bitfield >>> 1, acc)
  end

  defp decode_bin_rows(<<rest::bits>>, [:decimal | fields], null_bitfield, acc) do
    decode_decimal(rest, fields, null_bitfield >>> 1, acc)
  end

  defp decode_bin_rows(<<rest::bits>>, [:float32 | fields], null_bitfield, acc) do
    decode_float32(rest, fields, null_bitfield >>> 1, acc)
  end

  defp decode_bin_rows(<<rest::bits>>, [:float64 | fields], null_bitfield, acc) do
    decode_float64(rest, fields, null_bitfield >>> 1, acc)
  end

  defp decode_bin_rows(<<rest::bits>>, [:bit | fields], null_bitfield, acc) do
    decode_string(rest, fields, null_bitfield >>> 1, acc)
  end

  defp decode_bin_rows(<<rest::bits>>, [:nil | fields], null_bitfield, acc) do
    decode_bin_rows(rest, fields, null_bitfield >>> 1, [nil | acc])
  end

  defp decode_bin_rows(<<>>, [], _, acc) do
    Enum.reverse(acc)
  end

  defp decode_string(<<len::8, string::size(len)-binary, rest::bits>>, fields, nullint,  acc) when len <= 250 do
    decode_bin_rows(rest, fields, nullint,  [string | acc])
  end

  defp decode_string(<<252::8, len::16-little, string::size(len)-binary, rest::bits>>, fields, nullint,  acc) do
    decode_bin_rows(rest, fields, nullint,  [string | acc])
  end

  defp decode_string(<<253::8, len::24-little, string::size(len)-binary, rest::bits>>, fields, nullint,  acc) do
    decode_bin_rows(rest, fields, nullint,  [string | acc])
  end

  defp decode_string(<<254::8, len::64-little, string::size(len)-binary, rest::bits>>,  fields, nullint,  acc) do
    decode_bin_rows(rest, fields, nullint,  [string | acc])
  end

  defp decode_float32(<<value::size(32)-float-little, rest::bits>>, fields, null_bitfield, acc) do
    decode_bin_rows(rest, fields, null_bitfield, [value | acc])
  end

  defp decode_float64(<<value::size(64)-float-little, rest::bits>>, fields, null_bitfield, acc) do
    decode_bin_rows(rest, fields, null_bitfield, [value | acc])
  end

  defp decode_uint8(<<value::size(8)-little-unsigned, rest::bits>>,
                    fields, null_bitfield, acc) do
    decode_bin_rows(rest, fields, null_bitfield , [value | acc])
  end

  defp decode_int8(<<value::size(8)-little-signed, rest::bits>>,
                   fields, null_bitfield, acc) do
    decode_bin_rows(rest, fields, null_bitfield , [value | acc])
  end

  defp decode_uint16(<<value::size(16)-little-unsigned, rest::bits>>,
                     fields, null_bitfield, acc) do
    decode_bin_rows(rest, fields, null_bitfield , [value | acc])
  end

  defp decode_int16(<<value::size(16)-little-signed, rest::bits>>,
                    fields, null_bitfield, acc) do
    decode_bin_rows(rest, fields, null_bitfield , [value | acc])
  end

  defp decode_uint32(<<value::size(32)-little-unsigned, rest::bits>>,
                     fields, null_bitfield, acc) do
    decode_bin_rows(rest, fields, null_bitfield , [value | acc])
  end

  defp decode_int32(<<value::size(32)-little-signed, rest::bits>>,
                    fields, null_bitfield, acc) do
    decode_bin_rows(rest, fields, null_bitfield , [value | acc])
  end

  defp decode_uint64(<<value::size(64)-little-unsigned, rest::bits>>,
                     fields, null_bitfield, acc) do
    decode_bin_rows(rest, fields, null_bitfield , [value | acc])
  end

  defp decode_int64(<<value::size(64)-little-signed, rest::bits>>,
                    fields, null_bitfield, acc) do
    decode_bin_rows(rest, fields, null_bitfield , [value | acc])
  end

  defp decode_decimal(<<length,  raw_value::size(length)-little-binary, rest::bits>>,
                      fields, null_bitfield, acc) do
    value = Decimal.new(raw_value)
    decode_bin_rows(rest, fields, null_bitfield, [value | acc])
  end

  defp decode_time(<< 0::8-little, rest::bits>>,
                   fields, null_bitfield, acc) do
    decode_bin_rows(rest, fields, null_bitfield, [{0, 0, 0, 0} | acc])
  end

  defp decode_time(<<8::8-little, _::8-little, _::32-little, hour::8-little, min::8-little, sec::8-little, rest::bits>>,
                   fields, null_bitfield, acc) do
    decode_bin_rows(rest, fields, null_bitfield, [{hour, min, sec, 0} | acc])
  end

  defp decode_time(<< 12::8, _::32-little, _::8-little, hour::8-little, min::8-little, sec::8-little, msec::32-little, rest::bits >>,
                   fields, null_bitfield, acc) do

    decode_bin_rows(rest, fields, null_bitfield, [{hour, min, sec, msec} | acc])
  end

  defp decode_date(<< 0::8-little, rest::bits >>,
                   fields, null_bitfield, acc) do
    decode_bin_rows(rest, fields, null_bitfield, [{0, 0, 0} | acc])
  end

  defp decode_date(<< 4::8-little, year::16-little, month::8-little, day::8-little, rest::bits >>,
                   fields, null_bitfield, acc) do

    decode_bin_rows(rest, fields, null_bitfield, [{year, month, day} | acc])
  end

  defp decode_datetime(<< 0::8-little, rest::bits >>,
                       fields, null_bitfield, acc) do
    decode_bin_rows(rest, fields, null_bitfield, [{{0, 0, 0}, {0, 0, 0, 0}} | acc])
  end

  defp decode_datetime(<<4::8-little, year::16-little, month::8-little, day::8-little, rest::bits >>,
                       fields, null_bitfield, acc) do
    decode_bin_rows(rest, fields, null_bitfield, [{{year, month, day}, {0, 0, 0, 0}} | acc])
  end

  defp decode_datetime(<< 7::8-little, year::16-little, month::8-little, day::8-little, hour::8-little, min::8-little, sec::8-little, rest::bits >>,
                       fields, null_bitfield, acc) do
    decode_bin_rows(rest, fields, null_bitfield, [{{year, month, day}, {hour, min, sec, 0}} | acc])
  end

  defp decode_datetime(<<11::8-little, year::16-little, month::8-little, day::8-little, hour::8-little, min::8-little, sec::8-little, msec::32-little, rest::bits >>,
                       fields, null_bitfield, acc) do
    decode_bin_rows(rest, fields, null_bitfield, [{{year, month, day}, {hour, min, sec, msec}} | acc])
  end

  ### TEXT ROW PARSER

  def decode_text_init(columns) do
    for %Column{type: type, flags: flags} <- columns do
      Messages.__type__(:type, type)
      |> type_to_atom(flags)
    end
  end

  def decode_text_rows(binary, fields) do
    decode_text_part(binary, fields, [])
  end

  ### IMPLEMENTATION

  defp decode_text_part(<<len::8, string::size(len)-binary, rest::bits>>, fields, acc) when len <= 250 do
    decode_text_rows(string, rest, fields, acc)
  end

  defp decode_text_part(<<252::8, len::16-little, string::size(len)-binary, rest::bits>>, fields, acc) do
    decode_text_rows(string, rest, fields, acc)
  end

  defp decode_text_part(<<253::8, len::24-little, string::size(len)-binary, rest::bits>>, fields, acc) do
    decode_text_rows(string, rest, fields, acc)
  end

  defp decode_text_part(<<254::8, len::64-little, string::size(len)-binary, rest::bits>>, fields, acc) do
    decode_text_rows(string, rest, fields, acc)
  end

  defp decode_text_part(<<>>, [], acc) do
    Enum.reverse(acc)
  end

  defp decode_text_rows(string, rest, [:string | fields], acc) do
    decode_text_part(rest, fields, [string | acc])
  end

  defp decode_text_rows(string, rest, [type | fields], acc)
   when type in [:uint8, :int8, :uint16, :int16, :uint32, :int32, :uint64, :int64] do
    decode_text_part(rest, fields, [:erlang.binary_to_integer(string) | acc])
  end

  defp decode_text_rows(string, rest, [type | fields], acc)
   when type in [:float32, :float64, :decimal] do
    decode_text_part(rest, fields, [:erlang.binary_to_float(string) | acc])
  end

  defp decode_text_rows(string, rest, [:bit | fields], acc) do
    decode_text_part(rest, fields, [string | acc])
  end

  defp decode_text_rows(string, rest, [:time | fields], acc) do
    decode_text_time(string, rest, fields, acc)
  end

  defp decode_text_rows(string, rest, [:date | fields], acc) do
    decode_text_date(string, rest, fields, acc)
  end

  defp decode_text_rows(string, rest, [:datetime | fields], acc) do
    decode_text_datetime(string, rest, fields, acc)
  end

  defmacrop to_int(value) do
    quote do: :erlang.binary_to_integer(unquote(value))
  end

  defp decode_text_date(<<year::4-bytes, ?-, month::2-bytes, ?-, day::2-bytes>>, rest, fields, acc) do
    decode_text_part(rest, fields, [{to_int(year), to_int(month), to_int(day)} | acc])
  end

  defp decode_text_time(<<hour::2-bytes, ?:, min::2-bytes, ?:, sec::2-bytes>>, rest, fields, acc) do
    decode_text_part(rest, fields, [{to_int(hour), to_int(min), to_int(sec), 0} | acc])
  end

  defp decode_text_datetime(<<year::4-bytes, ?-, month::2-bytes, ?-, day::2-bytes,
    _::8-little, hour::2-bytes, ?:, min::2-bytes, ?:, sec::2-bytes>>, rest, fields, acc) do
    decode_text_part(rest, fields, [{{to_int(year), to_int(month), to_int(day)}, {to_int(hour), to_int(min), to_int(sec), 0}} | acc])
  end
end
