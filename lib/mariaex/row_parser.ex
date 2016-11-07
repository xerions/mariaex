defmodule Mariaex.RowParser do
  @moduledoc """
  Parse a row of the MySQL protocol

  This parser makes extensive use of binary pattern matching and recursion to take advantage
  of Erlang's optimizer that will not create sub binaries when called recusively.
  """
  use Bitwise

  def decode_bin_rows(packet, fields, nullbin_size) do
    << 0 :: 8, null_bitfield :: size(nullbin_size)-little-unit(8), rest :: binary >> = packet
    decode_bin_rows(rest, fields, null_bitfield >>> 2, [])
  end

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
end
