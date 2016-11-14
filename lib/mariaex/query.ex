defmodule Mariaex.Query do
  @moduledoc """
  Query struct returned from a successfully prepared query. Its fields are:

    * `name` - The name of the prepared statement;
    * `statement` - The prepared statement;
    * `num_params` - The number of parameters;
    * `ref` - Reference that uniquely identifies when the query was prepared;
  """

  defstruct name: "",
            reserved?: false,
            binary_as: nil,
            type: nil,
            statement: "",
            num_params: nil,
            ref: nil
end

defimpl DBConnection.Query, for: Mariaex.Query do
  @moduledoc """
  Implementation of `DBConnection.Query` protocol.
  """

  use Bitwise
  import Mariaex.Coder.Utils
  alias Mariaex.Messages
  alias Mariaex.Column
  alias Mariaex.RowParser

  @doc """
  Parse a query.

  This function is called to parse a query term before it is prepared.
  """
  def parse(%{name: name, statement: statement, ref: nil} = query, _) do
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
  def encode(%Mariaex.Query{ref: nil} = query, _params, _opts) do
    raise ArgumentError, "query #{inspect query} has not been prepared"
  end
  def encode(%Mariaex.Query{num_params: num_params} = query, params, _opts)
      when length(params) != num_params do
    raise ArgumentError, "parameters must be of length #{num_params} for query #{inspect query}"
  end
  def encode(%Mariaex.Query{type: :binary, binary_as: binary_as}, params, _opts) do
    parameters_to_binary(params, binary_as)
  end
  def encode(%Mariaex.Query{type: :text}, params, _opts) do
    params
  end

  defp parameters_to_binary([], _binary_as), do: <<>>
  defp parameters_to_binary(params, binary_as) do
    set = {0, 0, <<>>, <<>>}
    {nullint, len, typesbin, valuesbin} = Enum.reduce(params, set, fn(p, acc) -> encode_params(p, acc, binary_as) end)
    nullbin_size = div(len + 7, 8)
    << nullint :: size(nullbin_size)-little-unit(8), 1 :: 8, typesbin :: binary, valuesbin :: binary >>
  end

  defp encode_params(param, {nullint, idx, typesbin, valuesbin}, binary_as) do
    {nullvalue, type, value} = encode_param(param, binary_as)

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
      nullint ||| (nullvalue <<< idx),
      idx + 1,
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

  @commands_without_rows [:create, :insert, :replace, :update, :delete, :set,
                          :alter, :rename, :drop, :begin, :commit, :rollback,
                          :savepoint, :execute, :prepare, :truncate]
  @unsigned_flag 0x20

  def decode(_, %{rows: nil} = res, _), do: res
  def decode(%Mariaex.Query{statement: statement}, {res, types}, opts) do
    command = Mariaex.Protocol.get_command(statement)
    if command in @commands_without_rows do
      %Mariaex.Result{res | command: command, rows: nil}
    else
      mapper = opts[:decode_mapper] || fn x -> x end
      %Mariaex.Result{rows: rows} = res
      decoded = do_decode(rows, types, mapper)
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

  def do_decode(_, columns, mapper \\ fn x -> x end)
  def do_decode(rows, columns, mapper) do
    row_types = columns
    |> Enum.map(fn(column) ->
      Messages.__type__(:type, column.type)
      |> type_to_atom(column.flags)
    end)

    nullbin_size = div(length(row_types) + 7 + 2, 8)

    rows
    |> Enum.reduce([], fn(row, acc) ->
      decoded_row = row
      |> RowParser.decode_bin_rows(row_types, nullbin_size)
      |> mapper.()

      [decoded_row | acc]
    end)
  end

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
end

defimpl String.Chars, for: Mariaex.Query do
  def to_string(%Mariaex.Query{statement: statement}) do
    IO.iodata_to_binary(statement)
  end
end
