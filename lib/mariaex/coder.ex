defmodule Mariaex.Coder do
  @moduledoc """
  Declarative generator for MySQL protocol messages, which can generate based on declarative description
  decoder and encoder.

  Example:

        defcoder :text_cmd do
          command 1
          statement :string_eof
        end

  Will generate 2 functions:

      __encode__({:text_cmd, 0x0e, "test"}) # => <<14, 116, 101, 115, 116>>

      __decode__(:text_cmd, <<14, 116, 101, 115, 116>>) # => {:text_cmd, 14, "test"}

  Additionally it generates record, like `Record.record(:text_cmd, [:command, :statement])`,
  so that you can use it to create commands or access information in it.

  Example would be: `text_cmd(command: 14, statement: "test")`

  Check `Mariaex.Messages` for more examples.

  For now, there is possible to insert custom functions for decoding of data. Example is in handshake
  command:

  See definition and implementation:

      `auth_plugin_data2: {__MODULE__, auth_plugin_data2}`

  It is used only for decoding, but it may change in the future for encoding.
  """
  defmacro __using__(_opts) do
    quote do
      import Mariaex.Coder, only: [defcoder: 2]
      import Record, only: [defrecord: 2]
      import Mariaex.Coder.Utils

      @before_compile unquote(__MODULE__)
      Module.register_attribute(__MODULE__, :decoders, accumulate: true)
      Module.register_attribute(__MODULE__, :encoders, accumulate: true)
    end
  end

  defmacro __before_compile__(env) do
    decoders = Enum.reverse Module.get_attribute(env.module, :decoders)
    encoders = Enum.reverse Module.get_attribute(env.module, :encoders)
    [for {type, function} <- decoders do
      quote do
        def __decode__(unquote(type), body), do: unquote(function)(body)
      end
    end,
    for {type, function} <- encoders do
      quote do
        def __encode__(unquote(type)() = rec), do: unquote(function)(rec)
      end
    end]
  end

  defmacro defcoder(name, [do: spec]) do
    spec = case spec do
             {:__block__, _meta, spec} -> spec
             spec -> [spec]
           end
    keys = for {key, _, _} <- spec, key != :_, do: key
    decoder = split_to_stages(spec) |> gen_stages(name, keys)
    encoder = gen_encoder(name, spec, keys)
    quote do
      defrecord unquote(name), unquote(keys)
      unquote(decoder)
      unquote(encoder)
    end
  end

  def gen_encoder(name, spec, keys) do
    function = ("encode_" <> Atom.to_string(name)) |> String.to_atom
    quote do
      Module.put_attribute __MODULE__, :encoders, {unquote(name), unquote(function)}
      def unquote(function)(unquote(name)(unquote(for key <- keys, do: {key, Macro.var(key, nil)}))) do
        unquote({:<<>>, [], Enum.flat_map(spec, &match(&1, :encode))})
      end
    end
  end

  @empty_stage %{head: [], body: nil}

  defp split_to_stages(spec) do
    {last, other} = Enum.reduce(spec, {@empty_stage, []}, fn(kv = {_key, _, [value | _]}, {actual = %{head: head, body: _body}, all}) ->
      cond do
        is_integer(value) ->
          {%{actual | head: [kv | head]}, all}
        true ->
          {@empty_stage, [%{actual | head: Enum.reverse([:next | head]), body: kv} | all]}
      end
    end)
    case last do
      %{head: [], body: nil} -> other
      _ -> [%{last | head: Enum.reverse(last.head) } | other]
    end |> Enum.reverse
  end

  defp gen_stages(allspec, name, keys) do
    matches = gen_matches(allspec, keys)
    function = ("decode_" <> Atom.to_string(name)) |> String.to_atom
    quote do
      Module.put_attribute __MODULE__, :decoders, {unquote(name), unquote(function)}
      def unquote(function)(next) do
        unquote_splicing(matches)
        unquote(name)(unquote(for key <- keys, do: {key, Macro.var(key, nil)}))
      end
    end
  end

  defp gen_matches(allspec, keys) do
    for spec <- allspec do
      body = gen_body(spec[:body], keys)
      quoted_head = case spec[:head] do
        [:next] ->
          []
        head ->
          binary_match = {:<<>>, [], Enum.flat_map(head, &match(&1, :decode))}
          [(quote do: unquote(binary_match) = next)]
      end
      quoted_head ++ [body]
    end |> List.flatten
  end

  defp gen_body({key, _, [:length_string]}, _) do
    quote do
      <<length :: size(8)-little, unquote(Macro.var(key, nil)) :: size(length)-binary, next :: binary>> = next
    end
  end

  defp gen_body({key, _, [{module, function}]}, _) do
    quote do: {unquote(Macro.var(key, nil)), next} = apply(unquote(module), unquote(function), [next])
  end

  defp gen_body({key, _, [:length_encoded_integer]}, _) do
    quote do: {unquote(Macro.var(key, nil)), next} = length_encoded_integer(next)
  end

  defp gen_body({key, _, [:length_encoded_string]}, _) do
    quote do: {unquote(Macro.var(key, nil)), next} = length_encoded_string(next)
  end

  defp gen_body({key, _, [:length_encoded_string, :until_eof]}, _) do
    quote do: unquote(Macro.var(key, nil)) = length_encoded_string_eof(next)
  end

  defp gen_body({key, _, [:string]}, _) do
    quote do: [unquote(Macro.var(key, nil)), next] = :binary.split(next, <<0>>)
  end

  defp gen_body({key, _, [:string_eof]}, _) do
    quote do: unquote(Macro.var(key, nil)) = next
  end

  defp gen_body({key, _, [function]}, _keys) do
    quote do
      size = unquote(function) * 8
      <<unquote(Macro.var(key, nil)) :: size(size), next :: binary>> = next
    end
  end

  defp gen_body({key, _, [function, :string]}, _keys) do
    quote do
      size = unquote(function)
      <<unquote(Macro.var(key, nil)) :: size(size)-binary, next :: binary>> = next
    end
  end
  defp gen_body(nil, _keys) do
    []
  end

  defp match({:_, _, [length]}, _) when is_integer(length) do
    [quote do: 0 :: unquote(length)*8]
  end
  defp match({key, _, [length]}, _) when is_integer(length) do
    [quote do: unquote(Macro.var(key, nil)) :: size(unquote(length*8))-little]
  end
  defp match({key, _, [length, :string]}, _) do
    [quote do: unquote(Macro.var(key, nil)) :: size(unquote(length))-binary]
  end
  defp match(:next, _) do
    [quote do: next :: binary]
  end
  defp match({key, _, [:string]}, _) do
    [(quote do: unquote(Macro.var(key, nil)) :: binary),
     (quote do: 0 :: 8)]
  end
  defp match({key, _, [:length_string]}, :encode) do
    [(quote do: byte_size(unquote(Macro.var(key, nil))) :: 8 ),
     (quote do: unquote(Macro.var(key, nil)) :: binary)]
  end
  defp match({key, _, [:string_eof]}, :encode) do
    [(quote do: unquote(Macro.var(key, nil)) :: binary)]
  end
  # this clauses are wrong, because it is imposible to generate this kind of integer in a binary match
  defp match({key, _, [:length_encoded_integer]}, :encode) do
    [(quote do: unquote(Macro.var(key, nil)) :: integer)]
  end
  defp match({key, _, [:length_encoded_string | _]}, :encode) do
    [(quote do: unquote(Macro.var(key, nil)) :: binary)]
  end
  # All custom implementations are ignored yet
  defp match({key, _, [{_module, _function}]}, :encode) do
    [(quote do: unquote(Macro.var(key, nil)) :: binary)]
  end

  defmodule Utils do
    def length_encoded_string(bin) do
      {length, next} = length_encoded_integer(bin)
      << string :: size(length)-binary, next :: binary >> = next
      {string, next}
    end

    def length_encoded_string_eof(bin, acc \\ []) do
      case length_encoded_string(bin) do
        {value, ""} ->
          Enum.reverse([value | acc])
        {value, rest} ->
          length_encoded_string_eof(rest, [value | acc])
      end
    end

    def length_encoded_integer(bin) do
      case bin do
        << value :: 8, rest :: binary >> when value <= 250 -> {value, rest}
        << 252 :: 8, value :: 16-little, rest :: bits >> -> {value, rest}
        << 253 :: 8, value :: 24-little, rest :: bits >> -> {value, rest}
        << 254 :: 8, value :: 64-little, rest :: bits >> -> {value, rest}
      end
    end

    def to_length_encoded_integer(int) do
      case int do
        int when int <= 250 -> << int :: 8 >>
        int when int <= 65535 -> << 252 :: 8, int :: 16-little >>
        int when int <= 16777215 -> << 253 :: 8, int :: 24-little >>
        int -> << 254 :: 8, int :: 64-little >>
      end
    end
  end
end
