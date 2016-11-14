defmodule Mariaex.Cache do
  @moduledoc false

  def new() do
    :ets.new(:cache, [:public])
  end

  def id(cache, name) do
    try do
      :ets.lookup_element(cache, name, 2)
    catch
      :error, :badarg ->
        nil
    end
  end

  def lookup(cache, name) do
    case :ets.match(cache, {name, :"$1", :"$2"}) do
      [[id, ref]] ->
        {id, ref}
      [] ->
        nil
    end
  end

  def take(cache, name) do
    case :ets.take(cache, name) do
      [{_, id, _}] -> id
      []           -> nil
    end
  end

  def insert_new(cache, name, id, ref) do
    :ets.insert_new(cache, {name, id, ref})
  end

  def delete(cache, name) do
    :ets.delete(cache, name)
  end
end
