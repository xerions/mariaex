defmodule Mariaex.LruCache do
  @moduledoc false

  def new(size) do
    tab = :ets.new(:cache, [:public])
    :ets.insert(tab, {:__counter__, 0})
    {size, tab}
  end

  def id({_, cache}, statement) do
    try do
      :ets.lookup_element(cache, statement, 3)
    catch
      :error, :badarg ->
        nil
    end
  end

  def lookup({_, cache}, statement) do
    case :ets.match(cache, {statement, :_, :"$1", :"$2", :"$3"}) do
      [[id, ref, num_params]] ->
        increment(cache, statement)
        {id, ref, num_params}
      [] ->
        nil
    end
  end

  def garbage_collect({size, cache}) do
    if :ets.info(cache, :size) >= size do
      take_oldest(cache)
    end
  end

  def take({_, cache}, statement) do
    try do
      :ets.lookup_element(cache, statement, 3)
    catch
      :error, :badarg ->
        nil
    else
      id ->
        :ets.delete(cache, statement)
        id
    end
  end

  def insert_new({_, cache}, statement, id, ref, num_params) do
    :ets.insert_new(cache, {statement, increment(cache), id, ref, num_params})
  end

  def delete({_, cache}, statement) do
    :ets.delete(cache, statement)
  end

  defp take_oldest(cache) do
    {statement, _, id, _, _} = :ets.foldl(fn({:__counter__, _}, acc) ->
                                             acc
                                            (actual, nil) ->
                                             actual
                                            ({_, counter, _, _, _} = actual, {_, min, _, _, _} = acc) ->
                                             if counter < min do actual else acc end
                                          end, nil, cache)
    :ets.delete(cache, statement)
    id
  end

  defp increment(cache, statement) do
    :ets.update_element(cache, statement, {2, increment(cache)})
  end

  defp increment(cache) do
    :ets.update_counter(cache, :__counter__, {2, 1})
  end
end
