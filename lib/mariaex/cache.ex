defmodule Mariaex.Cache do
  import :os, only: [timestamp: 0]

  def new(size) do
    {size, :ets.new(:cache, [])}
  end

  def lookup({_, cache}, statement) do
    case :ets.lookup(cache, statement) do
      [{_, _, info}] -> info
      _ -> nil
    end
  end

  def delete({_, cache}, statement, cleanup) do
    case :ets.lookup(cache, statement) do
      [{_, _, info}] ->
        :ets.delete(cache, statement)
        cleanup.(statement, info)
      _ ->
        nil
    end
  end

  def insert({size, cache}, statement, data, cleanup) do
    if :ets.info(cache, :size) > size, do: remove_oldest(cache, cleanup)
    :ets.insert(cache, {statement, timestamp, data})
  end

  def update({_, cache}, statement, data) do
    :ets.insert(cache, {statement, timestamp, data})
  end

  defp remove_oldest(cache, cleanup) do
    {statement, _, data} = :ets.foldl(fn({statement, timestamp, data}, nil) ->
                                          {statement, timestamp, data}
                                        ({_, timestamp, _} = actual, {_, acc_timestamp, _} = acc) ->
                                          if timestamp < acc_timestamp do actual else acc end
                                      end, nil, cache)
    cleanup.(statement, data)
    :ets.delete(cache, statement)
  end
end
