defmodule Mariaex.Cache do

  def new do
    :ets.new(:cache, [])
  end

  def lookup(cache, statement) do
    :ets.lookup(cache, statement)
  end

  def delete(cache, statement) do
    :ets.delete(cache, statement)
  end
  
  def insert(cache, data) do
    :ets.insert(cache, data)
  end

  def insert(cache, data, cache_size) do
    {mega, secs, _} = :erlang.now
    data = Tuple.insert_at(data, 2, mega * 1000000 + secs) |> Tuple.insert_at(4, [])
    current_cache_size = :ets.foldl(fn(_data, acc) -> acc + 1 end, 0, cache)
    clean_cache = case current_cache_size >= cache_size do
                    true ->
                      :ets.foldl(fn(rec, acc) ->
                        old_st = elem(rec, 0)
                        old_id = elem(rec, 1)
                        old_time = elem(rec, 2)
                        old_sock = elem(rec, 5)
                        acc = case acc do
                                0 ->
                                  {old_st, old_time, old_id, old_sock}
                                {st, time, id, sock} ->
                                  case time > old_time do
                                    true ->  {old_st, old_time, old_id, old_sock}
                                    false -> {st, time, id, sock}
                                  end
                              end
                        acc
                      end, 0, cache)
                    false ->
                      :ets.insert(cache, data)
                  end

    case clean_cache do
      {st, _timestamp, id, sock} ->
        # remove the oldest query
        delete(cache, st)
        Mariaex.Protocol.close_statement(%{statement_id: id, statement: nil, sock: sock})
        # insert new query
        :ets.insert(cache, data)
      _ ->
        :ok
    end
  end

  def update(cache, statement, parameters_types) do
    case Mariaex.Cache.lookup(cache, statement) do
      [{statement, id, timestamp, num_params, _types, sock}] ->
        Mariaex.Cache.delete(cache, statement)
        Mariaex.Cache.insert(cache, {statement, id, timestamp, num_params, parameters_types, sock})
      _ ->
        :new_query
    end
  end

end
