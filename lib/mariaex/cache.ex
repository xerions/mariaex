defmodule Mariaex.Cache do
  @moduledoc """
  Simple cache for named queries
  """

  @doc """
  Create cache
  """
  def new() do
    :ets.new(:cache, [:public])
  end

  @doc """
  Lookup the named query in cache
  """
  def lookup(cache, name) do
    case :ets.lookup(cache, name) do
      [{_, info}] -> info
      _ -> nil
    end
  end

  @doc """
  Delete query, which get the cleanup fun to cleanup the actual prepared query
  """
  def delete(cache, name, cleanup) do
    case :ets.lookup(cache, name) do
      [{_, info}] ->
        :ets.delete(cache, name)
        cleanup.(name, info)
      _ ->
        nil
    end
  end

  @doc """
  Inserts the named queries with associated data.
  """
  def insert(cache, name, data) do
    :ets.insert(cache, {name, data})
  end
end
