defmodule Mariaex.Adapter do
  use Ecto.Adapters.SQL, driver: :mariaex

  @impl true
  def loaders({:map, _}, type),   do: [&json_decode/1, &Ecto.Type.embedded_load(type, &1, :json)]
  def loaders(:map, type),        do: [&json_decode/1, type]
  def loaders(:float, type),      do: [&float_decode/1, type]
  def loaders(:boolean, type),    do: [&bool_decode/1, type]
  def loaders(:binary_id, type),  do: [Ecto.UUID, type]
  def loaders(_, type),           do: [type]

  defp bool_decode(<<0>>), do: {:ok, false}
  defp bool_decode(<<1>>), do: {:ok, true}
  defp bool_decode(<<0::size(1)>>), do: {:ok, false}
  defp bool_decode(<<1::size(1)>>), do: {:ok, true}
  defp bool_decode(0), do: {:ok, false}
  defp bool_decode(1), do: {:ok, true}
  defp bool_decode(x), do: {:ok, x}

  defp float_decode(%Decimal{} = decimal), do: {:ok, Decimal.to_float(decimal)}
  defp float_decode(x), do: {:ok, x}

  defp json_decode(x) when is_binary(x), do: {:ok, Mariaex.json_library().decode!(x)}
  defp json_decode(x), do: {:ok, x}

  @impl true
  def supports_ddl_transaction? do
    false
  end

  @impl true
  def insert(adapter_meta, schema_meta, params, on_conflict, returning, opts) do
    %{source: source, prefix: prefix} = schema_meta
    {_, query_params, _} = on_conflict

    key = primary_key!(schema_meta, returning)
    {fields, values} = :lists.unzip(params)
    sql = @conn.insert(prefix, source, fields, [fields], on_conflict, [], [])
    opts = [{:cache_statement, "ecto_insert_#{source}"} | opts]

    case Ecto.Adapters.SQL.query(adapter_meta, sql, values ++ query_params, opts) do
      {:ok, %{num_rows: 1, last_insert_id: last_insert_id}} ->
        {:ok, last_insert_id(key, last_insert_id)}

      {:ok, %{num_rows: 2, last_insert_id: last_insert_id}} ->
        {:ok, last_insert_id(key, last_insert_id)}

      {:error, err} ->
        case @conn.to_constraints(err, source: source) do
          []          -> raise err
          constraints -> {:invalid, constraints}
        end
    end
  end

  @impl true
  def lock_for_migrations(meta, opts, fun) do
    %{opts: adapter_opts, repo: repo} = meta

    if Keyword.get(adapter_opts, :migration_lock, true) do
      if Keyword.fetch(adapter_opts, :pool_size) == {:ok, 1} do
        Ecto.Adapters.SQL.raise_migration_pool_size_error()
      end

      opts = opts ++ [log: false, timeout: :infinity]
      {:ok, result} =
        transaction(meta, opts, fn ->
          lock_name = "\"ecto_#{inspect(repo)}\""
          try do
            {:ok, _} = Ecto.Adapters.SQL.query(meta, "SELECT GET_LOCK(#{lock_name}, -1)", [], opts)
            fun.()
          after
            {:ok, _} = Ecto.Adapters.SQL.query(meta, "SELECT RELEASE_LOCK(#{lock_name})", [], opts)
          end
        end)
      result
    else
      fun.()
    end
  end  

  defp primary_key!(%{autogenerate_id: {_, key, _type}}, [key]), do: key
  defp primary_key!(_, []), do: nil
  defp primary_key!(%{schema: schema}, returning) do
    raise ArgumentError, "MySQL does not support :read_after_writes in schemas for non-primary keys. " <>
                         "The following fields in #{inspect schema} are tagged as such: #{inspect returning}"
  end

  defp last_insert_id(nil, _last_insert_id), do: []
  defp last_insert_id(_key, 0), do: []
  defp last_insert_id(key, last_insert_id), do: [{key, last_insert_id}]
end
