defmodule Mariaex.Result do
  @moduledoc """
  Result struct returned from any successful query. Its fields are:

    * `columns` - The column names;
    * `rows` - The result set. A list of lists, each list corresponding to a
      row, each element of the inner list corresponds to a column value;
    * `num_rows` - The number of fetched or affected rows;
  """

  @type t :: %__MODULE__{
    columns:  [String.t] | nil,
    rows:     [[any]] | nil,
    last_insert_id: integer,
    num_rows: integer,
    connection_id: nil}

  defstruct [:columns, :rows, :last_insert_id, :num_rows, :connection_id]
end

defmodule Mariaex.Error do
  defexception [:message, :tag, :action, :reason, :mariadb, :connection_id]

  def message(e) do
    cond do
      kw = e.mariadb ->
        "(#{kw[:code]}): #{kw[:message]}"
      tag = e.tag ->
        "[#{tag}] `#{e.action}` failed with: #{inspect e.reason}"
      true ->
        e.message || ""
    end
  end
end

defmodule Mariaex.Cursor do
  @moduledoc false
  defstruct [:ref, :statement_id]
end
