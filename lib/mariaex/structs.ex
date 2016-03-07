defmodule Mariaex.Result do
  @moduledoc """
  Result struct returned from any successful query. Its fields are:

    * `command` - An atom of the query command, for example: `:select` or
                  `:insert`;
    * `columns` - The column names;
    * `rows` - The result set. A list of tuples, each tuple corresponding to a
               row, each element in the tuple corresponds to a column;
    * `num_rows` - The number of fetched or affected rows;
  """

  @type t :: %__MODULE__{
    command:  atom,
    columns:  [String.t] | nil,
    rows:     [tuple] | nil,
    last_insert_id: integer,
    num_rows: integer,
    connection_id: nil}

  defstruct [:command, :columns, :rows, :last_insert_id, :num_rows, :connection_id]
end

defmodule Mariaex.Error do
  defexception [:message, :mariadb, :connection_id]

  def message(e) do
    if kw = e.mariadb do
      "(#{kw[:code]}): #{kw[:message]}"
    else
      e.message || ""
    end
  end
end
