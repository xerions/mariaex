defmodule Mariaex.Connection do
  @moduledoc """
  Main API for Mariaex. This module handles the connection to .
  """

  defdelegate start_link(opts), to: Mariaex
  defdelegate query(conn, statement), to: Mariaex
  defdelegate query(conn, statement, params), to: Mariaex
  defdelegate query(conn, statement, params, opts), to: Mariaex
  defdelegate query!(conn, statement), to: Mariaex
  defdelegate query!(conn, statement, params), to: Mariaex
  defdelegate query!(conn, statement, params, opts), to: Mariaex
end
