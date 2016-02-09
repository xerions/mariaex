defmodule Mariaex.Connection do
  @moduledoc """
  Main API for Mariaex. This module handles the connection to .
  """

  defdelegate [start_link(opts),
               query(conn, statement),
               query(conn, statement, params),
               query(conn, statement, params, opts),
               query!(conn, statement),
               query!(conn, statement, params),
               query!(conn, statement, params, opts),
              ], to: Mariaex
end
