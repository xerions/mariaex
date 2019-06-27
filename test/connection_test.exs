defmodule ConnectionTest do
  use ExUnit.Case
  import Mariaex.TestHelper

  @opts [
    database: "mariaex_test",
    username: "mariaex_user",
    password: "mariaex_pass",
    cache_size: 2,
    max_restarts: 0
  ]

  setup context do
    connection_opts = context[:connection_opts] || []
    {:ok, pid} = Mariaex.Connection.start_link(connection_opts ++ @opts)

    # remove all modes for this session to have the same behaviour on different versions of mysql/mariadb
    {:ok, _} = Mariaex.Connection.query(pid, "SET SESSION sql_mode = \"\";")

    {:ok, _} = Mariaex.Connection.query(pid, "set GLOBAL max_connections = 1;")

    {:ok, [pid: pid]}
  end

  test "query fails with connection not available", %{pid: orig_pid} do
    # my version of mariadb only allows setting max_connections to 10, so we have to start up a few connections to saturate this.
    assert Enum.find(1..15, fn _x ->
             {:ok, pid} = Mariaex.Connection.start_link(@opts)
             context = [pid: pid]

             case query("SELECT 1", []) do
               [[1]] ->
                 false

               %DBConnection.ConnectionError{
                 message:
                   "connection not available and request was dropped from queue after " <> rest
               } ->
                 true
             end
           end)

    # cleanup
    {:ok, _} = Mariaex.Connection.query(orig_pid, "set GLOBAL max_connections = 500;")
  end
end
