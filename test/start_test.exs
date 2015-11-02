defmodule StartTest do
  use ExUnit.Case, async: true

  test "connection_errors" do
    Process.flag :trap_exit, true
    assert {:error, %Mariaex.Error{mariadb: %{message: "Unknown database 'non_existing'"}}} =
      Mariaex.Connection.start_link(username: "root", database: "non_existing", sync_connect: true)
    assert {:error, %Mariaex.Error{mariadb: %{message: "Access denied for user " <> _}}} =
      Mariaex.Connection.start_link(username: "non_existing", database: "mariaex_test", sync_connect: true)
    assert {:error, %Mariaex.Error{message: "tcp connect: econnrefused"}} =
      Mariaex.Connection.start_link(username: "root", database: "mariaex_test", port: 60999, sync_connect: true)
  end
end
