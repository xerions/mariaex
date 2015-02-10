defmodule StartTest do
  use ExUnit.Case, async: true
  import Mariaex.TestHelper

  test "connection_errors", context do
    assert {:error, %Mariaex.Error{mariadb: %{message: "Unknown database 'non_existing'"}}} =
      Mariaex.Connection.start_link(username: "root", database: "non_existing")
    assert {:error, %Mariaex.Error{mariadb: %{message: "Access denied for user " <> _}}} =
      Mariaex.Connection.start_link(username: "non_existing", database: "mariaex_test")
  end

end
