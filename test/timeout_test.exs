defmodule TimeoutTest do
  use ExUnit.Case, async: true
  import Mariaex.TestHelper

  @opts [database: "mariaex_test", username: "mariaex_user", password: "mariaex_pass", cache_size: 2, backoff_type: :stop, max_restarts: 0]

  setup context do
    connection_opts = context[:connection_opts] || []
    {:ok, pid} = Mariaex.Connection.start_link(connection_opts ++ @opts)
    # remove all modes for this session to have the same behaviour on different versions of mysql/mariadb
    {:ok, _} = Mariaex.Connection.query(pid, "SET SESSION sql_mode = \"\";")
    {:ok, [pid: pid]}
  end

  test "query failing with configured timeout" do
    opts = [{:timeout, 500} | Keyword.take(@opts, [:database, :username, :password])]
    {:ok, pid} = Mariaex.Connection.start_link(opts)

    context = [pid: pid]

    assert query("SELECT sleep(1)", []) == %DBConnection.ConnectionError{message: "tcp recv: timeout"}
  end

  test "query with timeout overwrite in options working" do
    opts = [{:timeout, 500} | Keyword.take(@opts, [:database, :username, :password])]
    {:ok, pid} = Mariaex.Connection.start_link(opts)

    context = [pid: pid]

    assert query("SELECT sleep(1)", [], timeout: 2000) == [[0]]
  end

end
