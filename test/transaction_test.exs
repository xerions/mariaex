defmodule TransactionTest do
  use ExUnit.Case
  import Mariaex.TestHelper

  setup do
    opts = [database: "mariaex_test", username: "mariaex_user", password: "mariaex_pass", backoff_type: :stop]
    {:ok, pid} = Mariaex.Connection.start_link(opts)
    {:ok, [pid: pid]}
  end

  test "transaction shows correct transaction status", context do
    pid = context[:pid]
    opts = [mode: :transaction]

    assert DBConnection.status(pid, opts) == :idle
    assert query("SELECT 42", []) == [[42]]
    assert DBConnection.status(pid, opts) == :idle
    DBConnection.transaction(pid, fn conn ->
      assert DBConnection.status(conn, opts) == :transaction
    end, opts)
    assert DBConnection.status(pid, opts) == :idle
    assert query("SELECT 42", []) == [[42]]
    assert DBConnection.status(pid) == :idle
  end
end
