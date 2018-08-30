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
    {conn, _} = DBConnection.begin!(pid, opts)
    assert DBConnection.status(conn, opts) == :transaction
    DBConnection.commit!(conn, opts)
    assert DBConnection.status(pid, opts) == :idle
    assert query("SELECT 42", []) == [[42]]
    assert DBConnection.status(pid) == :idle
  end

  test "can not begin transaction if already begun", context do
    pid = context[:pid]
    opts = [mode: :transaction]

    {conn, _} = DBConnection.begin!(pid, opts)
    assert {:error, %DBConnection.TransactionError{status: :transaction}} =
      DBConnection.begin(conn, opts)
    DBConnection.commit!(conn, opts)
  end

  test "can not commit or rollback transaction if not begun", context do
    pid = context[:pid]
    opts = [mode: :transaction]

    assert {:error, %DBConnection.TransactionError{status: :idle}} =
      DBConnection.commit(pid, opts)
    assert {:error, %DBConnection.TransactionError{status: :idle}} =
      DBConnection.rollback(pid, opts)
  end

  test "savepoint transaction shows correct transaction status", context do
    pid = context[:pid]
    opts = [mode: :savepoint]

    {conn, _} = DBConnection.begin!(pid, [mode: :transaction])
    assert DBConnection.status(conn, opts) == :transaction

    assert {:ok, conn, _} = DBConnection.begin(conn, opts)
    assert DBConnection.status(conn, opts) == :transaction
    DBConnection.commit!(conn, opts)
    assert DBConnection.status(pid, opts) == :transaction

    assert {:ok, conn, _} = DBConnection.begin(pid, opts)
    assert DBConnection.status(conn, opts) == :transaction
    DBConnection.rollback!(conn, opts)
    assert DBConnection.status(pid, opts) == :transaction

   DBConnection.commit!(pid, [mode: :transaction])
   assert DBConnection.status(pid) == :idle
   assert query("SELECT 42", []) == [[42]]
  end

  test "can not begin, commit or rollback savepoint transaction if not begun", context do
    pid = context[:pid]
    opts = [mode: :savepoint]

    assert {:error, %DBConnection.TransactionError{status: :idle}} =
      DBConnection.begin(pid, opts)
    assert {:error, %DBConnection.TransactionError{status: :idle}} =
      DBConnection.commit(pid, opts)
    assert {:error, %DBConnection.TransactionError{status: :idle}} =
      DBConnection.rollback(pid, opts)
  end
end
