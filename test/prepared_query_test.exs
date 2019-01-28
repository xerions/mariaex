defmodule PreparedQueryTest do
  use ExUnit.Case
  import Mariaex.TestHelper

  setup context do
    opts = [database: "mariaex_test", username: "mariaex_user", password: "mariaex_pass", backoff_type: :stop]
    opts = opts ++ (context |> Map.to_list() |> Keyword.take([:prepare]))
    {:ok, pid} = Mariaex.Connection.start_link(opts)
    {:ok, [pid: pid]}
  end

  test "simple prepared query", context do
    :ok = query("CREATE TABLE prepared_test (id int, text text)", [])
    assert with_prepare!("test", "SELECT * FROM prepared_test", []) == []
  end

  test "executing unprepared query raises", context do
    :ok = query("CREATE TABLE unprepared_test (id int, text text)", [])
    conn = context[:pid]
    query = %Mariaex.Query{name: "unprepared_test", statement: "SELECT * FROM unprepared_test"}
    assert_raise ArgumentError, ~r"has not been prepared", fn() -> Mariaex.execute!(conn, query, []) end
  end

  test "prepare, execute and close", context do
    assert (%Mariaex.Query{name: "42"} = query) = prepare("42", "SELECT 42")
    assert [[42]] = execute(query, [])
    assert [[42]] = execute(query, [])
    assert :ok = close(query)
    assert [[42]] = query("SELECT 42", [])
  end

  test "prepare_execute, execute and close", context do
    assert {%Mariaex.Query{} = query, [[42]]} = prepare_execute("42", "SELECT ?", [42])
    assert [[41]] = execute(query, [41])
    assert :ok = close(query)
    assert [[43]] = query("SELECT ?", [43])
  end

  @tag prepare: :unnamed
  test "prepare named is unnamed when named not allowed", context do
    assert (%Mariaex.Query{name: ""} = query) = prepare("42", "SELECT 42")
    assert [[42]] = execute(query, [])
    assert [[42]] = execute(query, [])
    assert :ok = close(query)
    assert [[42]] = query("SELECT 42", [])
  end

  @tag prepare: :unnamed
  test "stableized prepared statement count with prepare unnamed", context do
    count = prepared() + 1
    for _n <- 1..2 do
      for n <- 1..10 do
        assert [[^n]] = with_prepare!("#{n}", "SELECT #{n}", [])
        assert ^count = prepared()
      end
    end
  end

  test "prepare and execute different queries with same name", context do
    query42 = prepare("select", "SELECT 42")
    assert close(query42) == :ok
    assert %Mariaex.Query{} = prepare("select", "SELECT 41")
    assert [[42]] = execute(query42, [])

    assert [[42]] = query("SELECT 42", [])
  end

  test "prepare, close and execute", context do
    query = prepare("reuse", "SELECT ? + ?")
    assert [[40]] = execute(query, [13, 27])
    assert :ok = close(query)
    assert [[40]] = execute(query, [3, 37])
  end

  test "closing prepared query that does not exist succeeds", context do
    query = prepare("42", "SELECT 42")
    assert :ok = close(query)
    assert :ok = close(query)
  end

  test "execute and query the same unnamed statement", context do
    query1 = prepare("", "SELECT 42")
    assert :ok = close(query1)
    query2 = prepare("", "SELECT 42")
    assert [[42]] = execute(query2, [])
    assert [[42]] = query("SELECT 42", [])
    assert [[42]] = execute(query1, [])
  end

  test "prepare and execute call procedure stream", context do
    sql =
      """
      CREATE PROCEDURE executeproc (IN a INT, IN b INT)
      BEGIN
      SELECT a + b;
      END
      """
    assert :ok = query(sql, [])
    query = prepare("", "CALL executeproc(?, ?)")
    assert [[3]] = execute(query, [1, 2])

    assert [[42]] = query("SELECT 42", [])
  end
end
