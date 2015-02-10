defmodule MariaexTest do
  use ExUnit.Case, async: true
  import Mariaex.TestHelper

  setup do
    opts = [ database: "mariaex_test", username: "root" ]
    {:ok, pid} = Mariaex.Connection.start_link(opts)
    {:ok, [pid: pid]}
  end

  test "decode basic types", context do
    assert [{1,0}] = query("SELECT true, false", [])
    assert [{"mo"}] = query("SELECT 'mo'", [])
    assert [{"mø"}] = query("SELECT 'mø'", [])
    assert [{{2013,12,21,23,1,27}}] = query("SELECT timestamp('2013-12-21 23:01:27')", [])
    assert [{{:date, {2013,12,21}}}] = query("SELECT date('2013-12-21 23:01:27')", [])
    assert [{{:time, {23,1,27}}}] = query("SELECT time('2013-12-21 23:01:27')", [])
    assert [{51.0}] = query("select 51.0", [])
    assert [{0.012321421121421}] = query("select 00.012321421121421", [])
    assert [{100000000.27}] = query("select 100000000.27", [])
  end

  test "decode time", context do
    assert [{{:time, {0,0,0}}}] = query("SELECT time('00:00:00')", [])
    #    assert [{{:time, {3,1,7}}}] = query("SELECT time('03:01:07')", [])
    assert [{{:time, {23,10,27}}}] = query("SELECT time('23:10:27')", [])
    #    assert [{{:time, {2,1,2}}}] = query("SELECT time('02:01:02 EST')", [])
  end

  test "decode date", context do
    assert [{{:date, {1,1,1}}}] = query("SELECT date('0001-01-01')", [])
    assert [{{:date, {1,2,3}}}] = query("SELECT date('0001-02-03')", [])
    assert [{{:date, {2013,12,21}}}] = query("SELECT date('2013-12-21')", [])
  end

  test "decode timestamp", context do
    assert [{{1,1,1,0,0,0}}] = query("SELECT timestamp('0001-01-01 00:00:00')", [])
    assert [{{2013,12,21,23,1,27}}] = query("SELECT timestamp('2013-12-21 23:01:27')", [])
    assert [{{2013,12,21,23,1,27}}] = query("SELECT timestamp('2013-12-21 23:01:27 EST')", [])
  end


  test "non data statement", context do
    assert :ok = query("BEGIN", [])
    assert :ok = query("COMMIT", [])
  end

  test "result struct", context do
    assert {:ok, res} = Mariaex.Connection.query(context[:pid], "SELECT 1 AS first, 10 AS last", [])
    assert %Mariaex.Result{} = res
    assert res.command == :select
    assert res.columns == ["first", "last"]
    assert res.num_rows == 1
  end

  test "error record", context do
    assert {:error, %Mariaex.Error{}} = Mariaex.Connection.query(context[:pid], "SELECT 123 + `deertick`", [])
  end

  test "insert", context do
    :ok = query("CREATE TABLE test (id int, text text)", [])
    [] = query("SELECT * FROM test", [])
    [] = query("INSERT INTO test VALUES (27, 'foobar')", [], [])
    [{27, "foobar"}] = query("SELECT * FROM test", [])
  end

  test "connection works after failure", context do
    assert %Mariaex.Error{} = query("wat", [])
    assert [{"syntax"}] = query("SELECT 'syntax'", [])
  end
end
