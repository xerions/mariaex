defmodule QueryTest do
  use ExUnit.Case, async: true
  import Mariaex.TestHelper

  setup do
    opts = [ database: "mariaex_test", username: "root" ]
    {:ok, pid} = Mariaex.Connection.start_link(opts)
    {:ok, [pid: pid]}
  end

  test "decode basic types", context do
    assert [{nil}] = query("SELECT null", [])
    assert [{1,0}] = query("SELECT true, false", [])
    assert [{"mo"}] = query("SELECT 'mo'", [])
    assert [{"mø"}] = query("SELECT 'mø'", [])
    assert [{{{2013,12,21},{23,1,27}}}] = query("SELECT timestamp('2013-12-21 23:01:27')", [])
    assert [{{2013,12,21}}] = query("SELECT date('2013-12-21 23:01:27')", [])
    assert [{{23,1,27}}] = query("SELECT time('2013-12-21 23:01:27')", [])
    assert [{51.0}] = query("select 51.0", [])
    assert [{0.012321421121421}] = query("select 00.012321421121421", [])
    assert [{100000000.27}] = query("select 100000000.27", [])
  end

  test "decode time", context do
    assert [{{0,0,0}}] = query("SELECT time('00:00:00')", [])
    #    assert [{{:time, {3,1,7}}}] = query("SELECT time('03:01:07')", [])
    assert [{{23,10,27}}] = query("SELECT time('23:10:27')", [])
    #    assert [{{:time, {2,1,2}}}] = query("SELECT time('02:01:02 EST')", [])
  end

  test "decode date", context do
    assert [{{1,1,1}}] = query("SELECT date('0001-01-01')", [])
    assert [{{1,2,3}}] = query("SELECT date('0001-02-03')", [])
    assert [{{2013,12,21}}] = query("SELECT date('2013-12-21')", [])
  end

  test "decode timestamp", context do
    assert [{{{1,1,1},{0,0,0}}}] = query("SELECT timestamp('0001-01-01 00:00:00')", [])
    assert [{{{2013,12,21},{23,1,27}}}] = query("SELECT timestamp('2013-12-21 23:01:27')", [])
    assert [{{{2013,12,21},{23,1,27}}}] = query("SELECT timestamp('2013-12-21 23:01:27 EST')", [])
  end

  test "encode and decode dates", context do
    date = {2010, 10, 17}
    time = {19, 27, 30}

    assert :ok = query("CREATE TABLE test_date_encoding (id int, date date, timestamp timestamp)", [])
    assert :ok = query("INSERT INTO test_date_encoding (date, timestamp) VALUES(?, ?)", [{date}, {date, time}])

    assert [{date, {date, time}}] = query("SELECT date, timestamp FROM test_date_encoding", [])
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
    :ok = query("INSERT INTO test VALUES (27, 'foobar')", [], [])
    [{27, "foobar"}] = query("SELECT * FROM test", [])
  end

  test "connection works after failure", context do
    assert %Mariaex.Error{} = query("wat", [])
    assert [{"syntax"}] = query("SELECT 'syntax'", [])
  end

  test "prepared_statements", context do
    assert :ok = query("CREATE TABLE test_statements (id int, text text)", [])
    assert :ok = query("INSERT INTO test_statements VALUES(?, ?)", [1, "test1"])
    assert :ok = query("INSERT INTO test_statements VALUES(?, ?)", [2, "test2"])
    assert [{1, "test1"}, {2, "test2"}] = query("SELECT id, text FROM test_statements WHERE id > ?", [0])
  end
end
