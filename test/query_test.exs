defmodule QueryTest do
  use ExUnit.Case, async: true
  import Mariaex.TestHelper

  setup do
    opts = [ database: "mariaex_test", username: "root" ]
    {:ok, pid} = Mariaex.Connection.start_link(opts)
    {:ok, [pid: pid]}
  end

  test "support primitive data types", context do
    integer          = 1
    negative_integer = -1
    float            = 3.1415
    negative_float   = -3.1415
    string           = "Californication"
    text             = "Some random text"
    binary           = <<0,1>>
    table            = "basic_types"

    sql = ~s{CREATE TABLE #{table} } <>
          ~s{(id serial, active boolean, count integer, intensity float, } <>
          ~s{title varchar(20), body text(20), data blob)}

    :ok = query(sql, [])

    # Booleans
    :ok = query("INSERT INTO #{table} (active) values (?)", [true])
    [{true}] = query("SELECT active from #{table} WHERE id = LAST_INSERT_ID()", [])

    # Integer
    :ok = query("INSERT INTO #{table} (count) values (?)", [integer])
    [{^integer}] = query("SELECT count from #{table} WHERE id = LAST_INSERT_ID()", [])
    :ok = query("INSERT INTO #{table} (count) values (?)", [negative_integer])
    [{^negative_integer}] = query("SELECT count from #{table} WHERE id = LAST_INSERT_ID()", [])

    # Float
    :ok = query("INSERT INTO #{table} (intensity) values (?)", [float])
    [{^float}] = query("SELECT intensity from #{table} WHERE id = LAST_INSERT_ID()", [])
    :ok = query("INSERT INTO #{table} (intensity) values (?)", [negative_float])
    [{^negative_float}] = query("SELECT intensity from #{table} WHERE id = LAST_INSERT_ID()", [])

    # String
    :ok = query("INSERT INTO #{table} (title) values (?)", [string])
    [{^string}] = query("SELECT title from #{table} WHERE id = LAST_INSERT_ID()", [])
    [{"mø"}] = query("SELECT 'mø'", [])

    # Text
    :ok = query("INSERT INTO #{table} (body) values (?)", [text])
    [{^text}] = query("SELECT body from #{table} WHERE id = LAST_INSERT_ID()", [])

    # Binary
    :ok = query("INSERT INTO #{table} (data) values (?)", [binary])
    [{^binary}] = query("SELECT data from #{table} WHERE id = LAST_INSERT_ID()", [])

    # Nil
    [{nil}] = query("SELECT null", [])
  end

  test "encode and decode decimals", context do
    table            = "test_decimals"
    :ok = query("CREATE TABLE #{table} (id serial, cost decimal(10,4))", [])

    :ok = query("INSERT INTO #{table} (cost) values (?)", [Decimal.new("12.93")])
    assert [{Decimal.new("12.9300")}] == query("SELECT cost FROM #{table}", [])

    :ok = query("INSERT INTO #{table} (cost) values (?)", [Decimal.new("-164.9")])
    assert [{Decimal.new("-164.9000")}] == query("SELECT cost FROM #{table} WHERE id = LAST_INSERT_ID()", [])

    :ok = query("INSERT INTO #{table} (cost) values (?)", [Decimal.new("16400")])
    assert [{Decimal.new("16400.0000")}] == query("SELECT cost FROM #{table} WHERE id = LAST_INSERT_ID()", [])
  end

  test "encode and decode dates", context do
    date = {2010, 10, 17}
    time = {19, 27, 30}
    datetime = {date, time}
    assert [{date, datetime}] =  query("SELECT date(?), timestamp(?)", [date, datetime])
    assert [time] =  query("SELECT time(?)", [time])
  end

  test "decode time", context do
    assert [{{0, 0, 0}}] = query("SELECT time('00:00:00')", [])
    assert [{{3, 1, 7}}] = query("SELECT time('03:01:07')", [])
    assert [{{23, 10, 27}}] = query("SELECT time('23:10:27')", [])
  end

  test "decode date", context do
    assert [{{1, 1, 1}}] = query("SELECT date('0001-01-01')", [])
    assert [{{1, 2, 3}}] = query("SELECT date('0001-02-03')", [])
    assert [{{2013, 12, 21}}] = query("SELECT date('2013-12-21')", [])
  end

  test "decode timestamp", context do
    assert [{{{1, 1, 1}, {0, 0, 0}}}] = query("SELECT timestamp('0001-01-01 00:00:00')", [])
    assert [{{{2013, 12, 21}, {23, 1, 27}}}] = query("SELECT timestamp('2013-12-21 23:01:27')", [])
    assert [{{{2013, 12, 21}, {23, 1, 27}}}] = query("SELECT timestamp('2013-12-21 23:01:27 EST')", [])
  end

  test "encode time", context do
    assert [{{1, 0, 0}}] = query("SELECT time(?)", [{1, 0, 0}])
    assert [{{3, 1, 7}}] = query("SELECT time(?)", [{3, 1, 7}])
    assert [{{23, 10, 27}}] = query("SELECT time(?)", [{23, 10, 27}])
  end

  test "encode date", context do
    assert [{{2221, 1, 1}}] = query("SELECT date(?)", [{2221, 1, 1}])
    assert [{{2013, 12, 21}}] = query("SELECT date(?)", [{2013, 12, 21}])
  end

  test "encode timestamp", context do
    assert [{{{1, 1, 1}, {1, 0, 0}}}] = query("SELECT timestamp(?)", [{{1, 1, 1}, {1, 0, 0}}])
    assert [{{{2013, 12, 21}, {23, 1, 27}}}] = query("SELECT timestamp(?)", [{{2013, 12, 21}, {23, 1, 27}}])
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
