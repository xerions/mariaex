defmodule QueryTest do
  use ExUnit.Case, async: true
  import Mariaex.TestHelper

  setup do
    opts = [ database: "mariaex_test", username: "root" ]
    {:ok, pid} = Mariaex.Connection.start_link(opts)
    {:ok, [pid: pid]}
  end

  test "support primitive data types in binary protocol", context do
    string  = "Californication"
    text    = "Some random text"
    binary  = <<0,1>>
    table   = "basic_types_binary_protocol"

    sql = ~s{CREATE TABLE #{table} } <>
          ~s{(id serial, active boolean, title varchar(20), body text(20), data blob)}

    :ok = query(sql, [])
    insert = ~s{INSERT INTO #{table} (active, title, body, data) } <>
             ~s{VALUES (?, ?, ?, ?)}
    :ok = query(insert, [true, string, text, binary])

    # Boolean
    [{true}] = query("SELECT active from #{table} WHERE id = ?", [1])

    # String
    [{^string}] = query("SELECT title from #{table} WHERE id = ?", [1])

    # Text
    [{^text}] = query("SELECT body from #{table} WHERE id = ?", [1])

    # Binary
    [{^binary}] = query("SELECT data from #{table} WHERE id = ?", [1])
  end

  test "support numeric data types in binary protocol", context do
    integer = 16
    float   = 0.1
    double  = 3.1415
    decimal = Decimal.new("16.90")
    table   = "numeric_types_binary_test"

    sql = ~s{CREATE TABLE #{table} } <>
          ~s{(id serial, count integer, intensity float, accuracy double, value decimal(10, 2))}

    :ok = query(sql, [])
    insert = ~s{INSERT INTO #{table} (count, intensity, accuracy, value) } <>
             ~s{VALUES (?, ?, ?, ?)}
    :ok = query(insert, [integer, float, double, decimal])

    # Integer
    [{^integer}] = query("SELECT count from #{table} WHERE id = ?", [1])

    # Double
    [{^double}] = query("SELECT accuracy from #{table} WHERE id = ?", [1])

    # Float
    [{0.10000000149011612}] = query("SELECT intensity from #{table} WHERE id = ?", [1])

    # Decimal
    [{^decimal}] = query("SELECT ?", [decimal])
  end

  test "support primitive data types in text protocol", context do
    integer          = 1
    negative_integer = -1
    float            = 3.1415
    negative_float   = -3.1415
    string           = "Californication"
    text             = "Some random text"
    binary           = <<0,1>>
    table            = "basic_types_text_protocol"

    sql = ~s{CREATE TABLE #{table} } <>
          ~s{(id serial, active boolean, count integer, intensity float, } <>
          ~s{title varchar(20), body text(20), data blob)}

    :ok = query(sql, [])

    # Boolean
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

  test "encode and decode nils", context do
    double  = 3.1415
    decimal = Decimal.new("18.93")
    table   = "encoding_and_decoding_nils"

    sql = ~s{CREATE TABLE #{table} } <>
          ~s{(id serial, count integer, accuracy double, value decimal(10, 2))}

    :ok = query(sql, [])
    insert = ~s{INSERT INTO #{table} (count, accuracy, value) } <>
             ~s{VALUES (?, ?, ?)}

    :ok = query(insert, [nil, double, decimal])
    [{nil}] = query("SELECT count from #{table} WHERE id = ?", [1])

    :ok = query(insert, [nil, double, nil])
    [{nil, ^double, nil}] = query("SELECT count, accuracy, value from #{table} WHERE id = ?", [2])
  end

  test "encode and decode nils with more than 8 columns", context do
    table = "encoding_and_decoding_multiple_columns"

    sql = ~s{CREATE TABLE #{table} } <>
          ~s{(id serial, count_1 integer, count_2 integer, count_3 integer, count_4 integer, } <>
          ~s{count_5 integer, count_6 integer, count_7 integer, count_8 integer, } <>
          ~s{count_9 integer, count_10 integer)}


    :ok = query(sql, [])

    insert = ~s{INSERT INTO #{table} (count_1, count_2, count_3, count_4, count_5, } <>
             ~s{count_6, count_7, count_8, count_9, count_10) } <>
             ~s{VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}

    values = [1, nil, 3, 4, nil, 6, nil, 8, nil, 10]
    result = {1, 1, nil, 3, 4, nil, 6, nil, 8, nil, 10}

    :ok = query(insert, values)
    [^result] = query("SELECT * FROM #{table}", [])
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

    # Text protocol
    :ok = query("INSERT INTO test VALUES (?, ?)", [28, nil], [])
    [{28, nil}] = query("SELECT * FROM test where id = 28", [])

    # Binary protocol
    :ok = query("INSERT INTO test VALUES (29, NULL)", [], [])
    [{29, nil}] = query("SELECT * FROM test where id = 29", [])

    # Inserting without specifying a column
    :ok = query("INSERT INTO test (id) VALUES (30)", [], [])
    [{30, nil}] = query("SELECT * FROM test where id = 30", [])
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

  test "encoding bad parameters", context do
    assert %Mariaex.Error{message: "query has invalid number of parameters"} = query("SELECT 1", [:badparam])
    assert %Mariaex.Error{message: "query has invalid parameters"} = query("SELECT ?", [:badparam])
  end
end
