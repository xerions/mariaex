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
    assert query("SELECT active from #{table} WHERE id = ?", [1]) == [{true}]

    # String
    assert query("SELECT title from #{table} WHERE id = ?", [1]) == [{string}]

    # Text
    assert query("SELECT body from #{table} WHERE id = ?", [1]) == [{text}]

    # Binary
    assert query("SELECT data from #{table} WHERE id = ?", [1]) == [{binary}]
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
    assert query("SELECT count from #{table} WHERE id = ?", [1]) == [{integer}]

    # Double
    assert query("SELECT accuracy from #{table} WHERE id = ?", [1]) == [{double}]

    # Float
    assert query("SELECT intensity from #{table} WHERE id = ?", [1]) == [{0.10000000149011612}]

    # Decimal
    assert query("SELECT ?", [decimal]) == [{decimal}]
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
    assert [{true}] = query("SELECT active from #{table} WHERE id = LAST_INSERT_ID()", [])

    # Integer
    :ok = query("INSERT INTO #{table} (count) values (?)", [integer])
    assert query("SELECT count from #{table} WHERE id = LAST_INSERT_ID()", []) == [{integer}]
    :ok = query("INSERT INTO #{table} (count) values (?)", [negative_integer])
    assert query("SELECT count from #{table} WHERE id = LAST_INSERT_ID()", []) == [{negative_integer}]

    # Float
    :ok = query("INSERT INTO #{table} (intensity) values (?)", [float])
    assert query("SELECT intensity from #{table} WHERE id = LAST_INSERT_ID()", []) == [{float}]
    :ok = query("INSERT INTO #{table} (intensity) values (?)", [negative_float])
    assert query("SELECT intensity from #{table} WHERE id = LAST_INSERT_ID()", []) == [{negative_float}]

    # String
    :ok = query("INSERT INTO #{table} (title) values (?)", [string])
    assert query("SELECT title from #{table} WHERE id = LAST_INSERT_ID()", []) == [{string}]
    assert query("SELECT 'mø'", []) == [{"mø"}]

    # Text
    :ok = query("INSERT INTO #{table} (body) values (?)", [text])
    assert query("SELECT body from #{table} WHERE id = LAST_INSERT_ID()", []) == [{text}]

    # Binary
    :ok = query("INSERT INTO #{table} (data) values (?)", [binary])
    assert query("SELECT data from #{table} WHERE id = LAST_INSERT_ID()", []) == [{binary}]

    # Nil
    assert query("SELECT null", []) == [{nil}]
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
    assert query("SELECT count from #{table} WHERE id = ?", [1]) == [{nil}]

    :ok = query(insert, [nil, double, nil])
    assert query("SELECT count, accuracy, value from #{table} WHERE id = ?", [2]) == [{nil, double, nil}]
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
    assert query("SELECT * FROM #{table}", []) == [result]
  end

  test "encode and decode decimals", context do
    table            = "test_decimals"
    :ok = query("CREATE TABLE #{table} (id serial, cost decimal(10,4))", [])

    :ok = query("INSERT INTO #{table} (cost) values (?)", [Decimal.new("12.93")])
    assert query("SELECT cost FROM #{table}", []) == [{Decimal.new("12.9300")}]

    :ok = query("INSERT INTO #{table} (cost) values (?)", [Decimal.new("-164.9")])
    assert query("SELECT cost FROM #{table} WHERE id = LAST_INSERT_ID()", []) == [{Decimal.new("-164.9000")}]

    :ok = query("INSERT INTO #{table} (cost) values (?)", [Decimal.new("16400")])
    assert query("SELECT cost FROM #{table} WHERE id = LAST_INSERT_ID()", []) == [{Decimal.new("16400.0000")}]
  end

  test "encode and decode dates", context do
    time = {19, 27, 30, 10}
    datetime = {date, time}
    assert [{^date, ^datetime}] = query("SELECT date(?), timestamp(?)", [date, datetime])
    assert [{^time}] = query("SELECT time(?)", [time])
  end

  test "decode time", context do
    assert query("SELECT time('00:00:00')", []) == [{{0, 0, 0}}]
    assert query("SELECT time('03:01:07')", []) == [{{3, 1, 7}}]
    assert query("SELECT time('23:10:27')", []) == [{{23, 10, 27}}]
  end

  test "decode date", context do
    assert query("SELECT date('0001-01-01')", []) == [{{1, 1, 1}}]
    assert query("SELECT date('0001-02-03')", []) == [{{1, 2, 3}}]
    assert query("SELECT date('2013-12-21')", []) == [{{2013, 12, 21}}]
  end

  test "decode timestamp", context do
    assert query("SELECT timestamp('0001-01-01 00:00:00')", []) == [{{{1, 1, 1}, {0, 0, 0}}}]
    assert query("SELECT timestamp('2013-12-21 23:01:27')", []) == [{{{2013, 12, 21}, {23, 1, 27}}}]
    assert query("SELECT timestamp('2013-12-21 23:01:27 EST')", []) == [{{{2013, 12, 21}, {23, 1, 27}}}]
  end

  test "encode time", context do
    assert query("SELECT time(?)", [{1, 0, 0, 0}]) == [{{1, 0, 0}}]
    assert query("SELECT time(?)", [{3, 1, 7, 0}]) == [{{3, 1, 7}}]
    assert query("SELECT time(?)", [{23, 10, 27, 0}]) == [{{23, 10, 27}}]
  end

  test "encode date", context do
    assert query("SELECT date(?)", [{2221, 1, 1}]) == [{{2221, 1, 1}}]
    assert query("SELECT date(?)", [{2013, 12, 21}]) == [{{2013, 12, 21}}]
  end

  test "encode timestamp", context do
    assert query("SELECT timestamp(?)", [{{1, 1, 1}, {1, 0, 0, 0}}]) == [{{{1, 1, 1}, {1, 0, 0}}}]
    assert query("SELECT timestamp(?)", [{{2013, 12, 21}, {23, 1, 27, 0}}]) == [{{{2013, 12, 21}, {23, 1, 27}}}]
  end

  test "non data statement", context do
    :ok = query("BEGIN", [])
    :ok = query("COMMIT", [])
  end

  test "result struct", context do
    {:ok, res} = Mariaex.Connection.query(context[:pid], "SELECT 1 AS first, 10 AS last", [])
    %Mariaex.Result{} = res
    assert res.command == :select
    assert res.columns == ["first", "last"]
    assert res.num_rows == 1
  end

  test "error record", context do
    {:error, %Mariaex.Error{}} = Mariaex.Connection.query(context[:pid], "SELECT 123 + `deertick`", [])
  end

  test "insert", context do
    :ok = query("CREATE TABLE test (id int, text text)", [])
    assert query("SELECT * FROM test", []) == []

    :ok = query("INSERT INTO test VALUES (27, 'foobar')", [], [])
    assert query("SELECT * FROM test", []) == [{27, "foobar"}]

    # Text protocol
    :ok = query("INSERT INTO test VALUES (?, ?)", [28, nil], [])
    assert query("SELECT * FROM test where id = 28", []) == [{28, nil}]

    # Binary protocol
    :ok = query("INSERT INTO test VALUES (29, NULL)", [], [])
    assert query("SELECT * FROM test where id = 29", []) == [{29, nil}]

    # Inserting without specifying a column
    :ok = query("INSERT INTO test (id) VALUES (30)", [], [])
    assert query("SELECT * FROM test where id = 30", []) == [{30, nil}]
  end

  test "connection works after failure", context do
    assert %Mariaex.Error{} = query("wat", [])
    assert query("SELECT 'syntax'", []) == [{"syntax"}]
  end

  test "prepared_statements", context do
    :ok = query("CREATE TABLE test_statements (id int, text text)", [])
    :ok = query("INSERT INTO test_statements VALUES(?, ?)", [1, "test1"])
    :ok = query("INSERT INTO test_statements VALUES(?, ?)", [2, "test2"])
    assert query("SELECT id, text FROM test_statements WHERE id > ?", [0]) == [{1, "test1"}, {2, "test2"}]
  end

  test "encoding bad parameters", context do
    assert %Mariaex.Error{message: "query has invalid number of parameters"} = query("SELECT 1", [:badparam])
    assert %Mariaex.Error{message: "query has invalid parameters"} = query("SELECT ?", [:badparam])
  end
end
