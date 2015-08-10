defmodule QueryTest do
  use ExUnit.Case, async: true
  import Mariaex.TestHelper

  setup do
    opts = [ database: "mariaex_test", username: "root" ]
    {:ok, pid} = Mariaex.Connection.start_link(opts)
    {:ok, [pid: pid]}
  end

  test "simple query using password connection" do
    opts = [ database: "mariaex_test", username: "mariaex_user", password: "mariaex_pass" ]
    {:ok, pid} = Mariaex.Connection.start_link(opts)

    context = [pid: pid]

    :ok = query("CREATE TABLE test_pass (id int, text text)", [])
    assert query("SELECT * FROM test_pass", []) == []

    :ok = query("INSERT INTO test_pass VALUES (27, 'foobar')", [], [])
    assert query("SELECT * FROM test_pass", []) == [[27, "foobar"]]
  end

  test "connection without database" do
    opts = [ username: "mariaex_user", password: "mariaex_pass", skip_database: true ]
    {:ok, pid} = Mariaex.Connection.start_link(opts)

    context = [pid: pid]

    assert :ok = query("CREATE DATABASE database_from_connection", [])
    assert [] = query("DROP DATABASE database_from_connection", [])
  end

  test "queries are dequeued after previous query is processed", context do
    assert {:timeout, _} =
           catch_exit(query("SLEEP(0.1", [], timeout: 0))
    assert [[1]] = query("SELECT 1", [])
  end

  test "support primitive data types using prepared statements", context do
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
    assert query("SELECT active from #{table} WHERE id = LAST_INSERT_ID()", []) == [[1]]

    # String
    assert query("SELECT title from #{table} WHERE id = LAST_INSERT_ID()", []) == [[string]]

    # Text
    assert query("SELECT body from #{table} WHERE id = LAST_INSERT_ID()", []) == [[text]]

    # Binary
    assert query("SELECT data from #{table} WHERE id = LAST_INSERT_ID()", []) == [[binary]]
  end

  test "booleen and tiny int tests", context do
    table = "boolean_test"
    :ok = query("CREATE TABLE #{table} (id serial, active boolean, tiny tinyint)", [])

    :ok = query(~s{INSERT INTO #{table} (id, active, tiny) VALUES (?, ?, ?)}, [1, 0, 127])
    :ok = query(~s{INSERT INTO #{table} (id, active, tiny) VALUES (?, ?, ?)}, [2, true, -128])

    assert query("SELECT active, tiny from #{table} WHERE id = ?", [1]) == [[0, 127]]
    assert query("SELECT active, tiny from #{table} WHERE id = ?", [2]) == [[1, -128]]
  end

  test "support numeric data types using prepared statements", context do
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
    assert query("SELECT count from #{table} WHERE id = ?", [1]) == [[integer]]

    # Double
    assert query("SELECT accuracy from #{table} WHERE id = ?", [1]) == [[double]]

    # Float
    assert query("SELECT intensity from #{table} WHERE id = ?", [1]) == [[0.10000000149011612]]

    # Decimal
    assert query("SELECT ?", [decimal]) == [[decimal]]
  end

  test "support primitive data types with prepared statement", context do
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
    assert [[1]] = query("SELECT active from #{table} WHERE id = LAST_INSERT_ID()", [])

    # Integer
    :ok = query("INSERT INTO #{table} (count) values (?)", [integer])
    assert query("SELECT count from #{table} WHERE id = LAST_INSERT_ID()", []) == [[integer]]
    :ok = query("INSERT INTO #{table} (count) values (?)", [negative_integer])
    assert query("SELECT count from #{table} WHERE id = LAST_INSERT_ID()", []) == [[negative_integer]]

    # Float
    :ok = query("INSERT INTO #{table} (intensity) values (?)", [float])
    [[query_float]] = query("SELECT intensity from #{table} WHERE id = LAST_INSERT_ID()", [])
    assert_in_delta query_float, float, 0.0001
    :ok = query("INSERT INTO #{table} (intensity) values (?)", [negative_float])
    [[query_negative_float]] = query("SELECT intensity from #{table} WHERE id = LAST_INSERT_ID()", [])
    assert_in_delta query_negative_float, negative_float, 0.0001


    # String
    :ok = query("INSERT INTO #{table} (title) values (?)", [string])
    assert query("SELECT title from #{table} WHERE id = LAST_INSERT_ID()", []) == [[string]]
    assert query("SELECT 'mø'", []) == [["mø"]]

    # Text
    :ok = query("INSERT INTO #{table} (body) values (?)", [text])
    assert query("SELECT body from #{table} WHERE id = LAST_INSERT_ID()", []) == [[text]]

    # Binary
    :ok = query("INSERT INTO #{table} (data) values (?)", [binary])
    assert query("SELECT data from #{table} WHERE id = LAST_INSERT_ID()", []) == [[binary]]

    # Nil
    assert query("SELECT null", []) == [[nil]]
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
    assert query("SELECT count from #{table} WHERE id = ?", [1]) == [[nil]]

    :ok = query(insert, [nil, double, nil])
    assert query("SELECT count, accuracy, value from #{table} WHERE id = ?", [2]) == [[nil, double, nil]]
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
    result = [1, 1, nil, 3, 4, nil, 6, nil, 8, nil, 10]

    :ok = query(insert, values)
    assert query("SELECT * FROM #{table}", []) == [result]
  end

  test "encode and decode decimals", context do
    table            = "test_decimals"
    :ok = query("CREATE TABLE #{table} (id serial, cost decimal(10,4))", [])

    :ok = query("INSERT INTO #{table} (cost) values (?)", [Decimal.new("12.93")])
    assert query("SELECT cost FROM #{table}", []) == [[Decimal.new("12.9300")]]

    :ok = query("INSERT INTO #{table} (cost) values (?)", [Decimal.new("-164.9")])
    assert query("SELECT cost FROM #{table} WHERE id = LAST_INSERT_ID()", []) == [[Decimal.new("-164.9000")]]

    :ok = query("INSERT INTO #{table} (cost) values (?)", [Decimal.new("16400")])
    assert query("SELECT cost FROM #{table} WHERE id = LAST_INSERT_ID()", []) == [[Decimal.new("16400.0000")]]
  end

  test "encode and decode date", context do
    date0 = {2010, 10, 17}
    date1 = {0, 0, 0}
    table = "test_dates"

    sql = ~s{CREATE TABLE #{table} (id int, d date)}
    :ok = query(sql, [])

    insert = ~s{INSERT INTO #{table} (id, d) VALUES (?, ?)}
    :ok = query(insert, [1, date0])
    :ok = query(insert, [2, date1])

    assert query("SELECT d FROM #{table} WHERE id = 1", []) == [[date0]]
    assert query("SELECT d FROM #{table} WHERE id = ?", [1]) == [[date0]]
    assert query("SELECT d FROM #{table} WHERE id = ?", [2]) == [[date1]]
  end

  test "encode and decode time", context do
    time = {19, 27, 30, 0}
    time_with_msec = {10, 14, 16, 23}
    table = "test_times"

    sql = ~s{CREATE TABLE #{table} (id int, t1 time, t2 time)}
    :ok = query(sql, [])

    insert = ~s{INSERT INTO #{table} (id, t1, t2) VALUES (?, ?, ?)}
    :ok = query(insert, [1, time, time_with_msec])

    # Time
    # Only MySQL 5.7 supports microseconds storage, so it will return 0 here
    assert query("SELECT t1, t2 FROM #{table} WHERE id = 1", []) == [[time, {10, 14, 16, 0}]]
    assert query("SELECT t1, t2 FROM #{table} WHERE id = ?", [1]) == [[time, {10, 14, 16, 0}]]
    assert query("SELECT time('00:00:00')", []) == [[{0, 0, 0, 0}]]
  end

  test "encode and decode datetime", context do
    date = {2010, 10, 17}
    datetime = {date, {10, 10, 30, 0}}
    datetime_with_msec = {date, {13, 32, 15, 12}}
    table = "test_datetimes"

    sql = ~s{CREATE TABLE #{table} (id int, dt1 datetime, dt2 datetime)}
    :ok = query(sql, [])

    insert = ~s{INSERT INTO #{table} (id, dt1, dt2) VALUES (?, ?, ?)}
    :ok = query(insert, [1, datetime, datetime_with_msec])

    # Datetime
    # Only MySQL 5.7 supports microseconds storage, so it will return 0 here
    assert query("SELECT dt1, dt2 FROM #{table} WHERE id = 1", []) == [[datetime, {date, {13, 32, 15, 0}}]]
    assert query("SELECT dt1, dt2 FROM #{table} WHERE id = ?", [1]) == [[datetime, {date, {13, 32, 15, 0}}]]
  end

  test "encode and decode timestamp", context do
    date = {2010, 10, 17}
    timestamp = {date, {10, 10, 30, 0}}
    timestamp_with_msec = {date, {13, 32, 15, 12}}
    table = "test_timestamps"

    sql = ~s{CREATE TABLE #{table} (id int, ts1 timestamp, ts2 timestamp)}
    :ok = query(sql, [])

    insert = ~s{INSERT INTO #{table} (id, ts1, ts2) VALUES (?, ?, ?)}
    :ok = query(insert, [1, timestamp, timestamp_with_msec])

    # Timestamp
    # Only MySQL 5.7 supports microseconds storage, so it will return 0 here
    assert query("SELECT ts1, ts2 FROM #{table} WHERE id = 1", []) == [[timestamp, {date, {13, 32, 15, 0}}]]
    assert query("SELECT ts1, ts2 FROM #{table} WHERE id = ?", [1]) == [[timestamp, {date, {13, 32, 15, 0}}]]
    assert query("SELECT timestamp('0000-00-00 00:00:00')", []) == [[{{0, 0, 0}, {0, 0, 0, 0}}]]
    assert query("SELECT timestamp('0001-01-01 00:00:00')", []) == [[{{1, 1, 1}, {0, 0, 0, 0}}]]
    assert query("SELECT timestamp('2013-12-21 23:01:27')", []) == [[{{2013, 12, 21}, {23, 1, 27, 0}}]]
    assert query("SELECT timestamp('2013-12-21 23:01:27 EST')", []) == [[{{2013, 12, 21}, {23, 1, 27, 0}}]]
  end

  test "decode smallint", context do
    table = "test_smallint"
    :ok = query("CREATE TABLE #{table} (id serial, testfield smallint)", [])

    max_signed = 32767
    min_signed = -32768
    out_of_range = max_signed + 1
    :ok = query("INSERT INTO #{table} (id, testfield) values (1, ?)", [max_signed])
    :ok = query("INSERT INTO #{table} (id, testfield) values (2, ?)", [min_signed])
    :ok = query("INSERT INTO #{table} (id, testfield) values (3, ?)", [out_of_range])
    assert query("SELECT testfield FROM #{table} WHERE id = 1", []) == [[max_signed]]
    assert query("SELECT testfield FROM #{table} WHERE id = 2", []) == [[min_signed]]
    assert query("SELECT testfield FROM #{table} WHERE id = 3", []) == [[max_signed]]
  end

  test "decode mediumint", context do
    table = "test_mediumint"
    :ok = query("CREATE TABLE #{table} (id serial, testfield mediumint)", [])

    max_signed = 8388607
    min_signed = -8388608
    out_of_range = max_signed + 1
    :ok = query("INSERT INTO #{table} (id, testfield) values (1, ?)", [max_signed])
    :ok = query("INSERT INTO #{table} (id, testfield) values (2, ?)", [min_signed])
    :ok = query("INSERT INTO #{table} (id, testfield) values (3, ?)", [out_of_range])
    assert query("SELECT testfield FROM #{table} WHERE id = 1", []) == [[max_signed]]
    assert query("SELECT testfield FROM #{table} WHERE id = 2", []) == [[min_signed]]
    assert query("SELECT testfield FROM #{table} WHERE id = 3", []) == [[max_signed]]
  end

  test "decode year", context do
    table = "test_year"
    :ok = query("CREATE TABLE #{table} (id serial, testfield year)", [])

    year = 2015
    :ok = query("INSERT INTO #{table} (id, testfield) values (1, ?)", [year])
    assert query("SELECT testfield FROM #{table} WHERE id = 1", []) == [[year]]
  end

  test "non data statement", context do
    :ok = query("BEGIN", [])
    :ok = query("COMMIT", [])
  end

  test "result struct on select", context do
    {:ok, res} = Mariaex.Connection.query(context[:pid], "SELECT 1 AS first, 10 AS last", [])

    assert %Mariaex.Result{} = res
    assert res.command == :select
    assert res.columns == ["first", "last"]
    assert res.num_rows == 1
  end

  test "result struct on update", context do
    table = "struct_on_update"

    :ok = query(~s{CREATE TABLE #{table} (num int)}, [])
    :ok = query(~s{INSERT INTO #{table} (num) VALUES (?)}, [1])

    {:ok, res} = Mariaex.Connection.query(context[:pid], "UPDATE #{table} SET num = 2", [])
    assert %Mariaex.Result{} = res
    assert res.command == :update
    assert res.num_rows == 1

    {:ok, res} = Mariaex.Connection.query(context[:pid], "UPDATE #{table} SET num = 2", [])
    assert %Mariaex.Result{} = res
    assert res.command == :update
    assert res.num_rows == 1
  end

  test "error struct", context do
    {:error, %Mariaex.Error{}} = Mariaex.Connection.query(context[:pid], "SELECT 123 + `deertick`", [])
  end

  test "insert", context do
    :ok = query("CREATE TABLE test (id int, text text)", [])
    assert query("SELECT * FROM test", []) == []

    :ok = query("INSERT INTO test VALUES (27, 'foobar')", [], [])
    assert query("SELECT * FROM test", []) == [[27, "foobar"]]

    # Text protocol
    :ok = query("INSERT INTO test VALUES (?, ?)", [28, nil], [])
    assert query("SELECT * FROM test where id = 28", []) == [[28, nil]]

    # Binary protocol
    :ok = query("INSERT INTO test VALUES (29, NULL)", [], [])
    assert query("SELECT * FROM test where id = 29", []) == [[29, nil]]

    # Inserting without specifying a column
    :ok = query("INSERT INTO test (id) VALUES (30)", [], [])
    assert query("SELECT * FROM test where id = 30", []) == [[30, nil]]
  end

  test "connection works after failure", context do
    assert %Mariaex.Error{} = query("wat", [])
    assert query("SELECT 'syntax'", []) == [["syntax"]]
  end

  test "prepared_statements", context do
    :ok = query("CREATE TABLE test_statements (id int, text text)", [])
    :ok = query("INSERT INTO test_statements VALUES(?, ?)", [1, "test1"])
    :ok = query("INSERT INTO test_statements VALUES(?, ?)", [2, "test2"])
    assert query("SELECT id, text FROM test_statements WHERE id > ?", [0]) == [[1, "test1"], [2, "test2"]]
  end

  test "encoding bad parameters", context do
    assert %Mariaex.Error{message: "query has invalid number of parameters"} = query("SELECT 1", [:badparam])
    assert %Mariaex.Error{message: "query has invalid parameters"} = query("SELECT ?", [:badparam])
  end

  test "non ascii character", context do
    :ok = query("CREATE TABLE test_charset (id int, text text)", [])
    :ok = query("INSERT INTO test_charset VALUES (?, ?)", [1, "忍者"])

    assert query("SELECT * FROM test_charset where id = 1", []) == [[1, "忍者"]]
  end

  test "test nullbit", context do
    :ok = query("CREATE TABLE test_nullbit (id int, t1 text, t2 text, t3 text, t4 text, t5 text not NULL, t6 text, t7 text not NULL)", [])
    :ok = query("INSERT INTO test_nullbit VALUES (?, ?, ?, ?, ?, ?, ?, ?)", [nil, "t1", nil, "t3", nil, "t5", nil, "t7"])
    assert query("SELECT * FROM test_nullbit WHERE t1 = 't1'", []) == [[nil, "t1", nil, "t3", nil, "t5", nil, "t7"]]
  end

  test "\\n next to SELECT should not cause failure", context do
    assert query("SELECT\n1", []) == [[1]]
  end

  test "prepared statements outlive transaction rollback", context do
    assert :ok = query("CREATE TABLE test_rollback (id int, text text)", [])
    assert :ok = query("BEGIN", [])
    assert :ok = query("INSERT INTO test_rollback VALUES(?, ?)", [1, "test1"])
    assert :ok = query("ROLLBACK", [])
    assert :ok = query("INSERT INTO test_rollback VALUES(?, ?)", [1, "test1"])
  end

  test "prepared statements outlive transaction commit", context do
    assert :ok = query("CREATE TABLE test_commit (id int, text text)", [])
    assert :ok = query("BEGIN", [])
    assert :ok = query("INSERT INTO test_commit VALUES(?, ?)", [1, "test1"])
    assert :ok = query("COMMIT", [])
    assert :ok = query("INSERT INTO test_commit VALUES(?, ?)", [2, "test2"])
  end

  test "test rare commands in prepared statements", context do
    assert _ = query("SHOW FULL PROCESSLIST", [])
  end

end
