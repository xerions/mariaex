defmodule QueryTest do
  use ExUnit.Case, async: true
  import Mariaex.TestHelper

  @opts [database: "mariaex_test", username: "mariaex_user", password: "mariaex_pass", cache_size: 2, backoff_type: :stop]

  setup context do
    connection_opts = context[:connection_opts] || []
    {:ok, pid} = Mariaex.Connection.start_link(connection_opts ++ @opts)
    # remove all modes for this session to have the same behaviour on different versions of mysql/mariadb
    {:ok, _} = Mariaex.Connection.query(pid, "SET SESSION sql_mode = \"\";")
    {:ok, [pid: pid]}
  end

  test "simple query using password connection" do
    opts = [database: "mariaex_test", username: "mariaex_user", password: "mariaex_pass"]
    {:ok, pid} = Mariaex.Connection.start_link(opts)

    context = [pid: pid]

    :ok = query("CREATE TABLE test_pass (id int, text text)", [])
    assert query("SELECT * FROM test_pass", []) == []

    :ok = query("INSERT INTO test_pass VALUES (27, 'foobar')", [], [])
    assert query("SELECT * FROM test_pass", []) == [[27, "foobar"]]
  end

  test "connection without database" do
    opts = [username: "mariaex_user", password: "mariaex_pass", skip_database: true]
    {:ok, pid} = Mariaex.Connection.start_link(opts)

    context = [pid: pid]

    assert :ok = query("CREATE DATABASE database_from_connection", [])
    assert :ok = query("DROP DATABASE database_from_connection", [])
  end

  test "queries are dequeued after previous query is processed", context do
    conn = context[:pid]

    Process.flag(:trap_exit, true)
    capture_log fn ->
      assert %Mariaex.Error{} = query("DO SLEEP(0.1)", [], timeout: 0)
      assert_receive {:EXIT, ^conn, {:shutdown, %DBConnection.ConnectionError{}}}
    end
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

  test "boolean and tiny int tests", context do
    table = "boolean_test"
    :ok = query("CREATE TABLE #{table} (id serial, active boolean, tiny tinyint)", [])

    :ok = query(~s{INSERT INTO #{table} (id, active, tiny) VALUES (?, ?, ?)}, [1, 0, 127])
    :ok = query(~s{INSERT INTO #{table} (id, active, tiny) VALUES (?, ?, ?)}, [2, true, -128])
    :ok = query(~s{INSERT INTO #{table} (id, active, tiny) VALUES (?, ?, ?)}, [3, false, -128])

    assert query("SELECT active, tiny from #{table} WHERE id = ?", [1]) == [[0, 127]]
    assert query("SELECT active, tiny from #{table} WHERE id = ?", [2]) == [[1, -128]]
    assert query("SELECT active, tiny from #{table} WHERE id = ?", [3]) == [[0, -128]]
  end

  test "boolean and unsigned tiny int tests", context do
    table = "boolean_test_unsigned"
    :ok = query("CREATE TABLE #{table} (id serial, active boolean, tiny tinyint unsigned)", [])

    :ok = query(~s{INSERT INTO #{table} (id, active, tiny) VALUES (?, ?, ?)}, [1, 0, 255])
    :ok = query(~s{INSERT INTO #{table} (id, active, tiny) VALUES (?, ?, ?)}, [2, true, 0])

    assert query("SELECT active, tiny from #{table} WHERE id = ?", [1]) == [[0, 255]]
    assert query("SELECT active, tiny from #{table} WHERE id = ?", [2]) == [[1, 0]]
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
    date0 = ~D[2010-10-17]
    date1 = ~D[0000-01-01]
    table = "test_dates"

    sql = ~s{CREATE TABLE #{table} (id int, d date)}
    :ok = query(sql, [])

    insert = ~s{INSERT INTO #{table} (id, d) VALUES (?, ?)}
    :ok = query(insert, [1, date0])
    :ok = query(insert, [2, date1])

    # Strings
    assert query("SELECT cast(d AS char) FROM #{table} WHERE id = 1", []) == [["2010-10-17"]]
    assert query("SELECT cast(d AS char) FROM #{table} WHERE id = 2", []) == [["0000-01-01"]]

    # Date
    assert query("SELECT d FROM #{table} WHERE id = 1", []) == [[date0]]
    assert query("SELECT d FROM #{table} WHERE id = ?", [1]) == [[date0]]
    assert query("SELECT d FROM #{table} WHERE id = ?", [2]) == [[date1]]
    assert query("SELECT date('0000-01-01')", []) == [[date1]]
  end

  test "encode and decode time", context do
    time = ~T[19:27:30]
    time_with_msec = ~T[10:14:16.23]
    table = "test_times"

    sql = ~s{CREATE TABLE #{table} (id int, t1 time, t2 time)}
    :ok = query(sql, [])

    insert = ~s{INSERT INTO #{table} (id, t1, t2) VALUES (?, ?, ?)}
    :ok = query(insert, [1, time, time_with_msec])

    # Strings
    assert query("SELECT cast(t1 as char), cast(t2 as char) FROM #{table} WHERE id = 1", []) ==
           [["19:27:30", "10:14:16"]]

    # Time
    # Only MySQL 5.7 supports microseconds storage, so it will return 0 here
    assert query("SELECT t1, t2 FROM #{table} WHERE id = 1", []) == [[~T[19:27:30], ~T[10:14:16]]]
    assert query("SELECT t1, t2 FROM #{table} WHERE id = ?", [1]) == [[~T[19:27:30], ~T[10:14:16]]]
    assert query("SELECT time('00:00:00')", []) == [[~T[00:00:00]]]
  end

  test "encode and decode datetime", context do
    datetime = ~N[2010-10-17 10:10:30]
    datetime_with_msec = ~N[2010-10-17 13:32:15.12]
    table = "test_datetimes"

    sql = ~s{CREATE TABLE #{table} (id int, dt1 datetime, dt2 datetime)}
    :ok = query(sql, [])

    insert = ~s{INSERT INTO #{table} (id, dt1, dt2) VALUES (?, ?, ?)}
    :ok = query(insert, [1, datetime, datetime_with_msec])

    # Strings
    assert query("SELECT cast(dt1 as char), cast(dt2 as char) FROM #{table} WHERE id = 1", []) ==
           [["2010-10-17 10:10:30", "2010-10-17 13:32:15"]]

    # Datetime
    # Only MySQL 5.7 supports microseconds storage, so it will return 0 here
    assert query("SELECT dt1, dt2 FROM #{table} WHERE id = 1", []) == [[~N[2010-10-17 10:10:30], ~N[2010-10-17 13:32:15]]]
    assert query("SELECT dt1, dt2 FROM #{table} WHERE id = ?", [1]) == [[~N[2010-10-17 10:10:30], ~N[2010-10-17 13:32:15]]]
  end

  test "encode and decode timestamp", context do
    timestamp = ~N[2010-10-17 10:10:30]
    timestamp_with_msec = ~N[2010-10-17 13:32:15.12]
    table = "test_timestamps"

    sql = ~s{CREATE TABLE #{table} (id int, ts1 timestamp, ts2 timestamp)}
    :ok = query(sql, [])

    insert = ~s{INSERT INTO #{table} (id, ts1, ts2) VALUES (?, ?, ?)}
    :ok = query(insert, [1, timestamp, timestamp_with_msec])

    # Strings
    assert query("SELECT cast(ts1 as char), cast(ts2 as char) FROM #{table} WHERE id = 1", []) ==
           [["2010-10-17 10:10:30", "2010-10-17 13:32:15"]]

    # Timestamp
    # Only MySQL 5.7 supports microseconds storage, so it will return 0 here
    assert query("SELECT ts1, ts2 FROM #{table} WHERE id = 1", []) == [[~N[2010-10-17 10:10:30], ~N[2010-10-17 13:32:15]]]
    assert query("SELECT ts1, ts2 FROM #{table} WHERE id = ?", [1]) == [[~N[2010-10-17 10:10:30], ~N[2010-10-17 13:32:15]]]
    assert query("SELECT timestamp('0000-00-00 00:00:00')", []) == [[~N[0000-01-01 00:00:00]]]
    assert query("SELECT timestamp('0001-01-01 00:00:00')", []) == [[~N[0001-01-01 00:00:00]]]
    assert query("SELECT timestamp('2013-12-21 23:01:27')", []) == [[~N[2013-12-21 23:01:27]]]
    assert query("SELECT timestamp('2013-12-21 23:01:27 EST')", []) == [[~N[2013-12-21 23:01:27]]]
  end

  @tag connection_opts: [datetime: :tuples]
  test "encode and decode tuples date", context do
    date0 = {2010, 10, 17}
    date1 = {0, 0, 0}
    table = "test_tuples_dates"

    sql = ~s{CREATE TABLE #{table} (id int, d date)}
    :ok = query(sql, [])

    insert = ~s{INSERT INTO #{table} (id, d) VALUES (?, ?)}
    :ok = query(insert, [1, date0])
    :ok = query(insert, [2, date1])

    assert query("SELECT d FROM #{table} WHERE id = 1", []) == [[date0]]
    assert query("SELECT d FROM #{table} WHERE id = ?", [1]) == [[date0]]
    assert query("SELECT d FROM #{table} WHERE id = ?", [2]) == [[date1]]
  end

  @tag connection_opts: [datetime: :tuples]
  test "encode and decode tuples time", context do
    time = {19, 27, 30, 0}
    time_with_msec = {10, 14, 16, 23}
    table = "test_tuples_times"

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

  @tag connection_opts: [datetime: :tuples]
  test "encode and decode tuples datetime", context do
    date = {2010, 10, 17}
    datetime = {date, {10, 10, 30, 0}}
    datetime_with_msec = {date, {13, 32, 15, 12}}
    datetime_no_msec = {date, {10, 10, 29}}
    table = "test_tuples_datetimes"

    sql = ~s{CREATE TABLE #{table} (id int, dt1 datetime, dt2 datetime, dt3 datetime)}
    :ok = query(sql, [])

    insert = ~s{INSERT INTO #{table} (id, dt1, dt2, dt3) VALUES (?, ?, ?, ?)}
    :ok = query(insert, [1, datetime, datetime_with_msec, datetime_no_msec])

    # Datetime
    # Only MySQL 5.7 supports microseconds storage, so it will return 0 here
    assert query("SELECT dt1, dt2 FROM #{table} WHERE id = 1", []) == [[datetime, {date, {13, 32, 15, 0}}]]
    assert query("SELECT dt1, dt2 FROM #{table} WHERE id = ?", [1]) == [[datetime, {date, {13, 32, 15, 0}}]]
    assert query("SELECT dt3 FROM #{table} WHERE id = ?", [1]) == [[{date, {10, 10, 29, 0}}]]
    assert query("SELECT COUNT(*) FROM #{table} WHERE dt3 = ?", [datetime_no_msec]) == [[1]]
  end

  @tag connection_opts: [datetime: :tuples]
  test "encode and decode tuples timestamp", context do
    date = {2010, 10, 17}
    timestamp = {date, {10, 10, 30, 0}}
    timestamp_with_msec = {date, {13, 32, 15, 12}}
    table = "test_tuples_timestamps"

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
    # out_of_range = max_signed + 1
    :ok = query("INSERT INTO #{table} (id, testfield) values (1, ?)", [max_signed])
    :ok = query("INSERT INTO #{table} (id, testfield) values (2, ?)", [min_signed])
    # Do not work since MySQL 5.7.9, bit test is rather invalid
    # :ok = query("INSERT INTO #{table} (id, testfield) values (3, ?)", [out_of_range])
    assert query("SELECT testfield FROM #{table} WHERE id = 1", []) == [[max_signed]]
    assert query("SELECT testfield FROM #{table} WHERE id = 2", []) == [[min_signed]]
    # assert query("SELECT testfield FROM #{table} WHERE id = 3", []) == [[max_signed]]
  end

  test "decode unsigned smallint", context do
    table = "test_smallint_unsigned"
    :ok = query("CREATE TABLE #{table} (id serial, testfield smallint UNSIGNED)", [])

    max_unsigned = 65535
    min_unsigned = 0
    # out_of_range = max_signed + 1
    :ok = query("INSERT INTO #{table} (id, testfield) values (1, ?)", [max_unsigned])
    :ok = query("INSERT INTO #{table} (id, testfield) values (2, ?)", [min_unsigned])
    # Do not work since MySQL 5.7.9, bit test is rather invalid
    # :ok = query("INSERT INTO #{table} (id, testfield) values (3, ?)", [out_of_range])
    assert query("SELECT testfield FROM #{table} WHERE id = 1", []) == [[max_unsigned]]
    assert query("SELECT testfield FROM #{table} WHERE id = 2", []) == [[min_unsigned]]
    # assert query("SELECT testfield FROM #{table} WHERE id = 3", []) == [[max_signed]]
  end

  test "decode mediumint", context do
    table = "test_mediumint"
    :ok = query("CREATE TABLE #{table} (id serial, testfield mediumint)", [])

    max_signed = 8388607
    min_signed = -8388608
    # out_of_range = max_signed + 1
    :ok = query("INSERT INTO #{table} (id, testfield) values (1, ?)", [max_signed])
    :ok = query("INSERT INTO #{table} (id, testfield) values (2, ?)", [min_signed])
    # Do not work since MySQL 5.7.9, bit test is rather invalid
    # :ok = query("INSERT INTO #{table} (id, testfield) values (3, ?)", [out_of_range])
    assert query("SELECT testfield FROM #{table} WHERE id = 1", []) == [[max_signed]]
    assert query("SELECT testfield FROM #{table} WHERE id = 2", []) == [[min_signed]]
    # assert query("SELECT testfield FROM #{table} WHERE id = 3", []) == [[max_signed]]
  end

  test "decode bigint", context do
    table = "test_bigint"
    :ok = query("CREATE TABLE #{table} (id serial, testfield bigint)", [])

    max_signed = 9223372036854775807
    min_signed = -9223372036854775808
    # out_of_range = max_signed + 1
    :ok = query("INSERT INTO #{table} (id, testfield) values (1, ?)", [max_signed])
    :ok = query("INSERT INTO #{table} (id, testfield) values (2, ?)", [min_signed])
    # Do not work since MySQL 5.7.9, bit test is rather invalid
    # :ok = query("INSERT INTO #{table} (id, testfield) values (3, ?)", [out_of_range])
    assert query("SELECT testfield FROM #{table} WHERE id = 1", []) == [[max_signed]]
    assert query("SELECT testfield FROM #{table} WHERE id = 2", []) == [[min_signed]]
    # assert query("SELECT testfield FROM #{table} WHERE id = 3", []) == [[max_signed]]
  end

  test "decode unsigned bigint", context do
    table = "test_bigint_unsigned"
    :ok = query("CREATE TABLE #{table} (id serial, testfield bigint UNSIGNED)", [])

    max_unsigned = 18446744073709551615
    min_unsigned = 0
    # out_of_range = max_signed + 1
    :ok = query("INSERT INTO #{table} (id, testfield) values (1, ?)", [max_unsigned])
    :ok = query("INSERT INTO #{table} (id, testfield) values (2, ?)", [min_unsigned])
    # Do not work since MySQL 5.7.9, bit test is rather invalid
    # :ok = query("INSERT INTO #{table} (id, testfield) values (3, ?)", [out_of_range])
    assert query("SELECT testfield FROM #{table} WHERE id = 1", []) == [[max_unsigned]]
    assert query("SELECT testfield FROM #{table} WHERE id = 2", []) == [[min_unsigned]]
    # assert query("SELECT testfield FROM #{table} WHERE id = 3", []) == [[max_signed]]
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
    assert res.columns == ["first", "last"]
    assert res.num_rows == 1
  end

  test "columns list includes table name when include_table_name option is specified", context do
    table = "table_name_test"
    :ok = query("CREATE TABLE #{table} (id serial, name varchar(50))", [])
    :ok = query("INSERT INTO #{table} (id, name) VALUES(?, ?)", [1, "test_name"])

    {:ok, res} = Mariaex.Connection.query(context[:pid], "SELECT id, name FROM #{table}", [], include_table_name: true)

    assert %Mariaex.Result{} = res
    assert res.columns == ["#{table}.id", "#{table}.name"]
  end

  test "columns list on join associates columns with the correct table", context do
    table1 = "table_name_test_1"
    table2 = "table_name_test_2"
    :ok = query("CREATE TABLE #{table1} (id serial, name varchar(50))", [])
    :ok = query("CREATE TABLE #{table2} (id serial, table1_id integer, name varchar(50))", [])

    :ok = query("INSERT INTO #{table1} (id, name) VALUES(?, ?)", [1, "test_name_1"])
    :ok = query("INSERT INTO #{table2} (id, table1_id, name) VALUES(?, ?, ?)", [10, 1, "test_name_2"])

    {:ok, res} = Mariaex.Connection.query(context[:pid], "SELECT * FROM #{table1} JOIN #{table2} ON #{table1}.id = #{table2}.table1_id", [], include_table_name: true)

    assert res.columns == ["#{table1}.id", "#{table1}.name", "#{table2}.id", "#{table2}.table1_id", "#{table2}.name"]
    assert res.num_rows == 1
    assert List.first(res.rows) == [1, "test_name_1", 10, 1, "test_name_2"]
  end

  test "columns list uses alias as table name when inclue_table_name option is specified", context do
    table = "table_alias_test"
    :ok = query("CREATE TABLE #{table} (id serial, name varchar(50))", [])
    :ok = query("INSERT INTO #{table} (id, name) VALUES(?, ?)", [1, "test_name"])

    {:ok, res} = Mariaex.Connection.query(context[:pid], "SELECT id, name FROM #{table} t1", [], include_table_name: true)

    assert %Mariaex.Result{} = res
    assert res.columns == ["t1.id", "t1.name"]
  end

  test "result struct on update", context do
    table = "struct_on_update"

    :ok = query(~s{CREATE TABLE #{table} (num int)}, [])
    :ok = query(~s{INSERT INTO #{table} (num) VALUES (?)}, [1])

    {:ok, res} = Mariaex.Connection.query(context[:pid], "UPDATE #{table} SET num = 2", [])
    assert %Mariaex.Result{} = res
    assert res.num_rows == 1

    {:ok, res} = Mariaex.Connection.query(context[:pid], "UPDATE #{table} SET num = 2", [])
    assert %Mariaex.Result{} = res
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
    assert %ArgumentError{message: "parameters must be of length 0 for query" <> _} = catch_error(query("SELECT 1", [:badparam]))
    assert %ArgumentError{message: "query has invalid parameter :badparam"} = catch_error(query("SELECT ?", [:badparam]))
  end

  test "non ascii character", context do
    :ok = query("CREATE TABLE test_charset (id int, text text)", [])
    :ok = query("INSERT INTO test_charset VALUES (?, ?)", [1, "忍者"])

    assert query("SELECT * FROM test_charset where id = 1", []) == [[1, "忍者"]]
  end

  test "non ascii character to latin1 table", context do
    :ok = query("CREATE TABLE test_charset_latin1 (id int, text text) DEFAULT CHARSET=latin1", [])
    :ok = query("INSERT INTO test_charset_latin1 VALUES (?, ?)", [1, "ÖÄÜß"])

    assert query("SELECT * FROM test_charset_latin1 where id = 1", []) == [[1, "ÖÄÜß"]]
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
    _ = query("SHOW FULL PROCESSLIST", [])

    :ok = query("CREATE TABLE test_describe (id int)", [])
    assert query("DESCRIBE test_describe", []) == [["id", "int(11)", "YES", "", nil, ""]]
  end

  test "execute call procedure stream", context do
    sql =
      """
      CREATE PROCEDURE queryproc (IN a INT, IN b INT)
      BEGIN
      SELECT a + b;
      END
      """
    assert :ok = query(sql, [])
    assert query("CALL queryproc(1, 2)", []) == [[3]]
  end

  test "execute call procedure stream without results", context do
    assert :ok = query("CREATE TABLE test_command_proc (id int)", [])
    assert :ok = query("INSERT INTO test_command_proc VALUES(1)", [])
    assert query("SELECT COUNT(*) FROM test_command_proc", []) == [[1]]
    sql =
      """
      CREATE PROCEDURE commandproc ()
      BEGIN
      DELETE FROM test_command_proc;
      END
      """
    assert :ok = query(sql, [])
    assert :ok = query("CALL commandproc()", [])
    assert query("SELECT COUNT(*) FROM test_command_proc", []) == [[0]]
  end

  test "replace statement", context do
    date = {2014, 8, 20}
    timestamp = {date, {18, 47, 42, 0}}
    sql = "CREATE TABLE test_replace (id INT UNSIGNED NOT NULL AUTO_INCREMENT," <>
          "data VARCHAR(64) DEFAULT NULL, ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," <>
          "PRIMARY KEY (id));"
    assert :ok = query(sql, [])
    assert :ok = query("REPLACE INTO test_replace VALUES (1, 'Old', '2014-08-20 18:47:00');", [])
    assert :ok = query("REPLACE INTO test_replace VALUES (1, 'New', ?);", [timestamp])
  end

  @tag :json
  test "encode and decode json", context do
    map = %{"hoge" => "1", "huga" => "2"}
    map_string = ~s|{"hoge": "1", "huga": "2"}|

    table = "test_jsons"

    sql = ~s{CREATE TABLE #{table} (id int, map json)}
    :ok = query(sql, [])

    insert = ~s{INSERT INTO #{table} (id, map) VALUES (?, ?)}
    :ok = query(insert, [1, map_string])

    assert query("SELECT map FROM #{table} WHERE id = 1", []) == [[map]]
  end
end
