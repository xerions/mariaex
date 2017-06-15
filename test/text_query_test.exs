defmodule TextQueryTest do
  use ExUnit.Case, async: true
  import Mariaex.TestHelper

  setup_all do
    opts = [database: "mariaex_test", username: "mariaex_user", password: "mariaex_pass", backoff_type: :stop]
    {:ok, pid} = Mariaex.Connection.start_link(opts)
    # drop = "DROP TABLE IF EXISTS test"
    # {:ok, _} = Mariaex.execute(pid, %Mariaex.Query{type: :text, statement: drop}, [])
    create = """
    CREATE TABLE test_text_query_table (
    id serial,
    bools boolean,
    bits bit(2),
    varchars varchar(20),
    texts text(20),
    floats double,
    ts timestamp,
    dt datetime
    )
    """
    {:ok, _} = Mariaex.query(pid, create, [], [query_type: :text])
    insert = """
    INSERT INTO test_text_query_table (id, bools, bits, varchars, texts, floats, ts, dt)
    VALUES
    (1, true, b'10', 'hello', 'world', 1.1, '2016-09-26 16:36:06', '0001-01-01 00:00:00'),
    (2, false, b'11', 'goodbye', 'earth', 1.2, '2016-09-26T16:36:07', '0001-01-01 00:00:01')
    """
    {:ok, _} = Mariaex.query(pid, insert, [], [query_type: :text])

    if System.get_env "MYSQL_5_7" do
      create = """
      CREATE TABLE test_text_json_query_table (
      id serial,
      map json,
      dt datetime
      )
      """
      {:ok, _} = Mariaex.query(pid, create, [], [query_type: :text])
      insert = """
      INSERT INTO test_text_json_query_table (id, map, dt)
      VALUES
      (1, '{"hoge": "1", "huga": "2"}', '2017-01-01 00:00:00'),
      (2, '{"hoge": "3", "huga": "4"}', '2017-01-01 00:00:01')
      """
      {:ok, _} = Mariaex.query(pid, insert, [], [query_type: :text])
    end

    {:ok, [pid: pid]}
  end

  test "select int", context do
    rows = execute_text("SELECT id FROM test_text_query_table", [])
    assert(rows == [[1], [2]])
  end

  test "select bool", context do
    # bool is tinyint
    rows = execute_text("SELECT bools FROM test_text_query_table", [])
    assert(rows == [[1], [0]])
  end

  test "select bits", context do
    rows = execute_text("SELECT bits FROM test_text_query_table", [])
    assert(rows == [[<<2>>], [<<3>>]])
  end

  test "select string", context do
    rows = execute_text("SELECT floats FROM test_text_query_table", [])
    assert(rows == [[1.1], [1.2]])
  end

  test "select float", context do
    rows = execute_text("SELECT floats FROM test_text_query_table", [])
    assert(rows == [[1.1], [1.2]])
  end

  test "select timestamp", context do
    rows = execute_text("SELECT ts FROM test_text_query_table", [])
    assert(rows == [[{{2016, 9, 26}, {16, 36, 06, 0}}], [{{2016, 9, 26}, {16, 36, 07, 0}}]])
  end

  test "select datetime", context do
    rows = execute_text("SELECT dt FROM test_text_query_table", [])
    assert(rows == [[{{1,1,1}, {0,0,0,0}}], [{{1,1,1}, {0,0,1,0}}]])
  end

  test "select multiple columns", context do
    rows = execute_text("SELECT id, varchars FROM test_text_query_table", [])
    assert(rows == [[1, "hello"], [2, "goodbye"]])
  end

  if System.get_env "MYSQL_5_7" do
    test "select json", context do
      opts = [json_library: Poison]

      rows = execute_text("SELECT map FROM test_text_json_query_table", [], opts)
      assert(rows == [[%{"hoge" => "1", "huga" => "2"}], [%{"hoge" => "3", "huga" => "4"}]])
    end
  end
end
