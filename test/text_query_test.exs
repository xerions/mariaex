defmodule TextQueryTest do
  use ExUnit.Case, async: true
  import Mariaex.TestHelper

  setup do
    opts = [database: "mariaex_test", username: "mariaex_user", password: "mariaex_pass", backoff_type: :stop]
    {:ok, pid} = Mariaex.Connection.start_link(opts)
    drop = "DROP TABLE IF EXISTS test"
    {:ok, _} = Mariaex.execute(pid, %Mariaex.Query{type: :text, statement: drop}, [])
    create = """
    CREATE TABLE test (
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
    {:ok, _} = Mariaex.execute(pid, %Mariaex.Query{type: :text, statement: create}, [])
    insert = """
    INSERT INTO test (id, bools, bits, varchars, texts, floats, ts, dt)
    VALUES
    (1, true, b'10', 'hello', 'world', 1.1, '2016-09-26 16:36:06', '0001-01-01 00:00:00'),
    (2, false, b'11', 'goodbye', 'earth', 1.2, '2016-09-26T16:36:07', '0001-01-01 00:00:01')
    """
    {:ok, _} = Mariaex.execute(pid, %Mariaex.Query{type: :text, statement: insert}, [])
    {:ok, [pid: pid]}
  end

  test "select int", context do
    rows = execute_text("SELECT id FROM test", [])
    assert(rows == [[1], [2]])
  end

  test "select bool", context do
    # bool is tinyint
    rows = execute_text("SELECT bools FROM test", [])
    assert(rows == [[1], [0]])
  end

  test "select bits", context do
    rows = execute_text("SELECT bits FROM test", [])
    assert(rows == [[<<2>>], [<<3>>]])
  end

  test "select string", context do
    rows = execute_text("SELECT floats FROM test", [])
    assert(rows == [[1.1], [1.2]])
  end

  test "select float", context do
    rows = execute_text("SELECT floats FROM test", [])
    assert(rows == [[1.1], [1.2]])
  end

  test "select timestamp", context do
    rows = execute_text("SELECT ts FROM test", [])
    assert(rows == [[{{2016, 9, 26}, {16, 36, 06, 0}}], [{{2016, 9, 26}, {16, 36, 07, 0}}]])
  end

  test "select datetime", context do
    rows = execute_text("SELECT dt FROM test", [])
    assert(rows == [[{{1,1,1}, {0,0,0,0}}], [{{1,1,1}, {0,0,1,0}}]])
  end

  test "select multiple columns", context do
    rows = execute_text("SELECT id, varchars FROM test", [])
    assert(rows == [[1, "hello"], [2, "goodbye"]])
  end

  test "decode text row" do
    # rows are consed onto the front, so they are backwards
    rows = [["Goodbye", "2", "0.11111"],
            ["Hello", "1", "2.0000"]] |> Enum.map(&(length_encode_row/1))
    res = %Mariaex.Result{
      columns: nil, command: nil, connection_id: nil, last_insert_id: nil, num_rows: nil, rows: rows}
    types = [
      %Mariaex.Column{flags: 0, name: "someFloat", table: "", type: 4},
      %Mariaex.Column{flags: 0, name: "someInt", table: "", type: 2},
      %Mariaex.Column{flags: 0, name: "someText", table: "", type: 254},
    ]
    qry = %Mariaex.Query{type: :text,
                         statement: "SELECT someText, someInt, someFloat FROM someTable"}
    obs = DBConnection.Query.decode(qry, {res, types}, [])
    exp = %Mariaex.Result{res |
                          rows: [["Hello", 1, 2.0000], ["Goodbye", 2, 0.11111]],
                          columns: ["someText", "someInt", "someFloat"],
                          num_rows: 2,
                          command: :select}
    assert(obs == exp)
  end

end
