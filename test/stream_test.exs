defmodule StreamTest do
  use ExUnit.Case, async: true
  import Mariaex.TestHelper

  setup_all do
    {:ok, pid} = connect()
    {:ok, _} = Mariaex.query(pid, "CREATE TABLE stream (id int, text text)", [])
    {:ok, _} = Mariaex.query(pid, "INSERT INTO stream VALUES (1, 'foo'), (2, 'bar')", [])
    :sys.terminate(pid, :normal)
    :ok
  end

  setup do
    {:ok, pid} = connect()
    {:ok, [pid: pid]}
  end

  test "simple text stream", context do
    assert Mariaex.transaction(context[:pid], fn(conn) ->
      stream = Mariaex.stream(conn, "SELECT * FROM stream", [], [])
      assert [%Mariaex.Result{num_rows: 0, rows: []},
              %Mariaex.Result{num_rows: 2, rows: [[1, "foo"], [2, "bar"]]}] =
        Enum.to_list(stream)
      :done
    end) == {:ok, :done}
  end

  test "simple unnamed unprepared stream", context do
    assert Mariaex.transaction(context[:pid], fn(conn) ->
      stream = Mariaex.stream(conn, "SELECT * FROM stream", [], [])
      assert [%Mariaex.Result{num_rows: 0, rows: []},
              %Mariaex.Result{num_rows: 2, rows: [[1, "foo"], [2, "bar"]]}] =
        Enum.to_list(stream)
      :done
    end) == {:ok, :done}
    assert [[1, "foo"], [2, "bar"]] = query("SELECT * FROM stream", [])
  end

  test "simple unnamed prepared stream", context do
    query = prepare("", "SELECT * FROM stream")
    assert Mariaex.transaction(context[:pid], fn(conn) ->
      stream = Mariaex.stream(conn, query, [], [])
      assert [%Mariaex.Result{num_rows: 0, rows: []},
              %Mariaex.Result{num_rows: 2, rows: [[1, "foo"], [2, "bar"]]}] =
        Enum.to_list(stream)
      :done
    end) == {:ok, :done}
    assert [[1, "foo"], [2, "bar"]] = execute(query, [])
  end

  test "simple named prepared stream", context do
    query = prepare("stream", "SELECT * FROM stream")
    assert Mariaex.transaction(context[:pid], fn(conn) ->
      stream = DBConnection.stream(conn, query, [], [])
      assert [%Mariaex.Result{num_rows: 0, rows: []},
              %Mariaex.Result{num_rows: 2, rows: [[1, "foo"], [2, "bar"]]}] =
        Enum.to_list(stream)
      :done
    end) == {:ok, :done}
    assert [[1, "foo"], [2, "bar"]] = execute(query, [])
  end

  test "interleaving unnamed prepared stream", context do
    query = prepare("", "SELECT * FROM stream")
    assert Mariaex.transaction(context[:pid], fn(conn) ->
      stream = Mariaex.stream(conn, query, [], [])
      assert [{%Mariaex.Result{num_rows: 0, rows: []},
               %Mariaex.Result{num_rows: 0, rows: []}},
              {%Mariaex.Result{num_rows: 2, rows: [[1, "foo"], [2, "bar"]]},
               %Mariaex.Result{num_rows: 2, rows: [[1, "foo"], [2, "bar"]]}}] =
        Enum.zip(stream, stream)
      :done
    end) == {:ok, :done}
    assert [[1, "foo"], [2, "bar"]] = execute(query, [])
  end

  test "interleaving named prepared stream", context do
    query = prepare("stream", "SELECT * FROM stream")
    assert Mariaex.transaction(context[:pid], fn(conn) ->
      stream = Mariaex.stream(conn, query, [], [])
      assert [{%Mariaex.Result{num_rows: 0, rows: []},
               %Mariaex.Result{num_rows: 0, rows: []}},
              {%Mariaex.Result{num_rows: 2, rows: [[1, "foo"], [2, "bar"]]},
               %Mariaex.Result{num_rows: 2, rows: [[1, "foo"], [2, "bar"]]}}] =
        Enum.zip(stream, stream)
      :done
    end) == {:ok, :done}
    assert [[1, "foo"], [2, "bar"]] = execute(query, [])
  end

  test "split on max_rows stream", context do
    query = prepare("stream", "SELECT * FROM stream")
    assert Mariaex.transaction(context[:pid], fn(conn) ->
      stream = DBConnection.stream(conn, query, [], [max_rows: 1])
      assert [%Mariaex.Result{num_rows: 0, rows: []},
              %Mariaex.Result{num_rows: 1, rows: [[1, "foo"]]},
              %Mariaex.Result{num_rows: 1, rows: [[2, "bar"]]},
              %Mariaex.Result{num_rows: 0, rows: []}] =
        Enum.to_list(stream)
      :done
    end) == {:ok, :done}
    assert [[1, "foo"], [2, "bar"]] = execute(query, [])
  end

  test "take first result with stream", context do
    query = prepare("stream", "SELECT * FROM stream")
    assert Mariaex.transaction(context[:pid], fn(conn) ->
      stream = DBConnection.stream(conn, query, [], [])
      assert [%Mariaex.Result{num_rows: 0, rows: []}] = Enum.take(stream, 1)
      :done
    end) == {:ok, :done}
    assert [[1, "foo"], [2, "bar"]] = execute(query, [])
  end

  test "insert stream", context do
    assert Mariaex.transaction(context[:pid], fn(conn) ->
      stream = Mariaex.stream(conn, "UPDATE stream SET text='foo' WHERE id=?", [1], [])
      assert [%Mariaex.Result{num_rows: 1, rows: nil}] = Enum.to_list(stream)
      :done
    end) == {:ok, :done}
  end

  test "select empty rows stream", context do
    assert Mariaex.transaction(context[:pid], fn(conn) ->
      stream = Mariaex.stream(conn, "SELECT * FROM stream WHERE id=?", [42], [])
      assert [%Mariaex.Result{num_rows: 0, rows: []},
              %Mariaex.Result{num_rows: 0, rows: []}] = Enum.to_list(stream)
      :done
    end) == {:ok, :done}
  end

  test "call procedure stream", context do
    sql =
      """
      CREATE PROCEDURE streamproc ()
      BEGIN
      SELECT * FROM stream;
      END
      """
    assert :ok = query(sql, [])
    assert Mariaex.transaction(context[:pid], fn(conn) ->
      stream = Mariaex.stream(conn, "CALL streamproc()", [], [])
      assert [%Mariaex.Result{num_rows: 2, rows: [[1, "foo"], [2, "bar"]]}] = Enum.to_list(stream)

      assert %Mariaex.Result{rows: [[42]]} = Mariaex.query!(conn, "SELECT 42", [])
      :done
    end) == {:ok, :done}
  end

  defp connect() do
    opts = [database: "mariaex_test", username: "mariaex_user", password: "mariaex_pass", backoff_type: :stop]
    Mariaex.Connection.start_link(opts)
  end
end
