Mariaex
=======

Version: 0.0.0

```elixir
  iex(1)> {:ok, p} = Mariaex.Connection.start_link(%{user: "ecto", database: "ecto_test"})
  {:ok, #PID<0.108.0>}

  iex(2)> Mariaex.Connection.query(p, "CREATE TABLE test1 (id serial, title text)")
  {:ok_resp, 0, 0, 0, 2, 0, ""} # returned raw decoded records at the moment

  iex(3)> Mariaex.Connection.query(p, "INSERT INTO test1 VALUES(1, 'test')")
  {:ok_resp, 0, 1, 1, 2, 0, ""}

  iex(4)> Mariaex.Connection.query(p, "INSERT INTO test1 VALUES(2, 'test2')")
  {:ok_resp, 0, 1, 1, 2, 0, ""}

  iex(5)> Mariaex.Connection.query(p, "SELECT id, title FROM test1")
  {[{:column_definition_41, "def", "ecto_test", "test1", "test1", "title",
     "title", 12, 8, 65535, 252, 16, 0},
    {:column_definition_41, "def", "ecto_test", "test1", "test1", "id", "id", 12,
     63, 20, 8, 16931, 0}], [row: ["2", "test2"], row: ["1", "test"]]}
```
