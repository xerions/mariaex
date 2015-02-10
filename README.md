Mariaex [![Build Status](https://travis-ci.org/liveforeverx/mariaex.svg)](https://travis-ci.org/liveforeverx/mariaex)
=======

Version: 0.0.1-dev

```elixir
  iex(1)> {:ok, p} = Mariaex.Connection.start_link(username: "ecto", database: "ecto_test")
  {:ok, #PID<0.108.0>}

  iex(2)> Mariaex.Connection.query(p, "CREATE TABLE test1 (id serial, title text)")
  {:ok, %Mariaex.Result{columns: [], command: :create, num_rows: 0, rows: []}}

  iex(3)> Mariaex.Connection.query(p, "INSERT INTO test1 VALUES(1, 'test')")
  {:ok, %Mariaex.Result{columns: [], command: :insert, num_rows: 1, rows: []}}

  iex(4)> Mariaex.Connection.query(p, "INSERT INTO test1 VALUES(2, 'test2')")
  {:ok, %Mariaex.Result{columns: [], command: :insert, num_rows: 1, rows: []}}

  iex(5)> Mariaex.Connection.query(p, "SELECT id, title FROM test1")
  {:ok,
   %Mariaex.Result{columns: ["id", "title"], command: :select, num_rows: 2,
    rows: [{1, "test"}, {2, "test2"}]}}
```
