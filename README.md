Mariaex
=======

Version: 0.0.1

```elixir
  iex(1)> {:ok, p} = Mariaex.Connection.start_link(%{user: "ecto", database: "ecto_test"})
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
  iex(6)> Mariaex.Connection.in_transaction(p, fn() ->
                                                 Mariaex.Connection.query!(p, "INSERT INTO test1 VALUES(3, 'test3')")
                                                 Mariaex.Connection.query!(p, "INSERT INTO test1 VALUES(2, 'test2')")
                                               end)
  ** (Mariaex.Error) (1062): Duplicate entry '2' for key 'id'
   lib/mariaex/mariaex.ex:128: Mariaex.Connection.query!/4
   lib/mariaex/mariaex.ex:237: Mariaex.Connection.in_transaction/3
  iex(7)> Mariaex.Connection.query(p, "SELECT id, title FROM test1")
  {:ok,
   %Mariaex.Result{columns: ["id", "title"], command: :select, num_rows: 2,
    rows: [{1, "test"}, {2, "test2"}]}}
```
