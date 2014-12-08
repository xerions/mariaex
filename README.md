Mariaex
=======

Version: 0.0.0

```elixir
  iex(1)> {:ok, p} = Mariaex.Connection.start_link(%{user: "ecto", database: "ecto_test"})
  {:ok, #PID<0.108.0>}

  iex(2)> Mariaex.Connection.query(p, "CREATE TABLE test1 (id serial, title text)")
  {:ok_resp, 0, 0, 0, 2, 0, ""} # returned raw decoded records at the moment
```
