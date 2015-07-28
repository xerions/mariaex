Mariaex [![Build Status](https://travis-ci.org/xerions/mariaex.svg)](https://travis-ci.org/xerions/mariaex) [![Coverage Status](https://coveralls.io/repos/xerions/mariaex/badge.svg?branch=master&service=github)](https://coveralls.io/github/xerions/mariaex?branch=master)
=======

## Usage

Add Mariaex as a dependency in your `mix.exs` file.

```elixir
def deps do
  [{:mariaex, "~> 0.4.2"} ]
end
```

After you are done, run `mix deps.get` in your shell to fetch and compile Mariaex. Start an interactive Elixir shell with `iex -S mix`.

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
