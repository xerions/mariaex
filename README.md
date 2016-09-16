Mariaex [![Build Status](https://travis-ci.org/xerions/mariaex.svg)](https://travis-ci.org/xerions/mariaex) [![Coverage Status](https://coveralls.io/repos/xerions/mariaex/badge.svg?branch=master&service=github)](https://coveralls.io/github/xerions/mariaex?branch=master) [![Deps Status](https://beta.hexfaktor.org/badge/all/github/xerions/mariaex.svg)](https://beta.hexfaktor.org/github/xerions/mariaex)
=======

## Usage

Add Mariaex as a dependency in your `mix.exs` file.

```elixir
def deps do
  [{:mariaex, "~> 0.7.3"} ]
end
```

After you are done, run `mix deps.get` in your shell to fetch and compile Mariaex. Start an interactive Elixir shell with `iex -S mix`.

```elixir
  iex(1)> {:ok, p} = Mariaex.start_link(username: "ecto", database: "ecto_test")
  {:ok, #PID<0.108.0>}

  iex(2)> Mariaex.query(p, "CREATE TABLE test1 (id serial, title text)")
  {:ok, %Mariaex.Result{columns: [], command: :create, num_rows: 0, rows: []}}

  iex(3)> Mariaex.query(p, "INSERT INTO test1 VALUES(1, 'test')")
  {:ok, %Mariaex.Result{columns: [], command: :insert, num_rows: 1, rows: []}}

  iex(4)> Mariaex.query(p, "INSERT INTO test1 VALUES(2, 'test2')")
  {:ok, %Mariaex.Result{columns: [], command: :insert, num_rows: 1, rows: []}}

  iex(5)> Mariaex.query(p, "SELECT id, title FROM test1")
  {:ok,
   %Mariaex.Result{columns: ["id", "title"], command: :select, num_rows: 2,
    rows: [{1, "test"}, {2, "test2"}]}}
```

## Configuration

Important configuration, which depends on used charset for support unicode chars, see `:binary_as`
in `Mariaex.start_link/1`
