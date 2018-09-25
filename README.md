Mariaex [![Build Status](https://travis-ci.org/xerions/mariaex.svg)](https://travis-ci.org/xerions/mariaex) [![Coverage Status](https://coveralls.io/repos/xerions/mariaex/badge.svg?branch=master&service=github)](https://coveralls.io/github/xerions/mariaex?branch=master) [![Deps Status](https://beta.hexfaktor.org/badge/all/github/xerions/mariaex.svg)](https://beta.hexfaktor.org/github/xerions/mariaex)
=======

## Usage

Add Mariaex as a dependency in your `mix.exs` file.

```elixir
def deps do
  [{:mariaex, "~> 0.8.2"} ]
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
    rows: [[1, "test"], [2, "test2"]}}
```

## Configuration

Important configuration, which depends on used charset for support unicode chars, see `:binary_as`
in `Mariaex.start_link/1`

### JSON library

As default, [Poison](https://github.com/devinus/poison) is used for JSON library in mariaex to support JSON column.

If you want to use another library, please set `config.exs` like below.

```elixir
config :mariaex, json_library: SomeLibrary
```
=======
## Data representation

    MySQL                 Elixir
    ----------            ------
    NULL                  nil
    TINYINT               42
    INT                   42
    BIGINT                42
    FLOAT                 42.0
    DOUBLE                42.0
    DECIMAL               #Decimal<42.0> *
    VARCHAR               "eric"
    TEXT                  "eric"
    BLOB                  <<42>>
    DATE                  %Date{year: 2013, month: 10, day: 12}
    TIME                  %Time{hour: 0, minute: 37, second: 14} **
    YEAR                  2013
    DATETIME              %DateTime{year: 2013 month: 10, day: 12, hour: 0, minute: 37, second: 14} **
    TIMESTAMP             %DateTime{year: 2013 month: 10, day: 12, hour: 0, minute: 37, second: 14} **
    BIT                   << 1 >>
    GEOMETRY/POINT        %Mariaex.Geometry.Point{coordinates: {1.0, -1.0}, srid: 42}
    GEOMETRY/LINESTRING   %Mariaex.Geometry.LineString{coordinates: [{0.0, 0.0}, {10.0, 10.0}, {20.0, 25.0}, {50.0, 60.0}], srid: 0}
    GEOMETRY/POLYGON      %Mariaex.Geometry.Polygon{coordinates: [[{0.0, 0.0}, {10.0, 0.0}, {10.0, 10.0}, {0.0, 10.0}, {0.0, 0.0}], [{5.0, 5.0}, {7.0, 5.0}, {7.0, 7.0}, {5.0, 7.0}, {5.0, 5.0}]], srid: 0}
