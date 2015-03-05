Mariaex [![Build Status](https://travis-ci.org/xerions/mariaex.svg)](https://travis-ci.org/xerions/mariaex)
=======

## Usage

Add Mariaex as a dependency in your `mix.exs` file.

```elixir
def deps do
  [{:mariaex, "~> 0.1"} ]
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

## License

Copyright 2015 Travelping

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
