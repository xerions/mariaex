defmodule Mariaex do
  @moduledoc """
  Main API for Mariaex.
  """

  alias Mariaex.Protocol
  alias Mariaex.Query

  @timeout 5000

  ## Helper to raise error
  defmacrop arg_error_raiser(result) do
    quote do
      case unquote(result) do
        {:error, %ArgumentError{} = error} ->
          raise error
        result ->
          result
      end
    end
  end

  @typedoc """
  A connection process name, pid or reference.

  A connection reference is used when making multiple requests to the same
  connection, see `transaction/3` and `:after_connect` in `start_link/1`.
  """
  @type conn :: DBConnection.conn

  @pool_timeout 5000
  @timeout 5000
  @idle_timeout 5000
  @max_rows 500
  ### PUBLIC API ###

  @doc """
  Start the connection process and connect to mariadb.

  ## Options

    * `:hostname` - Server hostname (default: MDBHOST env variable, then localhost);
    * `:port` - Server port (default: MDBPORT env var, then 3306);
    * `:sock_type` - Socket type (default: :tcp);
    * `:database` - Database (required, if `:skip_database` not true);
    * `:skip_database` - Flag to set connection without database;
    * `:username` - Username (default: MDBUSER env variable, then USER env var);
    * `:password` - User password (default MDBPASSWORD);
    * `:encoder` - Custom encoder function;
    * `:decoder` - Custom decoder function;
    * `:sync_connect` - Block in `start_link/1` until connection is set up (default: `false`)
    * `:formatter` - Function deciding the format for a type;
    * `:parameters` - Keyword list of connection parameters;
    * `:timeout` - Connect timeout in milliseconds (default: #{@timeout});
    * `:charset` - Database encoding (default: "utf8");
    * `:socket_options` - Options to be given to the underlying socket;
    * `:cache_size` - Prepared statement cache size (default: 100);
    * `:insecure_auth` - Secure authorization (default: false)
    * `:after_connect` - A function to run on connect, either a 1-arity fun
       called with a connection reference, `{module, function, args}` with the
       connection reference prepended to `args` or `nil`, (default: `nil`)
    * `:idle_timeout` - Idle timeout to ping database to maintain a connection
       (default: `#{@idle_timeout}`)
    * `:backoff_start` - The first backoff interval when reconnecting (default:
      `200`);
    * `:backoff_max` - The maximum backoff interval when reconnecting (default:
      `15_000`);
    * `:backoff_type` - The backoff strategy when reconnecting, `:stop` for no
       backoff and to stop (see `:backoff`, default: `:jitter`)
    * `:transactions` - Set to `:strict` to error on unexpected transaction
      state, otherwise set to `naive` (default: `:naive`);
    * `:idle` - Either `:active` to asynchronously detect TCP disconnects when
      idle or `:passive` not to (default: `:passive`);
    * `:pool` - The pool module to use, see `DBConnection` for pool dependent
      options, this option must be included with all requests contacting the pool
     if not `DBConnection.Connection` (default: `DBConnection.Connection`);

  ## Function signatures

      @spec encoder(info :: TypeInfo.t, default :: fun, param :: term) ::
            binary
      @spec decoder(info :: TypeInfo.t, default :: fun, bin :: binary) ::
            term
      @spec formatter(info :: TypeInfo.t) ::
            :binary | :text | nil
  """
  @spec start_link(Keyword.t) :: {:ok, pid} | {:error, Mariaex.Error.t | term}
  def start_link(opts) do
    DBConnection.start_link(Protocol, opts)
  end

  @doc """
  Runs an (extended) query and returns the result as `{:ok, %Mariaex.Result{}}`
  or `{:error, %Mariaex.Error{}}` if there was an error. Parameters can be
  set in the query as `?` embedded in the query string. Parameters are given as
  a list of elixir values. See the README for information on how Mariaex
  encodes and decodes elixir values by default. See `Mariaex.Result` for the
  result data.

  A *type hinted* query is run if both the options `:param_types` and
  `:result_types` are given. One client-server round trip can be saved by
  providing the types to Mariaex because the server doesn't have to be queried
  for the types of the parameters and the result.

  ## Options

    * `:timeout` - Call timeout (default: `#{@timeout}`)
    * `:param_types` - A list of type names for the parameters
    * `:result_types` - A list of type names for the result rows
    * `:decode` - If the result set decoding should be done automatically
      (`:auto`) or manually (`:manual`) via `decode/2`. Defaults to `:auto`.
    * `:pool_timeout` - Time to wait in the queue for the connection
      (default: `#{@pool_timeout}`)
    * `:queue` - Whether to wait for connection in a queue (default: `true`);
    * `:timeout` - Query request timeout (default: `#{@timeout}`);
    * `:encode_mapper` - Fun to map each parameter before encoding, see
       (default: `fn x -> x end`)
    * `:decode_mapper` - Fun to map each row in the result to a term after
       decoding, (default: `fn x -> x end`);
    * `:include_table_name` - Boolean specifying whether the `columns` list in
       the result prepends the table name to the column name with a period.
       (default `false`)
    * `:binary_as` - encoding binary as `:field_type_var_string` (default)
       or `:field_type_blob`

  ## Examples

      Mariaex.query(pid, "CREATE TABLE posts (id serial, title text)")

      Mariaex.query(pid, "INSERT INTO posts (title) VALUES ('my title')")

      Mariaex.query(pid, "SELECT title FROM posts", [])

      Mariaex.query(pid, "SELECT id FROM posts WHERE title like ?", ["%my%"])

      Mariaex.query(pid, "SELECT ? || ?", ["4", "2"],
                                param_types: ["text", "text"], result_types: ["text"])

  """
  @spec query(conn, iodata, list, Keyword.t) :: {:ok, Mariaex.Result.t} | {:error, Mariaex.Error.t}
  def query(conn, statement, params \\ [], opts \\ []) do
    case DBConnection.prepare_execute(conn, %Query{statement: statement}, params, defaults(opts)) do
      {:ok, _, result} ->
        {:ok, result}
      {:error, %ArgumentError{} = err} ->
        raise err
      {:error, _} = error ->
        error
    end
  end

  @doc """
  Runs an (extended) query and returns the result or raises `Mariaex.Error` if
  there was an error. See `query/3`.
  """
  def query!(conn, statement, params \\ [], opts \\ []) do
    {_, result} = DBConnection.prepare_execute!(conn, %Query{statement: statement}, params, defaults(opts))
    result
  end

  @doc """
  Prepares an query and returns the result as
  `{:ok, %Mariaex.Query{}}` or `{:error, %Mariaex.Error{}}` if there was an
  error. Parameters can be set in the query as `?` embedded in the query
  string. To execute the query call `execute/4`. To close the prepared query
  call `close/3`. See `Mariaex.Query` for the query data.

  ## Options

    * `:pool_timeout` - Time to wait in the queue for the connection
    (default: `#{@pool_timeout}`)
    * `:queue` - Whether to wait for connection in a queue (default: `true`);
    * `:timeout` - Prepare request timeout (default: `#{@timeout}`);
    * `:pool` - The pool module to use, must match that set on
    `start_link/1`, see `DBConnection`

  ## Examples

      Mariaex.prepare(conn, "CREATE TABLE posts (id serial, title text)")
  """
  @spec prepare(conn, iodata, iodata, Keyword.t) :: {:ok, Mariaex.Query.t} | {:error, Mariaex.Error.t}
  def prepare(conn, name, statement, opts \\ []) do
    query = %Query{name: name, statement: statement}
    DBConnection.prepare(conn, query, defaults(opts))
    |> arg_error_raiser
  end

  @doc """
  Prepared an (extended) query and returns the prepared query or raises
  `Mariaex.Error` if there was an error. See `prepare/4`.
  """
  @spec prepare!(conn, iodata, iodata, Keyword.t) :: Mariaex.Query.t
  def prepare!(conn, name, statement, opts \\ []) do
    query = %Query{name: name, statement: statement}
    DBConnection.prepare!(conn, query, defaults(opts))
  end

  @doc """
  Runs an (extended) prepared query and returns the result as
  `{:ok, %Mariaex.Result{}}` or `{:error, %Mariaex.Error{}}` if there was an
  error. Parameters are given as part of the prepared query, `%Mariaex.Query{}`.
  See the README for information on how Mariaex encodes and decodes Elixir
  values by default. See `Mariaex.Query` for the query data and
  `Mariaex.Result` for the result data.

  ## Options

    * `:pool_timeout` - Time to wait in the queue for the connection
    (default: `#{@pool_timeout}`)
    * `:queue` - Whether to wait for connection in a queue (default: `true`);
    * `:timeout` - Execute request timeout (default: `#{@timeout}`);
    * `:decode_mapper` - Fun to map each row in the result to a term after
    decoding, (default: `fn x -> x end`);
    * `:pool` - The pool module to use, must match that set on
    `start_link/1`, see `DBConnection`

  ## Examples

      query = Mariaex.prepare!(conn, "CREATE TABLE posts (id serial, title text)")
      Mariaex.execute(conn, query, [])

      query = Mariaex.prepare!(conn, "SELECT id FROM posts WHERE title like $1")
      Mariaex.execute(conn, query, ["%my%"])
  """
  @spec execute(conn, Mariaex.Query.t, list, Keyword.t) ::
    {:ok, Mariaex.Result.t} | {:error, Mariaex.Error.t}
  def execute(conn, query, params, opts \\ []) do
    DBConnection.execute(conn, query, params, defaults(opts))
    |> arg_error_raiser
  end

  @doc """
  Runs an (extended) prepared query and returns the result or raises
  `Mariaex.Error` if there was an error. See `execute/4`.
  """
  @spec execute!(conn, Mariaex.Query.t, list, Keyword.t) :: Mariaex.Result.t
  def execute!(conn, query, params, opts \\ []) do
    DBConnection.execute!(conn, query, params, defaults(opts))
  end

  @doc """
  Close a prepared a query and returns `:ok` or `{:error, %Mariaex.Error{}}` if
  there was an error.

  ## Options

    * `:pool_timeout` - Time to wait in the queue for the connection
    (default: `#{@pool_timeout}`)
    * `:queue` - Whether to wait for connection in a queue (default: `true`);
    * `:timeout` - Prepare request timeout (default: `#{@timeout}`);
    * `:pool` - The pool module to use, must match that set on
    `start_link/1`, see `DBConnection`

  ## Examples

      query = Mariaex.prepare!(conn, "SELECT id FROM posts WHERE title like $1")
      Mariaex.close(conn, query)
  """
  @spec close(conn, Mariaex.Query.t, Keyword.t) :: :ok | {:error, Mariaex.Error.t}
  def close(conn, query, opts \\ []) do
    case DBConnection.close(conn, query, defaults(opts)) do
      {:ok, _} -> :ok
      other    -> arg_error_raiser(other)
    end
  end

  @doc """
  Close a prepared query and returns `:ok` or raises
  `Mariaex.Error` if there was an error. See `close/3`.
  """
  @spec close!(conn, Mariaex.Query.t, Keyword.t) :: :ok
  def close!(conn, query, opts \\ []) do
    DBConnection.close!(conn, query, defaults(opts))
  end

  @doc """
  Acquire a lock on a connection and run a series of requests inside a
  transaction. The result of the transaction fun is return inside an `:ok`
  tuple: `{:ok, result}`.

  To use the locked connection call the request with the connection
  reference passed as the single argument to the `fun`. If the
  connection disconnects all future calls using that connection
  reference will fail.

  `rollback/2` rolls back the transaction and causes the function to
  return `{:error, reason}`.

  `transaction/3` can be nested multiple times if the connection
  reference is used to start a nested transaction. The top level
  transaction function is the actual transaction.

  ## Options

    * `:pool_timeout` - Time to wait in the queue for the connection
    (default: `#{@pool_timeout}`)
    * `:queue` - Whether to wait for connection in a queue (default: `true`);
    * `:timeout` - Transaction timeout (default: `#{@timeout}`);
    * `:pool` - The pool module to use, must match that set on
    `start_link/1`, see `DBConnection;
    * `:mode` - Set to `:savepoint` to use savepoints instead of an SQL
    transaction, otherwise set to `:transaction` (default: `:transaction`);


  The `:timeout` is for the duration of the transaction and all nested
  transactions and requests. This timeout overrides timeouts set by internal
  transactions and requests. The `:pool` and `:mode` will be used for all
  requests inside the transaction function.

  ## Example

      {:ok, res} = Mariaex.transaction(pid, fn(conn) ->
        Mariaex.query!(conn, "SELECT title FROM posts", [])
      end)
  """
  @spec transaction(conn, ((DBConnection.t) -> result), Keyword.t) ::
    {:ok, result} | {:error, any} when result: var
  def transaction(conn, fun, opts \\ []) do
    DBConnection.transaction(conn, fun, defaults(opts))
  end

  @doc """
  Rollback a transaction, does not return.

  Aborts the current transaction fun. If inside multiple `transaction/3`
  functions, bubbles up to the top level.

  ## Example

      {:error, :oops} = Mariaex.transaction(pid, fn(conn) ->
        Mariaex.rollback(conn, :oops)
        IO.puts "never reaches here!"
      end)
  """
  @spec rollback(DBConnection.t, any) :: no_return()
  defdelegate rollback(conn, any), to: DBConnection

  @doc """
  Returns a stream for a query on a connection.

  Streams read chunks of at most `max_rows` rows and can only be used inside a
  transaction.

  ### Options
    * `:max_rows` - Maximum numbers of rows in a result (default to `#{@max_rows}`)

      Mariaex.transaction(pid, fn(conn) ->
        stream = Mariaex.stream(conn, "SELECT id FROM posts WHERE title like $1", ["%my%"])
        Enum.to_list(stream)
      end)
  """
  @spec stream(DBConnection.t, iodata | Mariaex.Query.t, list, Keyword.t) ::
    DBConnection.Stream.t
  def stream(conn, query, params, opts \\ [])

  def stream(conn, %Query{} = query, params, opts) do
    DBConnection.stream(conn, query, params, opts)
  end
  def stream(conn, statement, params, opts) do
    query = %Query{name: "", statement: statement}
    DBConnection.prepare_stream(conn, query, params, opts)
  end

  @doc """
  Returns a supervisor child specification for a DBConnection pool.
  """
  @spec child_spec(Keyword.t) :: Supervisor.Spec.spec
  def child_spec(opts) do
    DBConnection.child_spec(Mariaex.Protocol, opts)
  end

  ## Helpers

  defp defaults(opts) do
    Keyword.put_new(opts, :timeout, @timeout)
  end
end
