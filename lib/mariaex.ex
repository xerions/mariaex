defmodule Mariaex do
  @moduledoc """
  Main API for Mariaex.
  """

  alias Mariaex.Connection
  alias Mariaex.Protocol
  alias Mariaex.Query

  @timeout 5000
  @keepalive false

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
  ### PUBLIC API ###

  @doc """
  Start the connection process and connect to mariadb.

  ## Options

    * `:hostname` - Server hostname (default: MDBHOST env variable, then localhost);
    * `:port` - Server port (default: 3306);
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
    * `:keepalive` - Enable keepalive (default: false), please note, it is not considered stable API;
    * `:keepalive_interval` - Keepalive interval (default: 60000);
    * `:keepalive_timeout` - Keepalive timeout (default: 5000);
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
    * `:pool` - The pool module to use, see `DBConnection`, it must be
      included with all requests if not the default (default:
      `DBConnection.Connection`);

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
    sock_type = (opts[:sock_type] || :tcp) |> Atom.to_string |> String.capitalize()
    sock_mod = Module.concat(Connection, sock_type)
    opts = opts
      |> Keyword.put_new(:username, System.get_env("MDBUSER") || System.get_env("USER"))
      |> Keyword.put_new(:password, System.get_env("MDBPASSWORD"))
      |> Keyword.put_new(:hostname, System.get_env("MDBHOST") || "localhost")
      |> Keyword.put_new(:sock_mod, sock_mod)
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

  ## Examples

      Mariaex.Connection.query(pid, "CREATE TABLE posts (id serial, title text)")

      Mariaex.Connection.query(pid, "INSERT INTO posts (title) VALUES ('my title')")

      Mariaex.Connection.query(pid, "SELECT title FROM posts", [])

      Mariaex.Connection.query(pid, "SELECT id FROM posts WHERE title like ?", ["%my%"])

      Mariaex.Connection.query(pid, "SELECT ? || ?", ["4", "2"],
                                param_types: ["text", "text"], result_types: ["text"])

  """
  @spec query(conn, iodata, list, Keyword.t) :: {:ok, Mariaex.Result.t} | {:error, Mariaex.Error.t}
  def query(conn, statement, params \\ [], opts \\ []) do
    DBConnection.query(conn, %Query{statement: statement}, params, defaults(opts))
    |> arg_error_raiser
   #  GenServer.call(pid, message, timeout) do
   #   {:ok, %Mariaex.Result{} = res} ->
   #     case Keyword.get(opts, :decode, :auto) do
   #       :auto   -> {:ok, decode(res)}
   #       :manual -> {:ok, res}
   #     end
   #   error ->
   #     error
  end

  @doc """
  Runs an (extended) query and returns the result or raises `Mariaex.Error` if
  there was an error. See `query/3`.
  """
  def query!(conn, statement, params \\ [], opts \\ []) do
    DBConnection.query!(conn, %Query{statement: statement}, params, defaults(opts))
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

  ## Helpers

  defp defaults(opts) do
    Keyword.put_new(opts, :timeout, @timeout)
  end
end
