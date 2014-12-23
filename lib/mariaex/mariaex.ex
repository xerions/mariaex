defmodule Mariaex.Connection do
  @moduledoc """
  Main API for Mariaex. This module handles the connection to .
  """

  use GenServer
  alias Mariaex.Protocol
  alias Mariaex.Messages

  @timeout 5000 #:infinity

  defmacrop raiser(result) do
    quote do
      case unquote(result) do
        {:error, error} ->
          raise error
        result ->
          result
      end
    end
  end

  ### PUBLIC API ###

  @doc """
  Start the connection process and connect to mariadb.

  ## Options

    * `:hostname` - Server hostname (default: MDBHOST env variable, then localhost);
    * `:port` - Server port (default: 3306);
    * `:sock_type` - Socket type (default: :tcp);
    * `:database` - Database (required);
    * `:username` - Username (default: MDBUSER env variable, then USER env var);
    * `:password` - User password (default MDBPASSWORD);
    * `:encoder` - Custom encoder function;
    * `:decoder` - Custom decoder function;
    * `:formatter` - Function deciding the format for a type;
    * `:parameters` - Keyword list of connection parameters;
    * `:connect_timeout` - Connect timeout in milliseconds (default: 5000);

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
    sock_mod = ("Elixir.Mariaex.Connection." <> sock_type) |> String.to_atom
    opts = opts
      |> Dict.put_new(:user, System.get_env("MDBUSER") || System.get_env("USER"))
      |> Dict.put_new(:password, System.get_env("MDBPASSWORD"))
      |> Dict.put_new(:hostname, System.get_env("MDBHOST") || "localhost")
    case GenServer.start_link(__MODULE__, [sock_mod]) do
      {:ok, pid} ->
        timeout = opts[:connect_timeout] || @timeout
        case GenServer.call(pid, {:connect, opts}, timeout) do
          :ok -> {:ok, pid}
          err -> {:error, err}
        end
      err -> err
    end
  end

  @doc """
  Stop the process and disconnect.

  ## Options

    * `:timeout` - Call timeout (default: `#{@timeout}`)
  """
  @spec stop(pid, Keyword.t) :: :ok
  def stop(pid, opts \\ []) do
    GenServer.call(pid, :stop, opts[:timeout] || @timeout)
  end

  @doc """
  Runs an (extended) query and returns the result as `{:ok, %Mariaex.Result{}}`
  or `{:error, %Mariaex.Error{}}` if there was an error. Parameters can be
  set in the query as `$1` embedded in the query string. Parameters are given as
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

  ## Examples

      Mariaex.Connection.query(pid, "CREATE TABLE posts (id serial, title text)")

      Mariaex.Connection.query(pid, "INSERT INTO posts (title) VALUES ('my title')", [])

      Mariaex.Connection.query(pid, "SELECT title FROM posts", [])

      Mariaex.Connection.query(pid, "SELECT id FROM posts WHERE title like $1", ["%my%"])

      Mariaex.Connection.query(pid, "SELECT $1 || $2", ["4", "2"],
                                param_types: ["text", "text"], result_types: ["text"])

  """
  @spec query(pid, iodata, list, Keyword.t) :: {:ok, Mariaex.Result.t} | {:error, Mariaex.Error.t}
  def query(pid, statement, params \\ [], opts \\ []) do
    message = {:query, statement, params, opts}
    timeout = opts[:timeout] || @timeout
    GenServer.call(pid, message, timeout)
  end

  @doc """
  Runs an (extended) query and returns the result or raises `Postgrex.Error` if
  there was an error. See `query/3`.
  """

  def query!(pid, statement, params \\ [], opts \\ []) do
    query(pid, statement, params, opts) |> raiser
  end


  @doc """
  Starts a transaction. Returns `:ok` or `{:error, %Mariaex.Error{}}` if an
  error occurred. Transactions can be nested with the help of savepoints. A
  transaction won't end until a `rollback/1` or `commit/1` have been issued for
  every `begin/1`.

  ## Options

    * `:timeout` - Call timeout (default: `#{@timeout}`)

  ## Examples

      # Transaction begun
      Mariaex.Connection.begin(pid)
      Mariaex.Connection.query(pid, "INSERT INTO comments (text) VALUES ('first')")

      # Nested subtransaction begun
      Mariaex.Connection.begin(pid)
      Mariaex.Connection.query(pid, "INSERT INTO comments (text) VALUES ('second')")

      # Subtransaction rolled back
      Mariaex.Connection.rollback(pid)

      # Only the first comment will be commited because the second was rolled back
      Mariaex.Connection.commit(pid)
  """
  @spec begin(pid, Keyword.t) :: :ok | {:error, Mariaex.Error.t}
  def begin(pid, opts \\ []) do
    timeout = opts[:timeout] || @timeout
    GenServer.call(pid, {:begin, opts}, timeout)
  end

  @doc """
  Starts a transaction. Returns `:ok` if it was successful or raises
  `Mariaex.Error` if an error occurred. See `begin/1`.
  """
  @spec begin!(pid, Keyword.t) :: :ok
  def begin!(pid, opts \\ []) do
    begin(pid, opts) |> raiser
  end

  @doc """
  Rolls back a transaction. Returns `:ok` or `{:error, %Mariaex.Error{}}` if
  an error occurred. See `begin/1` for more information.

  ## Options

    * `:timeout` - Call timeout (default: `#{@timeout}`)
  """
  @spec rollback(pid, Keyword.t) :: :ok | {:error, Mariaex.Error.t}
  def rollback(pid, opts \\ []) do
    timeout = opts[:timeout] || @timeout
    GenServer.call(pid, {:rollback, opts}, timeout)
  end

  @doc """
  Rolls back a transaction. Returns `:ok` if it was successful or raises
  `Mariaex.Error` if an error occurred. See `rollback/1`.
  """
  @spec rollback!(pid, Keyword.t) :: :ok
  def rollback!(pid, opts \\ []) do
    rollback!(pid, opts) |> raiser
  end

  @doc """
  Commits a transaction. Returns `:ok` or `{:error, %Mariaex.Error{}}` if an
  error occurred. See `begin/1` for more information.

  ## Options

    * `:timeout` - Call timeout (default: `#{@timeout}`)
  """
  @spec commit(pid, Keyword.t) :: :ok | {:error, Mariaex.Error.t}
  def commit(pid, opts \\ []) do
    timeout = opts[:timeout] || @timeout
    GenServer.call(pid, {:commit, opts}, timeout)
  end

  @doc """
  Commits a transaction. Returns `:ok` if it was successful or raises
  `Mariaex.Error` if an error occurred. See `commit/1`.
  """
  @spec commit!(pid, Keyword.t) :: :ok
  def commit!(pid, opts \\ []) do
    commit!(pid, opts) |> raiser
  end

  @doc """
  Helper for creating reliable transactions. If an error is raised in the given
  function the transaction is rolled back, otherwise it is commited. A
  transaction can be cancelled with `throw :mariaex_rollback`. If there is a
  connection error `Mariaex.Error` will be raised. Do not use this function in
  conjunction with `begin/1`, `commit/1` and `rollback/1`.

  ## Options

    * `:timeout` - Call timeout (default: `#{@timeout}`). Note that it is not
      the maximum timeout of the entire call but rather the timeout of the
      `commit/2` and `rollback/2` calls that this function makes.
  """
  @spec in_transaction(pid, Keyword.t, (() -> term)) :: term
  def in_transaction(pid, opts \\ [], fun) do
    case begin(pid) do
      :ok ->
        try do
          value = fun.()
          case commit(pid, opts) do
            :ok -> value
            err -> raise err
          end
        catch
          :throw, :mariaex_rollback ->
            case rollback(pid, opts) do
              :ok -> nil
              err -> raise err
            end
          type, term ->
            _ = rollback(pid, opts)
            :erlang.raise(type, term, System.stacktrace)
        end
      err -> raise err
    end
  end

  ### GEN_SERVER CALLBACKS ###

  @doc false
  def init([sock_mod]) do
    {:ok, %{sock: nil, tail: "", state: :ready, parameters: %{}, backend_key: nil,
            sock_mod: sock_mod, seqnum: 0, rows: [], statement: nil,
            types: nil,
            transactions: 0, queue: :queue.new, opts: nil}}
  end

  @doc false
  def format_status(opt, [_pdict, s]) do
    s = %{s | types: :types_removed}
    if opt == :normal do
      [data: [{'State', s}]]
    else
      s
    end
  end

  @doc false
  def handle_call(:stop, from, s) do
    reply(:ok, from)
    {:stop, :normal, s}
  end

  def handle_call({:connect, opts}, from, %{queue: queue, sock_mod: sock_mod} = s) do
    sock_type = opts[:sock_type] || :tcp
    host      = opts[:hostname] || System.get_env("MDBHOST")
    host      = if is_binary(host), do: String.to_char_list(host), else: host
    port      = opts[:port] || 3306
    timeout   = opts[:connect_timeout] || @timeout

    case sock_mod.connect(host, port, timeout) do
      {:ok, sock} ->
        queue = :queue.in({{:connect, opts}, from, nil}, queue)
        s = %{s | opts: opts, state: :handshake, sock: {sock_mod, sock}, queue: queue}
        {:noreply, s}
      {:error, reason} ->
        {:stop, :normal, %Mariaex.Error{message: "tcp connect: #{reason}"}, s}
    end
  end

  def handle_call(command, from, %{state: state, queue: queue} = s) do
    # Assume last element in tuple is the options
    timeout = elem(command, tuple_size(command)-1)[:timeout] || @timeout

    unless timeout == :infinity do
      timer_ref = :erlang.start_timer(timeout, self(), :command)
    end
    queue = :queue.in({command, from, timer_ref}, queue)
    s = %{s | queue: queue}

    if state == :running do
      {:noreply, next(s)}
    else
      {:noreply, s}
    end
  end

  def handle_info(sock_message, %{sock: {sock_mod, sock}, tail: tail} = s) do
    new_s = sock_mod.receive(sock, sock_message) |> process(s)
    sock_mod.next(sock)
    {:noreply, new_s}
  end

  def next(%{queue: queue} = s) do
    case :queue.out(queue) do
      {{:value, {command, _from, _timer}}, _queue} ->
        command(command, s)
      {:empty, _queue} ->
        s
    end
  end

  defp command({:query, statement, _params, opts}, s) do
    Protocol.send_query(statement, s)
  end

  defp command({:begin, _opts}, %{transactions: trans} = s) do
    if trans == 0 do
      s = %{s | transactions: 1}
      new_query("BEGIN", s)
    else
      s = %{s | transactions: trans + 1}
      new_query("SAVEPOINT mariaex_#{trans}", s)
    end
  end

  defp command({:rollback, _opts}, %{queue: queue, transactions: trans} = s) do
    cond do
      trans == 0 ->
        reply(:ok, s)
        queue = :queue.drop(queue)
        {:ok, %{s | queue: queue}}
      trans == 1 ->
        s = %{s | transactions: 0}
        new_query("ROLLBACK", s)
      true ->
        trans = trans - 1
        s = %{s | transactions: trans}
        new_query("ROLLBACK TO SAVEPOINT mariaex_#{trans}", s)
    end
  end

  defp command({:commit, _opts}, %{queue: queue, transactions: trans} = s) do
    case trans do
      0 ->
        reply(:ok, s)
        queue = :queue.drop(queue)
        {:ok, %{s | queue: queue}}
      1 ->
        s = %{s | transactions: 0}
        new_query("COMMIT", s)
      _ ->
        reply(:ok, s)
        queue = :queue.drop(queue)
        {:ok, %{s | queue: queue, transactions: trans - 1}}
    end
  end

  @doc false
  def new_query(statement, %{queue: queue} = s) do
    command = {:query, statement, [], []}
    {{:value, {_command, from, timer}}, queue} = :queue.out(queue)
    queue = :queue.in_r({command, from, timer}, queue)
    command(command, %{s | queue: queue})
  end

  defp process(blob, %{state: state, tail: tail} = s) do
    case Messages.decode(tail <> blob, state) do
      {nil, tail} ->
        %{s | tail: tail}
      {packet, tail} ->
        new_s = Protocol.dispatch(packet, s)
        process(tail, new_s)
    end
  end

  def reply(reply, %{queue: queue} = state) do
    case :queue.out(queue) do
      {{:value, {_command, from, timer}}, queue} ->
        unless timer == nil, do: :erlang.cancel_timer(timer)
        GenServer.reply(from, reply)
        {true, %{state | queue: queue}}
      {:empty, _queue} ->
        {false, state}
    end
  end

  def reply(reply, {_, _} = from) do
    GenServer.reply(from, reply)
    true
  end

end
