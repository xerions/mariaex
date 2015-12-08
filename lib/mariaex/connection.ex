defmodule Mariaex.Connection do
  @moduledoc """
  Main API for Mariaex. This module handles the connection to .
  """

  use Connection
  alias Mariaex.Protocol
  alias Mariaex.Messages

  @timeout 5000
  @keepalive false

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
      |> Keyword.put_new(:username, System.get_env("MDBUSER") || System.get_env("USER"))
      |> Keyword.put_new(:password, System.get_env("MDBPASSWORD"))
      |> Keyword.put_new(:hostname, System.get_env("MDBHOST") || "localhost")
      |> Keyword.put_new(:sock_mod, sock_mod)
    Connection.start_link(__MODULE__, opts)
  end

  @doc """
  Stop the process and disconnect.

  ## Options

    * `:timeout` - Call timeout (default: `#{@timeout}`)
  """
  @spec stop(pid, Keyword.t) :: :ok
  def stop(pid, opts \\ []) do
    Connection.call(pid, :stop, opts[:timeout] || @timeout)
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

  ## Examples

      Mariaex.Connection.query(pid, "CREATE TABLE posts (id serial, title text)")

      Mariaex.Connection.query(pid, "INSERT INTO posts (title) VALUES ('my title')")

      Mariaex.Connection.query(pid, "SELECT title FROM posts", [])

      Mariaex.Connection.query(pid, "SELECT id FROM posts WHERE title like ?", ["%my%"])

      Mariaex.Connection.query(pid, "SELECT ? || ?", ["4", "2"],
                                param_types: ["text", "text"], result_types: ["text"])

  """
  @spec query(pid, iodata, list, Keyword.t) :: {:ok, Mariaex.Result.t} | {:error, Mariaex.Error.t}
  def query(pid, statement, params \\ [], opts \\ []) do
    message = {:query, statement, params, opts}
    timeout = opts[:timeout] || @timeout
    case GenServer.call(pid, message, timeout) do
      {:ok, %Mariaex.Result{} = res} ->
        case Keyword.get(opts, :decode, :auto) do
          :auto   -> {:ok, decode(res)}
          :manual -> {:ok, res}
        end
      error ->
        error
    end
  end

  @doc """
  Runs an (extended) query and returns the result or raises `Mariaex.Error` if
  there was an error. See `query/3`.
  """
  def query!(pid, statement, params \\ [], opts \\ []) do
    query(pid, statement, params, opts) |> raiser
  end

  @doc """
  Decodes a result set.

  It is a no-op if the result was already decoded.
  """
  def decode(_, mapper \\ fn x -> x end)
  def decode(%Mariaex.Result{decoder: :done} = result, _), do: result
  def decode(%Mariaex.Result{rows: rows, decoder: types} = result, mapper) do
    types = Enum.reverse(types)
    rows = rows |> Enum.reduce([], &([(Mariaex.Messages.decode_bin_rows(&1, types) |> mapper.()) | &2]))
    %{result | columns: (for {type, _} <- types, do: type),
               rows: rows,
               num_rows: length(rows),
               decoder: :done}
  end

  ### CONNECTION CALLBACKS ###

  @doc false
  def init(opts) do
    if opts[:sync_connect] do
      sync_connect(opts)
    else
      {:connect, :init, opts}
    end
  end

  @doc false
  def connect(_, opts) do
    case Protocol.init(opts) do
      {:ok, _} = ok   -> ok
      {:stop, reason} -> {:stop, reason, opts}
    end
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
  def handle_call(:stop, _from, s) do
    {:stop, :normal, :ok, s}
  end

  def handle_call({:connect, opts}, from, %{queue: queue, sock_mod: sock_mod} = s) do
    host      = Keyword.fetch!(opts, :hostname)
    host      = if is_binary(host), do: String.to_char_list(host), else: host
    port      = opts[:port] || 3306
    timeout   = opts[:timeout] || @timeout

    case sock_mod.connect(host, port, opts[:socket_options] || [], timeout) do
      {:ok, sock} ->
        queue = :queue.in({{:connect, opts}, from}, queue)
        s = %{s | opts: opts, state: :handshake, sock: {sock_mod, sock}, queue: queue}
        {:noreply, s}
      {:error, reason} ->
        {:stop, :normal, {:error, %Mariaex.Error{message: "tcp connect: #{reason}"}}, s}
    end
  end

  def handle_call(command, from, s) do
    s = update_in s.queue, &:queue.in({command, from}, &1)
    check_next(s)
  end

  def handle_cast({{:query, _, _, _} = command, from}, s) do
    handle_call(command, from, s)
  end

  def handle_cast({:keepalive, keepalive_interval, keepalive_timeout}, %{} = s) do
    Process.send_after(self, :ping, keepalive_interval)
    {:noreply, %{s | keepalive: {keepalive_interval, keepalive_timeout}}}
  end

  def handle_info({:tcp_closed, _}, s) do
    error(%Mariaex.Error{message: "connection closed"}, s)
  end

  def handle_info(:ping, s = %{keepalive: {keepalive_interval, keepalive_timeout}}) do
    last_answer = :os.timestamp |> :timer.now_diff(s.last_answer) |> div(1000)
    if last_answer >= keepalive_interval do
      s = update_in s.queue, &:queue.in(:ping, &1)
      check_next(%{s | keepalive_send: Process.send_after(self, :ping_timeout, keepalive_timeout)})
    else
      Process.send_after(self, :ping, keepalive_interval - last_answer)
      {:noreply, s}
    end
  end

  def handle_info(:ping_timeout, s) do
    error(%Mariaex.Error{message: "keepalive timeout"}, s)
  end

  def handle_info(sock_message, %{sock: {sock_mod, sock}} = s) do
    s = sock_mod.receive(sock, sock_message) |> process(s)
    sock_mod.next(sock)
    check_next(s)
  end

  ### PRIVATE FUNCTIONS ###

  defp sync_connect(opts) do
    case connect(:init, opts) do
      {:ok, _} = ok      -> ok
      {:stop, reason, _} -> {:stop, reason}
    end
  end

  defp check_next(s) do
    if s.state == :running do
      {:noreply, next(s)}
    else
      {:noreply, s}
    end
  end

  def next(%{queue: queue} = s) do
    case :queue.out(queue) do
      {{:value, {command, _from}}, _queue} ->
        command(command, s)
      {{:value, :ping}, _queue} ->
        Protocol.ping(s)
      {:empty, _queue} ->
        s
    end
  end

  defp command({:query, statement, params, _opts}, s) do
    Protocol.send_query(statement, params, s)
  end

  defp process(blob, %{state: state, substate: substate, tail: tail} = s) do
    case Messages.decode(tail <> blob, substate || state) do
      {nil, tail} ->
        %{s | tail: tail}
      {packet, tail} ->
        new_s = Protocol.dispatch(packet, s)
        process(tail, %{new_s | tail: ""})
    end
  end

  def pong(%{queue: queue, keepalive: {keepalive_interval, _}, keepalive_send: timer} = state) do
    queue = :queue.drop(queue)
    :erlang.cancel_timer(timer)
    Process.send_after(self, :ping, keepalive_interval)
    %{state | queue: queue, last_answer: :os.timestamp, keepalive_send: nil}
  end

  def reply(reply, %{queue: queue} = state) do
    case :queue.out(queue) do
      {{:value, queue_entry}, queue} ->
        reply(reply, queue_entry)
        {true, %{state | queue: queue, last_answer: :os.timestamp}}
      {:empty, _queue} ->
        {false, state}
    end
  end

  def reply(reply, {_command, from}) do
    Connection.reply(from, reply)
  end

  def error(error, state) do
    reply(error, state)
    {:stop, error, state}
  end
end
