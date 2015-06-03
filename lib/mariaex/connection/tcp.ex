defmodule Mariaex.Connection.Tcp do
  def connect(host, port, socket_options, timeout) do
    sock_opts = [{:packet, :raw}, :binary] ++ socket_options
    case :gen_tcp.connect(host, port, sock_opts, timeout) do
      {:ok, sock} = ok ->
        # A suitable :buffer is only set if :recbuf is included in
        # :socket_options.
        {:ok, [sndbuf: sndbuf, recbuf: recbuf, buffer: buffer]} =
          :inet.getopts(sock, [:sndbuf, :recbuf, :buffer])
        buffer = buffer
          |> max(sndbuf)
          |> max(recbuf)
        :ok = :inet.setopts(sock, [buffer: buffer, active: :once])
        ok
      {:error, _} = error ->
        error
    end
  end

  def receive(_sock, {:tcp, _, blob}), do: blob

  def next(sock), do: :inet.setopts(sock, active: :once)

  def send(sock, data), do: :gen_tcp.send(sock, data)
end
