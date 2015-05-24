defmodule Mariaex.Connection.Tcp do
  def connect(host, port, socket_options, timeout) do
    sock_opts = [{:active, :once}, {:packet, :raw}, :binary] ++ socket_options
    :gen_tcp.connect(host, port, sock_opts, timeout)
  end

  def receive(_sock, {:tcp, _, blob}), do: blob

  def next(sock), do: :inet.setopts(sock, active: :once)

  def send(sock, data), do: :gen_tcp.send(sock, data)
end
