defmodule Mariaex.Connection.Ssl do

  def recv(sock, bytes, timeout), do: :ssl.recv(sock, bytes, timeout)

  def recv_active(sock, timeout, buffer \\ :active_once) do
    receive do
      {:ssl, ^sock, buffer} ->
        {:ok, buffer}
      {:ssl_closed, ^sock} ->
        {:disconnect, {tag(), "async_recv", :closed, buffer}}
      {:ssl_error, ^sock, reason} ->
        {:disconnect, {tag(), "async_recv", reason, buffer}}
    after
      timeout ->
        {:ok, <<>>}
    end
  end

  def tag(), do: :ssl

  def fake_message(sock, buffer), do: {:ssl, sock, buffer}

  def receive(_sock, {:ssl, _, blob}), do: blob

  def setopts({:sslsocket, {:gen_tcp, sock, :tls_connection, _},_pid}, opts) do
    :inet.setopts(sock, opts)
  end

  def send(sock, data), do: :ssl.send(sock, data)

  def close(sock), do: :ssl.close(sock)
end
