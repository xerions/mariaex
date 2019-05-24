defmodule Mariaex.Connection.Ssl do
  def recv(sock, bytes, timeout), do: :ssl.recv(sock, bytes, timeout)

  def tag(), do: :ssl

  def send(sock, data), do: :ssl.send(sock, data)

  def close(sock), do: :ssl.close(sock)
end
