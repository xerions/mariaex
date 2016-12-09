defmodule Mariaex.ProtocolHelper do
  @doc"""
  Define a packet handler, `recv_func/2`.
  If a packet is received successfully, it will pass `(packet, request, state)` from MariaDB to `handle_func/3`.
  Otherwise, it will disconnect from the database.
  """
  defmacro def_handle(recv_func, handle_func) do
    quote do
      defp unquote(recv_func)(state, request) do
        case msg_recv(state) do
          {:ok, packet, state} ->
            unquote(handle_func)(packet, request, state)
          {:error, reason} ->
            {sock_mod, _} = state.sock
            Mariaex.Protocol.do_disconnect(state, {sock_mod.tag, "recv", reason, ""})
        end
      end
    end
  end
end
