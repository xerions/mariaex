defmodule Mariaex.ProtocolHelper do
  defmacro def_handle(recv_func, handle_func) do
    quote do
      defp unquote(recv_func)(state, request) do
        case msg_recv(state) do
          {:ok, packet} ->
            unquote(handle_func)(packet, request, state)
          {:error, reason} ->
            {sock_mod, _} = state.sock
            Mariaex.Protocol.do_disconnect(state, {sock_mod.tag, "recv", reason, ""})
        end
      end
    end
  end
end
