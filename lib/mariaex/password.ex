defmodule Mariaex.Password do
  use Agent

  @spec start_link(String.t) :: {:ok, pid()}
  def start_link(password) do
    Agent.start_link(fn -> password end)
  end

  @spec save!(String.t) :: pid()
  def save!(password) do
    {:ok, pid} = start_link(password)
    pid
  end

  @spec get(pid()) :: String.t
  def get(pid) do
    Agent.get(pid, &(&1))
  end
end
