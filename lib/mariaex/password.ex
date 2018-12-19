defmodule Mariaex.Password do

  @spec save!(String.t) :: :ets.tid()
  def save!(password) do
    tid = :ets.new(:password, [:private])
    true = :ets.insert(tid, {:password, password})
    tid
  end

  @spec get(:ets.tid()) :: String.t
  def get(tid) do
    [password: password] = :ets.lookup(tid, :password)
    password
  end
end
