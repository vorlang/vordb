defmodule VorDB.VnodeRouter do
  @moduledoc "Routes key operations to the correct vnode via consistent hashing."

  def vnode_for_key(key) do
    num_vnodes = Application.get_env(:vordb, :num_vnodes, 16)
    :erlang.phash2(key, num_vnodes)
  end

  def call(key, message) do
    vnode_index = vnode_for_key(key)
    call_vnode(vnode_index, message)
  end

  def cast(key, message) do
    vnode_index = vnode_for_key(key)
    cast_vnode(vnode_index, message)
  end

  def call_vnode(vnode_index, message) do
    case Registry.lookup(VorDB.VnodeRegistry, {:kv_store, vnode_index}) do
      [{pid, _}] -> GenServer.call(pid, message)
      [] -> {:error, :vnode_not_found}
    end
  end

  def cast_vnode(vnode_index, message) do
    case Registry.lookup(VorDB.VnodeRegistry, {:kv_store, vnode_index}) do
      [{pid, _}] -> GenServer.cast(pid, message)
      [] -> :ok
    end
  end

  def all_vnodes do
    num_vnodes = Application.get_env(:vordb, :num_vnodes, 16)
    Enum.to_list(0..(num_vnodes - 1))
  end
end
