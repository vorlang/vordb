defmodule VorDB do
  @moduledoc """
  VorDB — A CRDT-based distributed key-value store.

  Public API for programmatic access. Routes to correct vnode transparently.
  """

  def put(key, value) do
    VorDB.VnodeRouter.call(key, {:put, %{key: key, value: value}})
  end

  def get(key) do
    VorDB.VnodeRouter.call(key, {:get, %{key: key}})
  end

  def delete(key) do
    VorDB.VnodeRouter.call(key, {:delete, %{key: key}})
  end

  def set_add(key, element) do
    VorDB.VnodeRouter.call(key, {:set_add, %{key: key, element: element}})
  end

  def set_remove(key, element) do
    VorDB.VnodeRouter.call(key, {:set_remove, %{key: key, element: element}})
  end

  def set_members(key) do
    VorDB.VnodeRouter.call(key, {:set_members, %{key: key}})
  end

  def counter_increment(key, amount \\ 1) do
    VorDB.VnodeRouter.call(key, {:counter_increment, %{key: key, amount: amount}})
  end

  def counter_decrement(key, amount \\ 1) do
    VorDB.VnodeRouter.call(key, {:counter_decrement, %{key: key, amount: amount}})
  end

  def counter_value(key) do
    VorDB.VnodeRouter.call(key, {:counter_value, %{key: key}})
  end
end
