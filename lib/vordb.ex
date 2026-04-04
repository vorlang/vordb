defmodule VorDB do
  @moduledoc """
  VorDB — A CRDT-based distributed key-value store.

  Public API for programmatic access. For HTTP access, use the REST API.
  Calls the Vor-compiled Vor.Agent.KvStore gen_server directly.
  """

  def put(key, value) do
    GenServer.call(Vor.Agent.KvStore, {:put, %{key: key, value: value}})
  end

  def get(key) do
    GenServer.call(Vor.Agent.KvStore, {:get, %{key: key}})
  end

  def delete(key) do
    GenServer.call(Vor.Agent.KvStore, {:delete, %{key: key}})
  end
end
