defmodule VorDB.VnodeSupervisor do
  @moduledoc "Supervises N KvStore vnode agents, each registered via VnodeRegistry."

  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    node_id = Keyword.fetch!(opts, :node_id)
    num_vnodes = Keyword.get(opts, :num_vnodes, 16)
    sync_interval = Keyword.get(opts, :sync_interval_ms, 1_000)

    children =
      for vnode_index <- 0..(num_vnodes - 1) do
        name = {:via, Registry, {VorDB.VnodeRegistry, {:kv_store, vnode_index}}}

        %{
          id: {:kv_store, vnode_index},
          start:
            {Vor.Agent.KvStore, :start_link,
             [[node_id: node_id, vnode_id: vnode_index, sync_interval_ms: sync_interval, name: name]]}
        }
      end

    Supervisor.init(children, strategy: :one_for_one)
  end
end
