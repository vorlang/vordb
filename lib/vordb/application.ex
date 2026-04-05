defmodule VorDB.Application do
  @moduledoc "OTP Application — starts VorDB supervision tree."

  use Application

  @impl true
  def start(_type, _args) do
    node_id = Application.get_env(:vordb, :node_id, :node1)
    data_dir = Application.get_env(:vordb, :data_dir, "data/#{node_id}")
    http_port = Application.get_env(:vordb, :http_port, 4001)
    sync_interval = Application.get_env(:vordb, :sync_interval_ms, 1_000)
    full_sync_interval = Application.get_env(:vordb, :full_sync_interval_ms, 30_000)
    peers = Application.get_env(:vordb, :peers, [])
    num_vnodes = Application.get_env(:vordb, :num_vnodes, 16)

    children = [
      # Storage — one RocksDB instance, shared by all vnodes
      {VorDB.Storage, [data_dir: data_dir]},

      # Cluster — peer discovery
      {VorDB.Cluster, []},

      # Membership — dynamic cluster membership
      {VorDB.Membership, []},

      # DirtyTracker — vnode-aware per-peer dirty sets
      {VorDB.DirtyTracker, [peers: peers, num_vnodes: num_vnodes]},

      # VnodeRegistry — process registration for vnode lookup
      {Registry, keys: :unique, name: VorDB.VnodeRegistry},

      # VnodeSupervisor — starts N KvStore agents
      {VorDB.VnodeSupervisor, [node_id: node_id, num_vnodes: num_vnodes, sync_interval_ms: sync_interval]},

      # Gossip — delta + full-state fallback (iterates all vnodes)
      {VorDB.Gossip, [sync_interval_ms: sync_interval, full_sync_interval_ms: full_sync_interval]},

      # HTTP API
      {Plug.Cowboy, scheme: :http, plug: VorDB.HTTP.Router, options: [port: http_port]}
    ]

    opts = [strategy: :one_for_one, name: VorDB.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
