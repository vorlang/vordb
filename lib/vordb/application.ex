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

    children = [
      # Storage must start first
      {VorDB.Storage, [data_dir: data_dir]},

      # Cluster connects to peers
      {VorDB.Cluster, []},

      # DirtyTracker — after Cluster (needs peer list), before agent
      {VorDB.DirtyTracker, [peers: peers]},

      # Vor-compiled KvStore agent
      %{
        id: :kv_store,
        start: {GenServer, :start_link, [Vor.Agent.KvStore, [node_id: node_id], [name: Vor.Agent.KvStore]]}
      },

      # Gossip — delta + full-state fallback
      {VorDB.Gossip, [sync_interval_ms: sync_interval, full_sync_interval_ms: full_sync_interval]},

      # HTTP API
      {Plug.Cowboy, scheme: :http, plug: VorDB.HTTP.Router, options: [port: http_port]}
    ]

    opts = [strategy: :one_for_one, name: VorDB.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
