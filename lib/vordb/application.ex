defmodule VorDB.Application do
  @moduledoc "OTP Application — starts VorDB supervision tree."

  use Application

  @impl true
  def start(_type, _args) do
    node_id = Application.get_env(:vordb, :node_id, :node1)
    data_dir = Application.get_env(:vordb, :data_dir, "data/#{node_id}")
    http_port = Application.get_env(:vordb, :http_port, 4001)
    sync_interval = Application.get_env(:vordb, :sync_interval_ms, 1_000)

    children = [
      # Storage must start first — Vor.Agent.KvStore loads state from it on init
      {VorDB.Storage, [data_dir: data_dir]},

      # Cluster connects to peers
      {VorDB.Cluster, []},

      # Vor-compiled Vor.Agent.KvStore agent — registered as Vor.Agent.KvStore for direct access
      %{
        id: :kv_store,
        start: {GenServer, :start_link, [Vor.Agent.KvStore, [node_id: node_id], [name: Vor.Agent.KvStore]]}
      },

      # Gossip broadcasts state to peers on interval
      {VorDB.Gossip, [sync_interval_ms: sync_interval]},

      # HTTP API
      {Plug.Cowboy, scheme: :http, plug: VorDB.HTTP.Router, options: [port: http_port]}
    ]

    opts = [strategy: :one_for_one, name: VorDB.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
