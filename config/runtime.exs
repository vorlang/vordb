import Config

if config_env() != :test do
  config :vordb,
    node_id: System.get_env("VORDB_NODE_ID", "node1") |> String.to_atom(),
    http_port: System.get_env("VORDB_HTTP_PORT", "4001") |> String.to_integer(),
    data_dir: System.get_env("VORDB_DATA_DIR", "data/node1"),
    peers:
      System.get_env("VORDB_PEERS", "")
      |> String.split(",", trim: true)
      |> Enum.map(&String.to_atom/1)
end
