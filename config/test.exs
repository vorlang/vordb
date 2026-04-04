import Config

config :vordb,
  node_id: :test_node,
  http_port: 4099,
  data_dir: "tmp/test_data",
  sync_interval_ms: 60_000,
  peers: []

config :logger,
  level: :warning
