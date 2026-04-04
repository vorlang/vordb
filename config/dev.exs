import Config

config :vordb,
  node_id: :node1,
  http_port: 4001,
  data_dir: "data/node1",
  peers: [:node2@localhost, :node3@localhost]
