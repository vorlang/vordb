import Config

config :vordb,
  sync_interval_ms: 1_000

config :logger,
  level: :info

import_config "#{config_env()}.exs"
