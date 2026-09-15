import Config

config :rheo,
  topology: Rheo.Mongo,
  clock: Rheo.Clock.System,
  default_lease_ms: 30_000,
  default_max_attempts: 5,
  default_max_demand: 10

import_config "#{config_env()}.exs"
