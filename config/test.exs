import Config

config :rheo,
  mongo_url: System.get_env("RHEO_MONGO_URL", "mongodb://localhost:27017/rheo_test"),
  clock: Rheo.Clock.Frozen,
  start_on_application: false,
  default_lease_ms: 5_000,
  default_max_attempts: 3,
  default_max_demand: 5

config :logger, level: :warning
