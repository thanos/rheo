import Config

config :rheo,
  mongo_url: System.get_env("RHEO_MONGO_URL", "mongodb://localhost:27017/rheo_dev")
