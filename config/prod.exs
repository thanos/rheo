import Config

config :rheo,
  mongo_url: System.get_env("RHEO_MONGO_URL")
