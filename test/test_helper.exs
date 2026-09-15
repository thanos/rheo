url = System.get_env("RHEO_MONGO_URL", "mongodb://localhost:27017/rheo_test")
Application.put_env(:rheo, :mongo_url, url)
Application.put_env(:rheo, :start_on_application, false)
Application.put_env(:rheo, :mongo_client, Rheo.Backend.Mongo.Client.Driver)

{:ok, _} = Application.ensure_all_started(:mongodb_driver)

case Rheo.start_link(url: url) do
  {:ok, _} -> :ok
  {:error, {:already_started, _}} -> :ok
end

:ok = Rheo.ensure_indexes()

# Opt-in: RHEO_INTEGRATION=1 mix test
# or: mix test --include integration
exclude =
  if System.get_env("RHEO_INTEGRATION") in ["1", "true"] do
    []
  else
    [:integration]
  end

ExUnit.start(exclude: exclude)
