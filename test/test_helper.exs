url = System.get_env("RHEO_MONGO_URL", "mongodb://localhost:27017/rheo_test")
Application.put_env(:rheo, :mongo_url, url)
Application.put_env(:rheo, :start_on_application, false)

{:ok, _} = Application.ensure_all_started(:mongodb_driver)

case Rheo.start_link(url: url, name: Rheo.Mongo) do
  {:ok, _} -> :ok
  {:error, {:already_started, _}} -> :ok
end

:ok = Rheo.ensure_indexes()

ExUnit.start()
