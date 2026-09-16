url = System.get_env("RHEO_MONGO_URL", "mongodb://localhost:27017/rheo_test")
Application.put_env(:rheo, :mongo_url, url)
Application.put_env(:rheo, :start_on_application, false)
Application.put_env(:rheo, :mongo_client, Rheo.Backend.Mongo.Client.Driver)

{:ok, _} = Application.ensure_all_started(:mongodb_driver)
{:ok, _} = Application.ensure_all_started(:ecto_sql)

case Rheo.start_link(url: url) do
  {:ok, _} -> :ok
  {:error, {:already_started, _}} -> :ok
end

:ok = Rheo.ensure_indexes()

# Ecto SQL backend: SQLite always runs on a throwaway file database. PostgreSQL
# is opt-in through RHEO_POSTGRES_URL (or DATABASE_URL).
{sqlite_repo, sqlite_database} = Rheo.Test.Repos.start_sqlite!()
:ok = Rheo.Backend.Ecto.Migrations.up(sqlite_repo)
System.at_exit(fn _status -> Rheo.Test.Repos.cleanup_sqlite(sqlite_database) end)

postgres_exclude =
  if Rheo.Test.Repos.postgres?() do
    postgres_repo = Rheo.Test.Repos.start_postgres!()
    :ok = Rheo.Backend.Ecto.Migrations.down(postgres_repo)
    :ok = Rheo.Backend.Ecto.Migrations.up(postgres_repo)
    []
  else
    [:ecto_postgres]
  end

# Opt-in: RHEO_INTEGRATION=1 mix test
# or: mix test --include integration
integration_exclude =
  if System.get_env("RHEO_INTEGRATION") in ["1", "true"] do
    []
  else
    [:integration]
  end

ExUnit.start(exclude: integration_exclude ++ postgres_exclude)
