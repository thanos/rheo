defmodule Rheo.Test.SqliteRepo do
  @moduledoc false
  use Ecto.Repo, otp_app: :rheo, adapter: Ecto.Adapters.SQLite3
end

defmodule Rheo.Test.PostgresRepo do
  @moduledoc false
  use Ecto.Repo, otp_app: :rheo, adapter: Ecto.Adapters.Postgres
end

defmodule Rheo.Test.MigrationRepo do
  @moduledoc false
  # Scratch SQLite repo so migration up/down tests cannot drop tables the rest
  # of the suite is using.
  use Ecto.Repo, otp_app: :rheo, adapter: Ecto.Adapters.SQLite3
end

defmodule Rheo.Test.Repos do
  @moduledoc false

  # Helpers for booting the Ecto backend test repos. SQLite runs everywhere on a
  # throwaway file database; PostgreSQL is opt-in via RHEO_POSTGRES_URL.

  @sqlite_repo Rheo.Test.SqliteRepo
  @postgres_repo Rheo.Test.PostgresRepo

  @spec sqlite_database() :: String.t()
  def sqlite_database do
    Path.join(System.tmp_dir!(), "rheo_test_#{System.unique_integer([:positive])}.sqlite3")
  end

  @spec postgres_url() :: String.t() | nil
  def postgres_url do
    System.get_env("RHEO_POSTGRES_URL") || System.get_env("DATABASE_URL")
  end

  @spec postgres?() :: boolean()
  def postgres?, do: is_binary(postgres_url())

  @doc "Connection options for drivers that do not parse Ecto URLs (e.g. notifications)."
  @spec postgres_opts() :: keyword()
  def postgres_opts do
    uri = URI.parse(postgres_url())
    {username, password} = userinfo(uri.userinfo)

    [
      hostname: uri.host || "localhost",
      port: uri.port || 5432,
      database: String.trim_leading(uri.path || "", "/"),
      username: username,
      password: password
    ]
  end

  defp userinfo(nil), do: {"postgres", "postgres"}

  defp userinfo(userinfo) do
    case String.split(userinfo, ":", parts: 2) do
      [username, password] -> {username, password}
      [username] -> {username, ""}
    end
  end

  @doc "Starts a SQLite repo on a fresh temp database and returns the repo and path."
  @spec start_sqlite!(module(), keyword()) :: {module(), String.t()}
  def start_sqlite!(repo \\ @sqlite_repo, opts \\ []) do
    database = sqlite_database()

    config =
      Keyword.merge(
        [
          database: database,
          pool_size: 1,
          journal_mode: :wal,
          busy_timeout: 5_000,
          # Take the write lock up front so read-modify-write of group cursors
          # cannot fail with SQLITE_BUSY mid-transaction.
          transaction_mode: :immediate,
          log: false
        ],
        opts
      )

    {:ok, _pid} = repo.start_link(config)
    {repo, database}
  end

  @doc "Starts the PostgreSQL repo against RHEO_POSTGRES_URL."
  @spec start_postgres!(keyword()) :: module()
  def start_postgres!(opts \\ []) do
    config = Keyword.merge([url: postgres_url(), pool_size: 2, log: false], opts)
    {:ok, _pid} = @postgres_repo.start_link(config)
    @postgres_repo
  end

  @doc "Removes SQLite database files left behind by a test run."
  @spec cleanup_sqlite(String.t()) :: :ok
  def cleanup_sqlite(database) do
    Enum.each([database, database <> "-wal", database <> "-shm"], &File.rm/1)
  end
end
