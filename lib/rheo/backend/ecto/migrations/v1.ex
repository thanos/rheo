defmodule Rheo.Backend.Ecto.Migrations.V1 do
  @moduledoc """
  `Ecto.Migration` wrapper around `Rheo.Backend.Ecto.Migrations`.

  Lets a host app manage the Rheo schema with `mix ecto.migrate` instead of
  relying on `Rheo.ensure_indexes/1`:

      defmodule MyApp.Repo.Migrations.CreateRheoTables do
        use Ecto.Migration

        defdelegate up(), to: Rheo.Backend.Ecto.Migrations.V1
        defdelegate down(), to: Rheo.Backend.Ecto.Migrations.V1
      end

  Or run it directly:

      Ecto.Migrator.up(MyApp.Repo, 20_260_101_000_000, Rheo.Backend.Ecto.Migrations.V1)
  """

  use Ecto.Migration

  alias Rheo.Backend.Ecto.Migrations

  @doc "Creates the Rheo tables and indexes (idempotent)."
  @spec up() :: :ok
  def up, do: :ok = Migrations.up(repo(), prefix: prefix())

  @doc "Drops the Rheo tables, including the immutable event log."
  @spec down() :: :ok
  def down, do: :ok = Migrations.down(repo(), prefix: prefix())
end
