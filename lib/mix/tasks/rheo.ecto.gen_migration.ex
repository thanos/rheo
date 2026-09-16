defmodule Mix.Tasks.Rheo.Ecto.GenMigration do
  use Mix.Task

  @shortdoc "Generates a host migration that creates the Rheo SQL schema"

  @moduledoc """
  Generates an `Ecto.Migration` in the host app that creates Rheo's tables.

  Use this when you would rather manage the Rheo schema with `mix ecto.migrate`
  than let `Rheo.ensure_indexes/1` create tables on demand.

      mix rheo.ecto.gen_migration
      mix rheo.ecto.gen_migration --repo MyApp.Repo --name add_rheo_tables

  ## Options

    * `--repo` — repo module used to name the migration and locate
      `priv/<repo>/migrations` (default: the first entry in
      `config :my_app, ecto_repos: [...]`)
    * `--name` — migration file name (default `create_rheo_tables`)
    * `--migrations-path` — explicit output directory

  The generated migration delegates to `Rheo.Backend.Ecto.Migrations`, so a Rheo
  upgrade that changes the schema ships new DDL without rewriting your file.
  """

  @switches [repo: :string, name: :string, migrations_path: :string]
  @default_name "create_rheo_tables"

  @impl Mix.Task
  def run(args) do
    {opts, _rest} = OptionParser.parse!(args, strict: @switches)

    repo = Keyword.get_lazy(opts, :repo, &default_repo/0)
    name = Keyword.get(opts, :name, @default_name)
    path = Keyword.get_lazy(opts, :migrations_path, fn -> default_path(repo) end)
    file = Path.join(path, "#{timestamp()}_#{name}.exs")

    Mix.Generator.create_directory(path)
    Mix.Generator.create_file(file, migration(repo, name))

    file
  end

  defp default_repo do
    Mix.Project.config()
    |> Keyword.fetch!(:app)
    |> Application.get_env(:ecto_repos, [])
    |> List.first()
    |> case do
      nil -> "Repo"
      repo -> inspect(repo)
    end
  end

  defp default_path(repo) do
    dir = repo |> String.split(".") |> List.last() |> Macro.underscore()
    Path.join(["priv", dir, "migrations"])
  end

  defp migration(repo, name) do
    """
    defmodule #{repo}.Migrations.#{Macro.camelize(name)} do
      use Ecto.Migration

      def up, do: :ok = Rheo.Backend.Ecto.Migrations.up(repo(), prefix: prefix())

      def down, do: :ok = Rheo.Backend.Ecto.Migrations.down(repo(), prefix: prefix())
    end
    """
  end

  defp timestamp do
    %{year: year, month: month, day: day, hour: hour, minute: minute, second: second} =
      DateTime.utc_now()

    "#{year}#{pad(month)}#{pad(day)}#{pad(hour)}#{pad(minute)}#{pad(second)}"
  end

  defp pad(value) when value < 10, do: "0#{value}"
  defp pad(value), do: Integer.to_string(value)
end
