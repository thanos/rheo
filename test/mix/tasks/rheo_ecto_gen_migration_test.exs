defmodule Mix.Tasks.Rheo.Ecto.GenMigrationTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  @task Mix.Tasks.Rheo.Ecto.GenMigration

  setup do
    tmp = Path.join(System.tmp_dir!(), "rheo_gen_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf(tmp) end)
    %{tmp: tmp}
  end

  test "generates a migration that delegates to the library", %{tmp: tmp} do
    file = generate(["--migrations-path", tmp, "--repo", "MyApp.Repo"])

    assert Path.dirname(file) == tmp
    assert Path.basename(file) =~ ~r/^\d{14}_create_rheo_tables\.exs$/

    contents = File.read!(file)
    assert contents =~ "defmodule MyApp.Repo.Migrations.CreateRheoTables do"
    assert contents =~ "Rheo.Backend.Ecto.Migrations.up(repo(), prefix: prefix())"
    assert contents =~ "Rheo.Backend.Ecto.Migrations.down(repo(), prefix: prefix())"

    # The generated file must at least be valid Elixir.
    assert {:ok, _ast} = Code.string_to_quoted(contents)
  end

  test "honours a custom migration name", %{tmp: tmp} do
    file = generate(["--migrations-path", tmp, "--name", "add_rheo_tables"])

    assert Path.basename(file) =~ "add_rheo_tables"
    assert File.read!(file) =~ "Migrations.AddRheoTables do"
  end

  test "derives the output path from the first configured repo" do
    put_ecto_repos([Rheo.Test.SqliteRepo])
    on_exit(fn -> File.rm_rf("priv/sqlite_repo") end)

    file = generate([])

    assert Path.dirname(file) == Path.join(["priv", "sqlite_repo", "migrations"])
    assert File.read!(file) =~ "defmodule Rheo.Test.SqliteRepo.Migrations.CreateRheoTables do"
  end

  test "falls back to priv/repo/migrations without configuration" do
    put_ecto_repos(nil)
    on_exit(fn -> File.rm_rf("priv/repo") end)

    file = generate([])

    assert Path.dirname(file) == Path.join(["priv", "repo", "migrations"])
    assert File.read!(file) =~ "defmodule Repo.Migrations.CreateRheoTables do"
  end

  defp generate(args) do
    capture_io(fn -> send(self(), {:generated, @task.run(args)}) end)

    receive do
      {:generated, file} -> file
    end
  end

  defp put_ecto_repos(repos) do
    previous = Application.get_env(:rheo, :ecto_repos)

    on_exit(fn ->
      if previous do
        Application.put_env(:rheo, :ecto_repos, previous)
      else
        Application.delete_env(:rheo, :ecto_repos)
      end
    end)

    if repos do
      Application.put_env(:rheo, :ecto_repos, repos)
    else
      Application.delete_env(:rheo, :ecto_repos)
    end
  end
end
