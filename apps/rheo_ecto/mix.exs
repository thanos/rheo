defmodule RheoEcto.MixProject do
  use Mix.Project

  @version "0.8.0"
  @source_url "https://github.com/thanos/rheo"

  def project do
    [
      app: :rheo_ecto,
      version: @version,
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      description: "Ecto SQL backend for Rheo (PostgreSQL / SQLite)",
      source_url: @source_url
    ]
  end

  def application, do: [extra_applications: [:logger]]

  defp deps do
    [
      {:rheo, path: "../..", env: Mix.env()},
      {:ecto, "~> 3.11"},
      {:ecto_sql, "~> 3.11"},
      {:postgrex, "~> 0.19", optional: true},
      {:ecto_sqlite3, "~> 0.17", optional: true}
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url},
      files: ~w(lib mix.exs)
    ]
  end
end
