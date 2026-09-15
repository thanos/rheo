defmodule Rheo.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/thanos/rheo"

  def project do
    [
      app: :rheo,
      version: @version,
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      elixirc_paths: elixirc_paths(Mix.env()),
      test_coverage: [tool: ExCoveralls],
      dialyzer: [
        plt_local_path: "priv/plts",
        plt_core_path: "priv/plts",
        plt_add_apps: [:ex_unit, :mix],
        # Mongo driver result typespecs yield false-positive pattern_match warnings.
        flags: [:error_handling],
        ignore_warnings: ".dialyzer_ignore.exs"
      ],
      docs: [
        main: "readme",
        source_url: @source_url,
        source_ref: "v#{@version}",
        extras: [
          "README.md",
          "docs/architecture.md",
          "docs/roadmap.md",
          "notebooks/rheo_demo.livemd"
        ]
      ],
      package: package(),
      description: description(),
      name: "rheo",
      homepage_url: @source_url
    ]
  end

  def cli do
    [
      preferred_envs: [
        coveralls: :test,
        "coveralls.html": :test,
        "coveralls.detail": :test,
        "coveralls.github": :test,
        "coveralls.json": :test,
        credo: :test,
        dialyzer: :dev,
        "rheo.demo": :dev
      ]
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Rheo.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:mongodb_driver, "~> 1.5"},
      {:telemetry, "~> 1.2"},
      {:jason, "~> 1.4"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:stream_data, "~> 1.1", only: [:dev, :test]},
      {:excoveralls, "~> 0.18", only: :test}
    ]
  end

  defp aliases do
    [
      "rheo.demo": ["run priv/demo/demo.exs"],
      quality: [
        "format --check-formatted",
        "compile --warnings-as-errors",
        "credo --strict",
        "dialyzer",
        "coveralls"
      ]
    ]
  end

  defp description do
    "Durable consumer-group semantics over searchable databases (MongoDB first)."
  end

  defp package do
    [
      name: "rheo",
      licenses: ["MIT"],
      links: %{
        "GitHub" => @source_url,
        "Docs" => "https://hexdocs.pm/rheo"
      },
      files: ~w(
        lib
        .formatter.exs
        mix.exs
        README.md
        LICENSE
        coveralls.json
      )
    ]
  end
end
