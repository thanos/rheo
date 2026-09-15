defmodule Rheo.MixProject do
  use Mix.Project

  @version "0.2.0"
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
          "LICENSE",
          "docs/architecture.md",
          "docs/roadmap.md",
          "docs/diagrams.md",
          "docs/adr.md",
          "docs/adr/001-at-least-once-delivery.md",
          "docs/adr/002-events-immutable-consumer-state-separate.md",
          "docs/adr/003-mongodb-first-backend.md",
          "docs/adr/004-lease-and-fencing-model.md",
          "docs/adr/005-backend-boundary.md",
          "docs/adr/006-rheo-as-embedded-otp-library.md",
          "docs/adr/007-demand-and-backpressure.md",
          "docs/adr/008-mongodb-schema-and-indexes.md",
          "docs/adr/009-local-consumer-group-runtime.md",
          "docs/adr/010-backend-handle-and-instance-model.md",
          "docs/adr/013-portable-query-model.md",
          "docs/migrations/0.1-to-0.2.md",
          "CHANGELOG.md",
          "docs/tutorials.md",
          "docs/tutorials/01-why-consumer-groups-on-a-database.md",
          "docs/tutorials/02-what-is-a-consumer-group.md",
          "docs/tutorials/03-why-ack-is-harder.md",
          "docs/tutorials/04-rheo-as-otp-library.md",
          "docs/tutorials/05-demand-and-backpressure.md",
          "docs/tutorials/06-mongodb-searchable-event-log.md",
          "docs/tutorials/07-killing-consumers.md",
          "docs/tutorials/08-searching-the-stream.md",
          "docs/tutorials/09-why-rheo-0-2-broke-its-0-1-api.md",
          "notebooks/rheo_demo.livemd"
        ],
        groups_for_extras: [
          Guides: [
            "docs/architecture.md",
            "docs/roadmap.md",
            "docs/diagrams.md",
            "docs/migrations/0.1-to-0.2.md",
            "CHANGELOG.md",
            "notebooks/rheo_demo.livemd"
          ],
          ADRs: [
            "docs/adr.md",
            "docs/adr/001-at-least-once-delivery.md",
            "docs/adr/002-events-immutable-consumer-state-separate.md",
            "docs/adr/003-mongodb-first-backend.md",
            "docs/adr/004-lease-and-fencing-model.md",
            "docs/adr/005-backend-boundary.md",
            "docs/adr/006-rheo-as-embedded-otp-library.md",
            "docs/adr/007-demand-and-backpressure.md",
            "docs/adr/008-mongodb-schema-and-indexes.md",
            "docs/adr/009-local-consumer-group-runtime.md",
            "docs/adr/010-backend-handle-and-instance-model.md",
            "docs/adr/013-portable-query-model.md"
          ],
          Tutorials: [
            "docs/tutorials.md",
            "docs/tutorials/01-why-consumer-groups-on-a-database.md",
            "docs/tutorials/02-what-is-a-consumer-group.md",
            "docs/tutorials/03-why-ack-is-harder.md",
            "docs/tutorials/04-rheo-as-otp-library.md",
            "docs/tutorials/05-demand-and-backpressure.md",
            "docs/tutorials/06-mongodb-searchable-event-log.md",
            "docs/tutorials/07-killing-consumers.md",
            "docs/tutorials/08-searching-the-stream.md",
            "docs/tutorials/09-why-rheo-0-2-broke-its-0-1-api.md"
          ]
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
      {:mox, "~> 1.1", only: :test},
      {:excoveralls, "~> 0.18", only: :test}
    ]
  end

  defp aliases do
    [
      "rheo.demo": ["run priv/demo/demo.exs"],
      "test.unit": ["test", "--exclude", "mongo", "--exclude", "integration"],
      "test.integration": ["test", "--include", "integration"],
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
