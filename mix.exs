defmodule Rheo.MixProject do
  use Mix.Project

  @version "0.7.1"
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
          "CHANGELOG.md",
          "notebooks/rheo_demo.livemd",
          "docs/guides/quick-start.md",
          "docs/guides/configuration.md",
          "docs/guides/consumer-groups.md",
          "docs/guides/enqueuing.md",
          "docs/guides/dequeuing.md",
          "docs/guides/replay.md",
          "docs/guides/querying.md",
          "docs/guides/partitions-and-lag.md",
          "docs/guides/ets.md",
          "docs/guides/mongo.md",
          "docs/guides/using-ecto.md",
          "docs/guides/broadway.md",
          "docs/guides/genstage.md",
          "docs/guides/building-your-own-backend.md",
          "docs/migrations/0.1-to-0.2.md",
          "docs/migrations/0.3-to-0.4.md",
          "docs/migrations/0.4-to-0.5.md",
          "docs/migrations/0.5-to-0.6.md",
          "docs/migrations/0.6-to-0.7.md",
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
          "docs/adr/011-backend-capabilities.md",
          "docs/adr/012-backend-conformance-suite.md",
          "docs/adr/013-portable-query-model.md",
          "docs/adr/014-ets-backend.md",
          "docs/adr/015-replay-semantics.md",
          "docs/adr/016-partitions-and-ack-frontier.md",
          "docs/adr/017-ecto-backend.md",
          "docs/adr/018-broadway-genstage-interop.md",
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
          "docs/tutorials/10-if-rheo-is-database-agnostic-prove-it-with-ets.md",
          "docs/tutorials/11-search-and-replay-the-event-history.md",
          "docs/tutorials/12-acks-are-not-a-cursor.md",
          "docs/tutorials/13-one-consumer-api-postgresql-and-sqlite.md",
          "docs/tutorials/14-rheo-is-not-broadway-it-feeds-broadway.md"
        ],
        groups_for_extras: [
          Guides: [
            "docs/guides/quick-start.md",
            "docs/guides/configuration.md",
            "docs/guides/consumer-groups.md",
            "docs/guides/enqueuing.md",
            "docs/guides/dequeuing.md",
            "docs/guides/replay.md",
            "docs/guides/querying.md",
            "docs/guides/partitions-and-lag.md",
            "docs/guides/ets.md",
            "docs/guides/mongo.md",
            "docs/guides/using-ecto.md",
            "docs/guides/broadway.md",
            "docs/guides/genstage.md",
            "docs/guides/building-your-own-backend.md",
            "CHANGELOG.md",
            "notebooks/rheo_demo.livemd"
          ],
          "Migrating from previous versions": [
            "docs/migrations/0.1-to-0.2.md",
            "docs/migrations/0.3-to-0.4.md",
            "docs/migrations/0.4-to-0.5.md",
            "docs/migrations/0.5-to-0.6.md",
            "docs/migrations/0.6-to-0.7.md"
          ],
          "Design": [
            "docs/architecture.md",

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
            "docs/adr/011-backend-capabilities.md",
            "docs/adr/012-backend-conformance-suite.md",
            "docs/adr/013-portable-query-model.md",
            "docs/adr/014-ets-backend.md",
            "docs/adr/015-replay-semantics.md",
            "docs/adr/016-partitions-and-ack-frontier.md",
            "docs/adr/017-ecto-backend.md",
            "docs/adr/018-broadway-genstage-interop.md",
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
            "docs/tutorials/10-if-rheo-is-database-agnostic-prove-it-with-ets.md",
            "docs/tutorials/11-search-and-replay-the-event-history.md",
            "docs/tutorials/12-acks-are-not-a-cursor.md",
            "docs/tutorials/13-one-consumer-api-postgresql-and-sqlite.md",
            "docs/tutorials/14-rheo-is-not-broadway-it-feeds-broadway.md"
          ]
        ],
        before_closing_body_tag: &before_closing_body_tag/1
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
      # Ecto SQL backend. `ecto_sql` is a hard dependency so `Rheo.Backend.Ecto`
      # compiles without conditional guards; adapters stay optional because the
      # host app owns the Repo and picks its own driver.
      {:ecto, "~> 3.11"},
      {:ecto_sql, "~> 3.11"},
      {:postgrex, "~> 0.19", optional: true},
      {:ecto_sqlite3, "~> 0.17", optional: true},
      # GenStage / Broadway interop. Hard dependencies so `Rheo.Producer` and
      # `Rheo.Broadway.Acknowledger` compile against the real behaviours instead
      # of `Code.ensure_loaded?/1` guards (ADR 018).
      {:gen_stage, "~> 1.2"},
      {:broadway, "~> 1.2"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.34", only: [:dev, :test], runtime: false},
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
      ci: &verify/1
    ]
  end

  defp verify(_) do
    steps = [
      {"compile --warnings-as-errors", :dev},
      {"format --check-formatted", :dev},
      {"credo --strict", :dev},
      {"dialyzer", :dev},
      {"test --cover", :test},
      {"docs --warnings-as-errors", :dev}
    ]

    Enum.each(steps, fn {task, env} ->
      Mix.shell().info([:bright, "==> mix #{task}", :reset])

      {_, exit_code} =
        System.cmd("mix", String.split(task),
          env: [{"MIX_ENV", to_string(env)}],
          into: IO.stream()
        )

      if exit_code != 0 do
        Mix.raise("mix #{task} failed (exit code #{exit_code})")
      end
    end)

    Mix.shell().info([:green, :bright, "\nAll verification checks passed!", :reset])
  end

  defp before_closing_body_tag(:html) do
    """
    <script defer src="https://cdn.jsdelivr.net/npm/mermaid@10.2.3/dist/mermaid.min.js"></script>
    <script>
      let initialized = false;

      window.addEventListener("exdoc:loaded", () => {
        if (!initialized) {
          mermaid.initialize({
            startOnLoad: false,
            theme: document.body.className.includes("dark") ? "dark" : "default"
          });
          initialized = true;
        }

        let id = 0;
        for (const codeEl of document.querySelectorAll("pre code.mermaid")) {
          const preEl = codeEl.parentElement;
          const graphDefinition = codeEl.textContent;
          const graphEl = document.createElement("div");
          const graphId = "mermaid-graph-" + id++;
          mermaid.render(graphId, graphDefinition).then(({svg, bindFunctions}) => {
            graphEl.innerHTML = svg;
            bindFunctions?.(graphEl);
            preEl.insertAdjacentElement("afterend", graphEl);
            preEl.remove();
          });
        }
      });
    </script>
    """
  end

  defp before_closing_body_tag(_), do: ""

  defp description do
    "Durable consumer-group semantics over searchable databases " <>
      "(MongoDB, PostgreSQL/SQLite via Ecto, ETS). Partitions, contiguous ACK " <>
      "frontier, lag, and replay."
  end

  defp package do
    [
      name: "rheo",
      licenses: ["MIT"],
      links: %{
        "GitHub" => @source_url,
        "Docs" => "https://hexdocs.pm/rheo",
        "Changelog" => "https://hexdocs.pm/rheo/changelog.html"
      },
      files: ~w(
        lib
        .formatter.exs
        mix.exs
        README.md
        LICENSE
        CHANGELOG.md
        coveralls.json
      )
    ]
  end
end
