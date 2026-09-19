defmodule Rheo.MixProject do
  use Mix.Project

  @version "0.11.0"
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
        # `:mnesia` is included_applications (loaded, not auto-started) — add to PLT
        # so Dialyzer sees OTP APIs without putting it in extra_applications.
        plt_add_apps: [:ex_unit, :mix, :mnesia],
        flags: [:error_handling]
      ],
      docs: [
        main: "readme",
        source_url: @source_url,
        source_ref: "v#{@version}",
        assets: %{"docs/screenshots" => "screenshots"},
        extras: [
          "README.md",
          "LICENSE",
          "docs/guides/quick-start.md",
          "CHANGELOG.md",
          "notebooks/rheo_demo.livemd",
          "notebooks/quickstart.livemd",
          "notebooks/concepts.livemd",
          {"notebooks/ops.livemd", [filename: "ops-livebook"]},
          "notebooks/live_dashboard.livemd",
          "notebooks/pipelines.livemd",
          "notebooks/backends.livemd",
          "docs/guides/configuration.md",
          "docs/guides/consumer-groups.md",
          "docs/guides/enqueuing.md",
          "docs/guides/dequeuing.md",
          "docs/guides/replay.md",
          "docs/guides/querying.md",
          "docs/guides/partitions-and-lag.md",
          {"docs/guides/ops.md", [filename: "ops"]},
          "docs/guides/ets.md",
          "docs/guides/mnesia.md",
          "docs/guides/mongo.md",
          "docs/guides/using-ecto.md",
          "docs/guides/redis.md",
          "docs/guides/building-your-own-backend.md",
          "docs/guides/broadway.md",
          "docs/guides/genstage.md",
          "docs/upgrading.md",
          "docs/migrations/0.9-to-0.10.md",
          "docs/migrations/0.10-to-0.11.md",
          "docs/architecture.md",
          "docs/diagrams.md",
          "docs/roadmap.md",
          "docs/adr.md",
          "docs/adr/019-v0-8-architectural-reset.md",
          "docs/adr/020-package-and-dependency-boundaries.md",
          "docs/adr/021-logical-sequence-and-native-delivery-receipts.md",
          "docs/adr/022-consumer-runtime-and-handler-state.md",
          "docs/adr/023-backend-capabilities-v2.md",
          "docs/adr/024-backend-contract-v2.md",
          "docs/adr/025-backend-wakeup-contract.md",
          "docs/adr/026-redis-streams-backend.md",
          "docs/adr/027-ops-surface.md",
          "docs/adr/028-mnesia-backend.md",
          "docs/tutorials.md"
        ],
        groups_for_extras: [
          Start: [
            "README.md",
            "LICENSE",
            "docs/guides/quick-start.md",
            "CHANGELOG.md"
          ],
          Livebooks: [
            "notebooks/rheo_demo.livemd",
            "notebooks/quickstart.livemd",
            "notebooks/concepts.livemd",
            "notebooks/ops.livemd",
            "notebooks/live_dashboard.livemd",
            "notebooks/pipelines.livemd",
            "notebooks/backends.livemd"
          ],
          Guides: [
            "docs/guides/configuration.md",
            "docs/guides/consumer-groups.md",
            "docs/guides/enqueuing.md",
            "docs/guides/dequeuing.md",
            "docs/guides/replay.md",
            "docs/guides/querying.md",
            "docs/guides/partitions-and-lag.md",
            "docs/guides/ops.md"
          ],
          Backends: [
            "docs/guides/ets.md",
            "docs/guides/mnesia.md",
            "docs/guides/mongo.md",
            "docs/guides/using-ecto.md",
            "docs/guides/redis.md",
            "docs/guides/building-your-own-backend.md"
          ],
          Pipelines: [
            "docs/guides/broadway.md",
            "docs/guides/genstage.md"
          ],
          Upgrading: [
            "docs/upgrading.md",
            "docs/migrations/0.9-to-0.10.md",
            "docs/migrations/0.10-to-0.11.md"
          ],
          Design: [
            "docs/architecture.md",
            "docs/diagrams.md",
            "docs/roadmap.md",
            "docs/adr.md",
            "docs/adr/019-v0-8-architectural-reset.md",
            "docs/adr/020-package-and-dependency-boundaries.md",
            "docs/adr/021-logical-sequence-and-native-delivery-receipts.md",
            "docs/adr/022-consumer-runtime-and-handler-state.md",
            "docs/adr/023-backend-capabilities-v2.md",
            "docs/adr/024-backend-contract-v2.md",
            "docs/adr/025-backend-wakeup-contract.md",
            "docs/adr/026-redis-streams-backend.md",
            "docs/adr/027-ops-surface.md",
            "docs/adr/028-mnesia-backend.md",
            "docs/tutorials.md"
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
      # Load OTP `:mnesia` without auto-starting it. Auto-start creates a ram
      # schema; recycling via `:mnesia.stop/0` breaks Livebook (Logger `:epipe`).
      # `Rheo.Backend.Mnesia` starts `:mnesia` after `:dir` / disc schema are set.
      extra_applications: [:logger],
      included_applications: [:mnesia],
      mod: {Rheo.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:telemetry, "~> 1.2"},
      {:jason, "~> 1.4"},
      # Optional integrations (ADR 020): each guarded module compiles only when
      # its dependency is present in the host application.
      {:mongodb_driver, "~> 1.5", optional: true},
      {:ecto, "~> 3.11", optional: true},
      {:ecto_sql, "~> 3.11", optional: true},
      {:postgrex, "~> 0.19", optional: true},
      {:ecto_sqlite3, "~> 0.17", optional: true},
      {:redix, "~> 1.5", optional: true},
      {:gen_stage, "~> 1.2", optional: true},
      {:broadway, "~> 1.2", optional: true},
      {:telemetry_metrics, "~> 1.0", optional: true},
      {:phoenix_live_dashboard, "~> 0.8", optional: true},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.34", only: [:dev, :test], runtime: false},
      {:stream_data, "~> 1.1", only: [:dev, :test]},
      {:flow, "~> 1.2", only: :test},
      {:mox, "~> 1.1", only: :test},
      {:excoveralls, "~> 0.18", only: :test}
    ]
  end

  defp aliases do
    [
      "rheo.demo": ["run priv/demo/demo.exs"],
      "test.unit": [
        "test",
        "--exclude",
        "mongo",
        "--exclude",
        "redis",
        "--exclude",
        "integration"
      ],
      "test.integration": ["test", "--include", "integration"],
      "core.check": &core_check/1,
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
      {"docs --warnings-as-errors", :dev},
      {"core.check", :dev}
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

  # Compiles a throwaway project that depends on this checkout without any
  # optional integration, proving the guards in lib/ hold (ADR 020).
  defp core_check(_) do
    script = Path.expand("priv/scripts/check_core_only.sh", __DIR__)
    {_, exit_code} = System.cmd("bash", [script], into: IO.stream())
    if exit_code != 0, do: Mix.raise("core-only build check failed (exit code #{exit_code})")
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
    "Durable, searchable, replayable consumer-group semantics over storage " <>
      "systems (MongoDB, Redis Streams, PostgreSQL/SQLite via Ecto, Mnesia, ETS): " <>
      "leases, fencing, partitions, frontier, lag, replay, ops inspect, and a " <>
      "GenStage/Broadway producer."
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
