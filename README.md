# Rheo

[![CI](https://github.com/thanos/rheo/actions/workflows/ci.yml/badge.svg)](https://github.com/thanos/rheo/actions/workflows/ci.yml)
[![Hex.pm](https://img.shields.io/hexpm/v/rheo.svg)](https://hex.pm/packages/rheo)
[![Hex Docs](https://img.shields.io/badge/hex-docs-lightgreen.svg)](https://hexdocs.pm/rheo/)
[![Coverage Status](https://coveralls.io/repos/github/thanos/rheo/badge.svg?branch=main)](https://coveralls.io/github/thanos/rheo?branch=main)
[![Credo](https://img.shields.io/badge/credo-strict-brightgreen.svg)](https://github.com/rrrene/credo)
[![Dialyzer](https://img.shields.io/badge/dialyzer-passing-brightgreen.svg)](https://github.com/jeremyjh/dialyxir)
[![License](https://img.shields.io/hexpm/l/rheo.svg)](LICENSE)

Rheo is an Elixir/OTP library that provides durable **consumer-group** semantics
over searchable databases. The first backend is **MongoDB**.

Rheo is not a standalone messaging server. Applications add Rheo and consumers to
their existing supervision tree.

## Why

Databases already store and search historical events well. Message brokers already
coordinate consumers well. Rheo combines those strengths: immutable, queryable
events in MongoDB, with leases, acknowledgement, retry, and competing consumers
in OTP.

**Delivery guarantee:** at-least-once. Duplicates are possible after failures.
Use stable event IDs for idempotency.

## Quick start

```bash
docker compose up -d
mix deps.get
mix test
mix rheo.demo
```

Interactive walkthrough (Livebook):

```bash
docker compose up -d
livebook server notebooks/rheo_demo.livemd
```

Or open [notebooks/rheo_demo.livemd](notebooks/rheo_demo.livemd) in [Livebook](https://livebook.dev).

## Usage

```elixir
children = [
  {Rheo, url: "mongodb://localhost:27017/rheo"},
  {MyApp.RiskConsumer, concurrency: 1}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

```elixir
defmodule MyApp.RiskConsumer do
  use Rheo.Consumer,
    stream: "market-events",
    group: "risk",
    max_demand: 10

  @impl true
  def handle_event(event, state) do
    Risk.process(event)
    {:ack, state}
  end
end
```

Low-level API:

```elixir
Rheo.create_stream("market-events")
Rheo.append("market-events", %{type: "curve_update", currency: "EUR", price: 2.913})
Rheo.create_group("market-events", "risk")
{:ok, leases} = Rheo.fetch("market-events", "risk", limit: 10)
Enum.each(leases, &Rheo.ack/1)

Rheo.query("market-events", type: "curve_update", currency: "EUR")
```

## Quality gates

```bash
mix format --check-formatted
mix compile --warnings-as-errors
mix credo --strict
mix dialyzer
mix coveralls
```

CI tests a compatibility matrix of **Erlang/OTP 27–29** × **Elixir 1.17–1.20**
(excluding unsupported pairs per the [Elixir compatibility table](https://hexdocs.pm/elixir/compatibility-and-deprecations.html)).
Format, Credo, Dialyzer, and Coveralls run on Elixir 1.20.2 / OTP 29.

Coverage is published to [Coveralls](https://coveralls.io/github/thanos/rheo) from CI via
`mix coveralls.github`. Locally use `mix coveralls` or `mix coveralls.html`.

Hex releases publish from annotated version tags (`v*`) or a manual workflow run;
set repository secrets `COVERALLS_REPO_TOKEN` and `HEX_API_KEY`.

## Release roadmap

| Version | Focus |
|---|---|
| **0.1.0** | MVP: Mongo event log, leases/ACK, competing consumers, independent groups, query, `Rheo.Consumer`, demo |
| **0.2.0** | Hardening: richer telemetry, lease observability, retention hooks, query ergonomics |
| **0.3.0** | Partitioning: key-based partitions, ordered consume within a partition |
| **0.4.0** | Demand evolution: stronger backpressure, optional GenStage/Broadway interop |
| **0.5.0** | Second backend: PostgreSQL adapter; tighten `Rheo.Backend` from real portability lessons |
| **0.6.0** | Mongo push path: change-stream wakeups where they beat polling; keep poll fallback |
| **0.7.0** | Multi-node: safe concurrent consumers across BEAM nodes via durable Mongo coordination |
| **0.8.0** | Ops surface: dead-letter inspection APIs, lag metrics, admin-friendly query helpers |
| **0.9.0** | API freeze candidate: docs, benchmarks, compatibility guarantees, deprecations cleared |
| **1.0.0** | Stable public API: semantic versioning commitment for `Rheo` / `Rheo.Consumer` / `Rheo.Backend` |

Still out of scope through 1.0 unless demand forces it: standalone Rheo server, exactly-once claims, K8s operator, auth frameworks, multi-tenancy. See [docs/roadmap.md](docs/roadmap.md).

## Documentation

- [Livebook demo](notebooks/rheo_demo.livemd) — interactive end-to-end walkthrough
- [Architecture](docs/architecture.md)
- [Roadmap](docs/roadmap.md)
- [ADRs](docs/adr/)
- [Tutorials](docs/tutorials/)

## License

MIT
