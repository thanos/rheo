# Rheo

[![CI](https://github.com/thanos/rheo/actions/workflows/ci.yml/badge.svg)](https://github.com/thanos/rheo/actions/workflows/ci.yml)
[![Hex.pm](https://img.shields.io/hexpm/v/rheo.svg)](https://hex.pm/packages/rheo)
[![Hex Docs](https://img.shields.io/badge/hex-docs-lightgreen.svg)](https://hexdocs.pm/rheo/)
[![Coverage Status](https://coveralls.io/repos/github/thanos/rheo/badge.svg?branch=main)](https://coveralls.io/github/thanos/rheo?branch=main)
[![License](https://img.shields.io/hexpm/l/rheo.svg)](LICENSE)

**v0.2.0** — Durable consumer-group semantics over searchable databases.
The first backend is **MongoDB**. Rheo is an Elixir/OTP library you embed in
your supervision tree, not a standalone messaging server.

**Delivery guarantee:** at-least-once. Duplicates are possible after failures —
use stable event IDs for idempotency.

## When to use

Use Rheo when you want:

- An **immutable, queryable event log** in a database you already run
- **Consumer groups** with leases, ACK, retry, and dead-lettering
- **Competing consumers** and **independent groups** on the same stream
- OTP-native demand, concurrency, and lease renewal — without standing up Kafka,
  RabbitMQ, or a separate broker cluster

Skip Rheo when you need a dedicated broker (massive fan-out, cross-language
clients, exactly-once claims) or when a simple job queue is enough.

### Compared with other approaches

| Approach | Strengths | Trade-offs |
|---|---|---|
| **Rheo + MongoDB** | Searchable history + durable groups in one store; embeds in OTP | At-least-once only; Mongo is the first backend |
| **Kafka / Pulsar** | Huge throughput, mature ops, many languages | Separate cluster; history search is not the primary model |
| **RabbitMQ / NATS** | Classic messaging, routing | Not an immutable searchable event log |
| **Oban / Broadway alone** | Great job/pipeline DX on Elixir | Different problem: jobs/pipelines, not durable consumer groups over an event log |
| **Raw Mongo change streams** | Live updates | No leases, ACK fencing, competing groups, or retry/DLQ |

Databases already store and search historical events well. Message brokers
already coordinate consumers well. Rheo combines those strengths: immutable,
queryable events in MongoDB, with leases, acknowledgement, retry, and competing
consumers in OTP.

## Installation

Add Rheo to your `mix.exs` dependencies:

```elixir
def deps do
  [
    {:rheo, "~> 0.2.0"}
  ]
end
```

Then fetch deps:

```bash
mix deps.get
```

Rheo needs a MongoDB URL (or another backend once available). A typical app
config:

```elixir
# config/runtime.exs
config :rheo,
  mongo_url: System.get_env("RHEO_MONGO_URL", "mongodb://localhost:27017/rheo")
```

## Quick start

Start Rheo and a consumer in your supervision tree:

```elixir
children = [
  {Rheo, name: MyRheo, backend: {Rheo.Backend.Mongo, url: "mongodb://localhost:27017/rheo"}},
  {MyApp.RiskConsumer, rheo: MyRheo, concurrency: 8, max_demand: 100}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

Define a consumer — handlers only implement `handle_event/2`; a local
`Rheo.Group` owns fetch, concurrency, lease renewal, and settle:

```elixir
defmodule MyApp.RiskConsumer do
  use Rheo.Consumer,
    stream: "market-events",
    group: "risk",
    concurrency: 8,
    max_demand: 100

  @impl true
  def handle_event(event, state) do
    case Risk.process(event) do
      :ok ->
        {:ack, state}

      {:temporary_error, reason} ->
        {:retry, reason, state}

      {:permanent_error, reason} ->
        {:reject, reason, state}
    end
  end
end
```

Create a stream, append events, and query history:

```elixir
Rheo.create_stream("market-events")
Rheo.create_group("market-events", "risk")

Rheo.append("market-events", %{
  type: "curve_update",
  currency: "EUR",
  price: 2.913
})

Rheo.query("market-events", type: "curve_update", currency: "EUR")
```

Interactive walkthrough: open [notebooks/rheo_demo.livemd](notebooks/rheo_demo.livemd)
in [Livebook](https://livebook.dev) (with Mongo running). Or from a clone:
`mix rheo.demo`.

Upgrading from 0.1? See the [0.1 → 0.2 migration guide](docs/migrations/0.1-to-0.2.md).

## Documentation

- [HexDocs](https://hexdocs.pm/rheo/) — API reference
- [Architecture](docs/architecture.md)
- [Tutorials](docs/tutorials.md)
- [ADRs](docs/adr.md)
- [Livebook demo](notebooks/rheo_demo.livemd)
- [0.1 → 0.2 migration](docs/migrations/0.1-to-0.2.md)
- [Changelog](CHANGELOG.md)
- [Roadmap](docs/roadmap.md)

## More examples

### Low-level fetch / ACK

```elixir
{:ok, leases} = Rheo.fetch("market-events", "risk", limit: 10, rheo: MyRheo)

Enum.each(leases, fn lease ->
  # process lease.event
  Rheo.ack(lease, rheo: MyRheo)
end)
```

### Portable queries

```elixir
Rheo.query("market-events",
  type: "curve_update",
  order_by: [sequence: :desc],
  limit: 50
)

# or structured:
Rheo.query(%Rheo.Query{
  stream: "market-events",
  where: [type: "curve_update"],
  order_by: [sequence: :desc],
  limit: 50
})
```

### Competing consumers and independent groups

Multiple processes can compete for the same durable group. Separate groups on
the same stream process every event independently (e.g. `"risk"` and
`"surveillance"`).

```elixir
Rheo.create_group("market-events", "risk")
Rheo.create_group("market-events", "surveillance")
```

### Lease renewal and concurrency

`Rheo.Group` renews inflight leases (~half of `lease_ms`) and runs up to
`:concurrency` handler tasks while bounding outstanding leases with
`:max_demand`.

```elixir
{MyApp.RiskConsumer,
 rheo: MyRheo,
 concurrency: 8,
 max_demand: 100,
 lease_ms: 30_000,
 poll_ms: 200}
```

### Named instances

Run more than one Rheo instance in the same BEAM:

```elixir
children = [
  {Rheo, name: MyRheo, backend: {Rheo.Backend.Mongo, url: primary_url}},
  {Rheo, name: MyRheoAudit, backend: {Rheo.Backend.Mongo, url: audit_url}}
]
```

Pass `rheo: MyRheo` (or `rheo: MyRheoAudit`) on APIs and consumers.

## Roadmap

| Version | Focus |
|---|---|
| **0.1.0** | MVP: Mongo event log, leases/ACK, competing consumers, query, `Rheo.Consumer` |
| **0.2.0** (current) | `Rheo.Group` runtime, real concurrency, lease renewal, multi-instance handles, portable `Rheo.Query`, persistence-error semantics |
| **0.3.0** | ETS backend + backend conformance |
| **0.4.0** | Search/replay ergonomics and event lineage |
| **0.5.0** | Partitioning and ordered consume within a partition |
| **0.6.0** | PostgreSQL (or second durable) backend |
| **0.7.0** | Mongo change-stream wakeups |
| **0.8.0** | Ops surface: DLQ inspection, lag metrics, admin helpers |
| **0.9.0** | API freeze candidate |
| **1.0.0** | Stable public API (SemVer for `Rheo` / `Rheo.Consumer` / `Rheo.Backend`) |

Still out of scope through 1.0 unless demand forces it: standalone Rheo server,
exactly-once claims, K8s operator, auth frameworks, multi-tenancy. Details in
[docs/roadmap.md](docs/roadmap.md).

## License

MIT — see [LICENSE](LICENSE).

## Building and developing the library

For contributors working on Rheo itself (not application consumers):

```bash
docker compose up -d
mix deps.get
mix test
mix rheo.demo
```

Quality gates:

```bash
mix format --check-formatted
mix compile --warnings-as-errors
mix credo --strict
mix dialyzer
mix coveralls
```

Mongo-backed tests run by default when Mongo is available. Heavier end-to-end
scenarios are tagged `:integration` and excluded unless enabled:

```bash
RHEO_INTEGRATION=1 mix test
# or
mix test.integration
```

Unit-only (excludes Mongo):

```bash
mix test.unit
```

Livebook from a clone:

```bash
docker compose up -d
livebook server notebooks/rheo_demo.livemd
```

CI tests **Erlang/OTP 27–29** × **Elixir 1.17–1.20** (excluding unsupported
pairs). Format, Credo, Dialyzer, and Coveralls run on Elixir 1.20.2 / OTP 29.

Coverage publishes to [Coveralls](https://coveralls.io/github/thanos/rheo) from
CI via `mix coveralls.github`. Hex releases publish from annotated tags (`v*`);
set repository secrets `COVERALLS_REPO_TOKEN` and `HEX_API_KEY`.
