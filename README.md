# Rheo

[![CI](https://github.com/thanos/rheo/actions/workflows/ci.yml/badge.svg)](https://github.com/thanos/rheo/actions/workflows/ci.yml)
[![Hex.pm](https://img.shields.io/hexpm/v/rheo.svg)](https://hex.pm/packages/rheo)
[![Hex Docs](https://img.shields.io/badge/hex-docs-lightgreen.svg)](https://hexdocs.pm/rheo/)
[![Coverage Status](https://coveralls.io/repos/github/thanos/rheo/badge.svg?branch=main)](https://coveralls.io/github/thanos/rheo?branch=main)
[![License](https://img.shields.io/hexpm/l/rheo.svg)](https://github.com/thanos/rheo/blob/main/LICENSE)

**v0.10.0** — Durable consumer-group semantics over searchable stores.
Backends: **Redis Streams**, **MongoDB**, **PostgreSQL / SQLite** (host-owned
`Ecto.Repo`), and **ETS** (ephemeral). Rheo is an Elixir/OTP library you embed
in your supervision tree, not a standalone messaging server. Consume with
`Rheo.Consumer` or feed a **Broadway** pipeline with `Rheo.Producer`.

**Delivery guarantee:** at-least-once. Duplicates are possible after failures —
use stable event IDs for idempotency. Ordering is guaranteed **within a
partition** only (not globally across partitions).

Rheo is pre-1.0 and under active architectural development. Breaking changes
between minor releases may occur while the backend and consumer-group
contracts are refined; each one ships with a migration guide.

## When to use

Use Rheo when you want:

- An **immutable, queryable event log** in a database you already run
- **Consumer groups** with leases, ACK, retry, and dead-lettering
- **Competing consumers** and **independent groups** on the same stream
- **Partitions** with key routing, a contiguous ACK frontier, and `Rheo.lag/3`
- **SQL backends** via a host-owned `Ecto.Repo` (PostgreSQL or SQLite)
- A **durable source for Broadway/GenStage** instead of a second broker
- OTP-native demand, concurrency, and lease renewal — without standing up Kafka,
  RabbitMQ, or a separate broker cluster

Skip Rheo when you need a dedicated broker (massive fan-out, cross-language
clients, exactly-once claims) or when a simple job queue is enough.

### Compared with other approaches

| Approach | Strengths | Trade-offs |
|---|---|---|
| **Rheo + MongoDB** | Searchable history + durable groups in one store; embeds in OTP | At-least-once only |
| **Rheo + PostgreSQL** | Uses the database you already run; `jsonb` event log you can query in SQL; `SKIP LOCKED` claims across nodes | At-least-once only; you own the repo and migrations |
| **Rheo + SQLite** | Durable with no service at all; same API | Single node only (`distributed: false`) |
| **Rheo + ETS** | Same API with no Docker/DB; great for tests and Livebook | Ephemeral — data dies with the owner process |
| **Kafka / Pulsar** | Huge throughput, mature ops, many languages | Separate cluster; history search is not the primary model |
| **RabbitMQ / NATS** | Classic messaging, routing | Not an immutable searchable event log |
| **Oban / Broadway alone** | Great job/pipeline DX on Elixir | Different problem: jobs/pipelines, not durable consumer groups over an event log — so Rheo feeds Broadway rather than replacing it (`Rheo.Producer`) |
| **Raw Mongo change streams** | Live updates | No leases, ACK fencing, competing groups, or retry/DLQ |

Databases already store and search historical events well. Message brokers
already coordinate consumers well. Rheo combines those strengths: immutable,
queryable events in a database you run (MongoDB or PostgreSQL/SQLite), with
leases, acknowledgement, retry, and competing consumers in OTP.

## Installation

Add Rheo to your `mix.exs` dependencies:

```elixir
def deps do
  [
    {:rheo, "~> 0.10.0"}
  ]
end
```

### Optional integrations

Rheo core needs only `telemetry` and `jason`; ETS works out of the box. Add
the dependencies for the integrations you use and the matching modules are
compiled:

| Dependency | Enables |
|---|---|
| `{:mongodb_driver, "~> 1.5"}` | `Rheo.Backend.Mongo` |
| `{:redix, "~> 1.5"}` | `Rheo.Backend.Redis` (Redis 6.2+) |
| `{:ecto_sql, "~> 3.11"}` + `{:postgrex, "~> 0.19"}` or `{:ecto_sqlite3, "~> 0.17"}` | `Rheo.Backend.Ecto`, `mix rheo.ecto.gen_migration` |
| `{:gen_stage, "~> 1.2"}` | `Rheo.Producer` |
| `{:broadway, "~> 1.2"}` | `Rheo.Broadway` and its acknowledger |

Then fetch deps:

```bash
mix deps.get
```

Choose a backend:

```elixir
# Zero-infra (tests, Livebook, ephemeral apps)
{Rheo, name: MyRheo, backend: Rheo.Backend.ETS}

# Durable MongoDB
{Rheo, name: MyRheo, backend: {Rheo.Backend.Mongo, url: "mongodb://localhost:27017/rheo"}}

# Durable SQL on a repo your app already supervises (PostgreSQL or SQLite)
{Rheo, name: MyRheo, backend: {Rheo.Backend.Ecto, repo: MyApp.Repo}}
```


## Quick start

### ETS (no Docker)

```elixir
children = [
  {Rheo, name: MyRheo, backend: Rheo.Backend.ETS},
  {MyApp.RiskConsumer, rheo: MyRheo, concurrency: 8, max_demand: 100}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

### MongoDB

```elixir
children = [
  {Rheo, name: MyRheo, backend: {Rheo.Backend.Mongo, url: "mongodb://localhost:27017/rheo"}},
  {MyApp.RiskConsumer, rheo: MyRheo, concurrency: 8, max_demand: 100}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

### PostgreSQL or SQLite (Ecto)

Your app owns the repo; Rheo borrows it and never starts the pool:

```elixir
children = [
  MyApp.Repo,
  {Rheo, name: MyRheo, backend: {Rheo.Backend.Ecto, repo: MyApp.Repo}},
  {MyApp.RiskConsumer, rheo: MyRheo, concurrency: 8, max_demand: 100}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

Create the five `rheo_*` tables with a migration (preferred in production, so it
runs once under your release's migration step):

```bash
mix rheo.ecto.gen_migration --repo MyApp.Repo
mix ecto.migrate
```

`Rheo.ensure_indexes(rheo: MyRheo)` also creates them idempotently on boot. Pass
`notify: true` for a PostgreSQL `NOTIFY rheo_events` wakeup hint, or
`prefix: "rheo"` to keep the tables in their own schema. On PostgreSQL,
`metadata` and `payload` are `jsonb`, so the event log stays queryable in plain
SQL. See [ADR 017](https://hexdocs.pm/rheo/017-ecto-backend.html).

Define a consumer — handlers implement `handle_event/2` and return an outcome;
the local `Rheo.Group` started by the child spec owns fetch, concurrency, lease
renewal, and settlement:

```elixir
defmodule MyApp.RiskConsumer do
  use Rheo.Consumer,
    stream: "market-events",
    group: "risk",
    concurrency: 8,
    max_demand: 100

  @impl true
  def handle_event(event, _context) do
    case Risk.process(event) do
      :ok ->
        :ack

      {:temporary_error, reason} ->
        {:retry, reason}

      {:permanent_error, reason} ->
        {:reject, reason}
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

Interactive walkthroughs (open from a clone in
[Livebook](https://livebook.dev)):

| Notebook | Focus |
|---|---|
| [Index](https://github.com/thanos/rheo/blob/main/notebooks/rheo_demo.livemd) | Links to all demos ([HexDocs](https://hexdocs.pm/rheo/rheo_demo.html)) |
| [Quickstart](https://github.com/thanos/rheo/blob/main/notebooks/quickstart.livemd) | ETS publish / fetch / Consumer |
| [Concepts](https://github.com/thanos/rheo/blob/main/notebooks/concepts.livemd) | Leases, search, replay, partitions |
| [Ops](https://github.com/thanos/rheo/blob/main/notebooks/ops.livemd) | Inventory, lag, group health, dead letters (v0.10) |
| [LiveDashboard](https://github.com/thanos/rheo/blob/main/notebooks/live_dashboard.livemd) | Control panel + real Rheo LiveDashboard via Phoenix Playground |
| [Pipelines](https://github.com/thanos/rheo/blob/main/notebooks/pipelines.livemd) | GenStage, Flow, Broadway |
| [Backends](https://github.com/thanos/rheo/blob/main/notebooks/backends.livemd) | ETS, Mongo, SQLite, PostgreSQL |

### Ops control & LiveDashboard

Playground demo (`iex examples/live_dashboard_ops.exs` or the LiveDashboard
notebook): toggle a publisher and consumers, watch the stream tail, then open
the optional LiveDashboard page.

<p align="center">
  <img src="https://raw.githubusercontent.com/thanos/rheo/main/docs/screenshots/Rheo-Screenshot-Example-Control.jpg" alt="Rheo ops control panel — publisher, consumers, stream tail" width="720" />
</p>

<p align="center">
  <img src="https://raw.githubusercontent.com/thanos/rheo/main/docs/screenshots/Rheo-Screenshot-LiveDashboard.jpg" alt="Rheo LiveDashboard — group health" width="720" />
</p>

Guide: [Ops and observability](https://hexdocs.pm/rheo/ops.html).

CLI demo: `mix rheo.demo` (ETS) or `RHEO_BACKEND=mongo mix rheo.demo`.

Upgrading:

- [0.9 → 0.10](https://hexdocs.pm/rheo/0-9-to-0-10.html) (ops surface)
- [0.8 → 0.9](https://hexdocs.pm/rheo/0-8-to-0-9.html) (Redis Streams backend)
- [0.7 → 0.8](https://hexdocs.pm/rheo/0-7-to-0-8.html) (architectural reset)
- [0.6 → 0.7](https://hexdocs.pm/rheo/0-6-to-0-7.html) (additive — Broadway/GenStage interop)
- [0.5 → 0.6](https://hexdocs.pm/rheo/0-5-to-0-6.html) (additive — Ecto SQL backend)
- [0.4 → 0.5](https://hexdocs.pm/rheo/0-4-to-0-5.html) (partitions, frontier, lag)
- [0.3 → 0.4](https://hexdocs.pm/rheo/0-3-to-0-4.html) (additive — search, replay, lineage)
- [0.1 → 0.2](https://hexdocs.pm/rheo/0-1-to-0-2.html) (breaking Group / Query changes)

## Documentation

Links below use [HexDocs](https://hexdocs.pm/rheo/) (and GitHub for the Livebook
source). Relative `docs/…` paths break on [hex.pm](https://hex.pm/packages/rheo)
because those files are not in the Hex tarball.

**Guides**

- Introduction: [Quick Start](https://hexdocs.pm/rheo/quick-start.html) ·
  [Configuration](https://hexdocs.pm/rheo/configuration.html) ·
  [Consumer Groups](https://hexdocs.pm/rheo/consumer-groups.html) ·
  [Enqueuing](https://hexdocs.pm/rheo/enqueuing.html) ·
  [Dequeuing](https://hexdocs.pm/rheo/dequeuing.html)
- Advanced: [Replay](https://hexdocs.pm/rheo/replay.html) ·
  [Querying](https://hexdocs.pm/rheo/querying.html) ·
  [Partitions and lag](https://hexdocs.pm/rheo/partitions-and-lag.html) ·
  [Ops](https://hexdocs.pm/rheo/ops.html) ·
  [Broadway](https://hexdocs.pm/rheo/broadway.html) ·
  [GenStage](https://hexdocs.pm/rheo/genstage.html) ·
  [Building your own backend](https://hexdocs.pm/rheo/building-your-own-backend.html)
- Cookbook: [ETS](https://hexdocs.pm/rheo/ets.html) ·
  [Mongo](https://hexdocs.pm/rheo/mongo.html) ·
  [Using Ecto](https://hexdocs.pm/rheo/using-ecto.html)
- [Livebook demos](https://github.com/thanos/rheo/blob/main/notebooks/rheo_demo.livemd) ([HexDocs index](https://hexdocs.pm/rheo/rheo_demo.html)) — Quickstart, Concepts, Ops, LiveDashboard, Pipelines, Backends
- [Changelog](https://hexdocs.pm/rheo/changelog.html)

**Migrating from previous versions**

- [0.7 → 0.8](https://hexdocs.pm/rheo/0-7-to-0-8.html) · [0.6 → 0.7](https://hexdocs.pm/rheo/0-6-to-0-7.html) · [0.5 → 0.6](https://hexdocs.pm/rheo/0-5-to-0-6.html) · [0.4 → 0.5](https://hexdocs.pm/rheo/0-4-to-0-5.html)
- [0.3 → 0.4](https://hexdocs.pm/rheo/0-3-to-0-4.html) · [0.1 → 0.2](https://hexdocs.pm/rheo/0-1-to-0-2.html)

**Design**

- Architecture: [Architecture](https://hexdocs.pm/rheo/architecture.html) ·
  [Diagrams](https://hexdocs.pm/rheo/diagrams.html) ·
  [Roadmap](https://hexdocs.pm/rheo/roadmap.html)
- [ADRs](https://hexdocs.pm/rheo/adr.html) · [Tutorials index](https://hexdocs.pm/rheo/tutorials.html)
- [Article 12: ACKs Are Not a Cursor](https://hexdocs.pm/rheo/12-acks-are-not-a-cursor.html)
- [Article 13: One Consumer API, PostgreSQL and SQLite](https://hexdocs.pm/rheo/13-one-consumer-api-postgresql-and-sqlite.html)
- [Article 14: Rheo Is Not Broadway — It Feeds Broadway](https://hexdocs.pm/rheo/14-rheo-is-not-broadway-it-feeds-broadway.html)
- [Article 15: Breaking Rheo Before Anyone Depends on the Wrong Abstraction](https://hexdocs.pm/rheo/15-breaking-rheo-before-anyone-depends-on-the-wrong-abstraction.html)
- [Article 16: Rheo on Redis Streams — Portable Sequence, Native PEL](https://hexdocs.pm/rheo/16-rheo-on-redis-streams-portable-sequence-native-pel.html)
- [ADR 017: Ecto SQL backend](https://hexdocs.pm/rheo/017-ecto-backend.html)
- [ADR 018: GenStage / Broadway interop](https://hexdocs.pm/rheo/018-broadway-genstage-interop.html)

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
  after_sequence: 100,
  order_by: [sequence: :desc],
  limit: 50
)

{:ok, page} = Rheo.query_page("market-events", type: "curve_update", limit: 100)
Rheo.stream_query("market-events", type: "curve_update", limit: 100) |> Enum.take(250)
```

### Replay without copying events

```elixir
# Safest: new group from a cursor
Rheo.create_group("market-events", "risk-replay", start_after: 1_000)

# Or reopen an existing group (duplicates expected)
Rheo.replay("market-events", "risk", from_sequence: 1_000)
Rheo.reset_group("market-events", "risk", confirm: true)
```

### Event lineage metadata

```elixir
meta =
  Rheo.Event.Lineage.put(%{},
    correlation_id: "trade-42",
    causation_id: "cmd-9",
    producer: "pricing-v3",
    schema: "curve_update",
    schema_version: "1"
  )

Rheo.append("market-events", %{type: "curve_update", currency: "EUR", metadata: meta})
Rheo.query("market-events", correlation_id: "trade-42")
```

### Partitions, frontier, and lag

Sequences are monotonic **per partition**. Append with a `:key` (or explicit
`:partition`); ordering across partitions is undefined. Progress is a contiguous
committed frontier — ACKs with holes do not advance lag (see
[Article 12](https://hexdocs.pm/rheo/12-acks-are-not-a-cursor.html)).

```elixir
Rheo.create_stream("market-events", partition_count: 4)
Rheo.create_group("market-events", "risk")

Rheo.append("market-events", %{type: "curve_update", key: "EUR-EURIBOR-6M", price: 2.9})

{:ok, lag} = Rheo.lag("market-events", "risk")
# lag.partitions[p] => %{frontier: …, high_watermark: …, lag: …}
# lag.lag => sum of per-partition lags

# Static ownership (no automatic rebalance):
{MyApp.RiskConsumer, partitions: [0, 1], concurrency: 4}

Rheo.replay("market-events", "risk", from_sequence: 0, partition: 1)
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

### Broadway pipeline (or plain GenStage)

`Rheo.Producer` is a GenStage producer that turns demand into `Rheo.fetch/3` and
emits `%Rheo.Lease{}`. Under Broadway, `Rheo.Broadway.transform/2` wraps each
lease into a `%Broadway.Message{}` whose acknowledger settles it — ACK on
success, NACK (or reject) on failure.

```elixir
defmodule MyApp.RiskBroadway do
  use Broadway

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module:
          {Rheo.Producer,
           rheo: MyRheo, stream: "market-events", group: "risk", max_demand: 50},
        transformer: {Rheo.Broadway, :transform, []},
        concurrency: 1
      ],
      processors: [default: [concurrency: 8]]
    )
  end

  @impl true
  def handle_message(_processor, message, _context) do
    case Risk.process(message.data) do
      :ok -> message
      {:error, reason} -> Broadway.Message.failed(message, reason)
    end
  end
end
```

`message.data` is the `%Rheo.Event{}`; `message.metadata` carries `:lease`,
`:stream`, `:group`, `:partition`, and `:attempt`. `:max_demand` bounds unsettled
**leases**, while Broadway's `:concurrency` bounds pipeline work — they are
separate knobs.

Pick one surface per `{rheo, stream, group}`: `Rheo.Consumer` for the OTP handler
API, `Rheo.Producer` when you want Broadway's batching, rate limiting, or
fan-out. Plain GenStage consumers handle leases directly and settle with
`Rheo.Producer.ack/3`, `nack/4`, or `reject/4`. See
[Article 14](https://hexdocs.pm/rheo/14-rheo-is-not-broadway-it-feeds-broadway.html)
and [ADR 018](https://hexdocs.pm/rheo/018-broadway-genstage-interop.html).

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
| **0.2.0** | `Rheo.Group` runtime, real concurrency, lease renewal, multi-instance handles, portable `Rheo.Query`, persistence-error semantics |
| **0.3.0** | `Rheo.Backend.ETS`, capabilities, backend conformance suite, Docker-free demo |
| **0.4.0** | Search pagination/streaming, replay/reset, event lineage conventions |
| **0.4.1** | Hex README links point at HexDocs / GitHub |
| **0.5.0** | Partitions, key routing, contiguous ACK frontier, lag |
| **0.6.0** | Ecto SQL backend: PostgreSQL + SQLite on a host-owned repo |
| **0.7.0** | GenStage/Broadway interop: `Rheo.Producer`, lease-aware acknowledger |
| **0.7.1** | HexDocs Guides + Mermaid; Livebook Broadway section |
| **0.8.0** | Architectural reset: read-only handler context, single group owner, lease receipts, typed capabilities, settlement vocabulary, Redis/Flow readiness |
| **0.9.0** | Redis Streams native backend (optional `redix`); wakeup contract |
| **0.10.0** (current) | Ops surface: DLQ inspect, inventory, group health, optional LiveDashboard, Mix tasks |
| **0.11.0** | Mnesia / BEAM-native distributed backend |
| **0.12.0** | API freeze candidate |
| **1.0.0** | Stable public API (SemVer for `Rheo` / `Rheo.Consumer` / `Rheo.Backend`) |

Still out of scope through 1.0 unless demand forces it: standalone Rheo server,
exactly-once claims, K8s operator, auth frameworks, multi-tenancy. Details in
the [roadmap](https://hexdocs.pm/rheo/roadmap.html).

## License

MIT — see [LICENSE](https://github.com/thanos/rheo/blob/main/LICENSE).

## Building and developing the library

For contributors working on Rheo itself (not application consumers):

```bash
mix deps.get
mix test
mix rheo.demo
# Mongo demo:
docker compose up -d && RHEO_BACKEND=mongo mix rheo.demo
```

Quality gates (`mix ci` runs them all):

```bash
mix format --check-formatted
mix compile --warnings-as-errors
mix credo --strict
mix dialyzer
mix coveralls
mix docs --warnings-as-errors
mix core.check   # builds a project on rheo with no optional integration
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

The Ecto backend contract suite runs on SQLite on every `mix test` (a throwaway
temp database, no service needed). PostgreSQL cases are tagged `:ecto_postgres`
and run only when a URL is set:

```bash
docker compose up -d postgres
RHEO_POSTGRES_URL=ecto://postgres:postgres@localhost:5432/rheo_test mix test
```

Livebook from a clone:

```bash
livebook server notebooks/
# or: livebook server notebooks/quickstart.livemd
```

CI tests **Erlang/OTP 27–29** × **Elixir 1.17–1.20** (excluding unsupported
pairs). Format, Credo, Dialyzer, and Coveralls run on Elixir 1.20.2 / OTP 29.

Coverage publishes to [Coveralls](https://coveralls.io/github/thanos/rheo) from
CI via `mix coveralls.github`. Hex releases publish from annotated tags (`v*`);
set repository secrets `COVERALLS_REPO_TOKEN` and `HEX_API_KEY`.
