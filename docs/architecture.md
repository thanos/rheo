# Architecture

Rheo **v0.6.0** is an embedded Elixir/OTP library. Durable truth lives in the
backend (**MongoDB**, **PostgreSQL/SQLite via Ecto**, or **ETS** for ephemeral
use). OTP owns process lifecycle and concurrency, not consumer-group correctness.

```text
Application Supervision Tree
        |
        +-- MyApp.Repo                    (when using Ecto — host-owned)
        +-- Rheo (named instance Supervisor)
        |     +-- Registry
        |     +-- Backend handle (Mongo | Ecto.Server | ETS)
        |     +-- Rheo.Instance
        |     +-- Task.Supervisor
        |     +-- Rheo.GroupSupervisor
        |           +-- Rheo.Group {stream, "risk"}
        |           |     +-- worker Tasks
        |           +-- Rheo.Group {stream, "surveillance"}
        |
        +-- RiskConsumer (bridge → Group)
        +-- SurveillanceConsumer (bridge → Group)
```

## Core invariant

| Concept | Storage | Mutability |
|---|---|---|
| Event | `events` / `rheo_events` | Immutable |
| Per-partition sequence | stream counters / `rheo_stream_sequences` | Monotonic per partition |
| Materialization cursors | `groups.cursors` | How far deliveries were offered |
| Committed frontiers | `groups.frontiers` | Contiguous terminal progress |
| Lease / ACK / DLQ | `deliveries` / `rheo_deliveries` | Mutable per group+event |

Consumption never deletes events. An event may be processed independently by many
groups. Ordering is guaranteed **within a partition** only.

## Delivery

At-least-once with lease fencing:

1. `fetch` claims work with a unique `lease_id`
2. `ack` succeeds only if the lease token still matches
3. Inflight leases are renewed by `Rheo.Group` (~half `lease_ms`)
4. Expired leases become eligible for redelivery
5. Stale consumers cannot ACK a newer lease
6. Contiguous frontier advances on ACK/reject; holes block `Rheo.lag/3`

## Demand and concurrency

`Rheo.Group` bounds outstanding leases with `:max_demand` and parallel handlers
with `:concurrency`. Durable ACK state stays in the backend. Groups may take a
static `:partitions` assignment (`:all` or a list); automatic rebalancing is
deferred.

## Backend boundary

`Rheo.Backend` defines operations over an opaque `handle`, plus
`capabilities/0`. Implementations: `Rheo.Backend.Mongo`, `Rheo.Backend.Ecto`
(SQL — Postgres/SQLite), and `Rheo.Backend.ETS`. Queries use portable
`%Rheo.Query{}` with pagination (`query_page` / `stream_query`). Replay/reset
re-drive per-group deliveries without copying events (ADR 015). Partitions use
per-partition sequences and a contiguous ACK frontier (ADR 016). Ecto uses a
host-owned Repo (ADR 017).

## Try it

- CLI: `mix rheo.demo`
- Interactive: [Livebook demo](https://hexdocs.pm/rheo/rheo_demo.html)
  ([source](https://github.com/thanos/rheo/blob/main/notebooks/rheo_demo.livemd))
- Migrations: [0.5 → 0.6](https://hexdocs.pm/rheo/0-5-to-0-6.html) ·
  [0.4 → 0.5](https://hexdocs.pm/rheo/0-4-to-0-5.html) ·
  [0.3 → 0.4](https://hexdocs.pm/rheo/0-3-to-0-4.html) ·
  [0.1 → 0.2](https://hexdocs.pm/rheo/0-1-to-0-2.html)
- Changelog: [CHANGELOG](https://hexdocs.pm/rheo/changelog.html)
- Roadmap: [roadmap](https://hexdocs.pm/rheo/roadmap.html)
