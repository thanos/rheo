# Architecture

Rheo **v0.4.0** is an embedded Elixir/OTP library. Durable truth lives in the
backend (MongoDB for production; ETS for ephemeral/zero-infra use). OTP owns
process lifecycle and concurrency, not consumer-group correctness.

```text
Application Supervision Tree
        |
        +-- Rheo (named instance Supervisor)
        |     +-- Registry
        |     +-- Backend handle (e.g. Mongo)
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
| Event | `events` | Immutable |
| Stream sequence | `streams` | Monotonic counter |
| Consumer group | `groups` | Cursor / policy |
| Lease / ACK / DLQ | `deliveries` | Mutable per group+event |

Consumption never deletes events. An event may be processed independently by many
groups.

## Delivery

At-least-once with lease fencing:

1. `fetch` claims work with a unique `lease_id`
2. `ack` succeeds only if the lease token still matches
3. Inflight leases are renewed by `Rheo.Group` (~half `lease_ms`)
4. Expired leases become eligible for redelivery
5. Stale consumers cannot ACK a newer lease

## Demand and concurrency

`Rheo.Group` bounds outstanding leases with `:max_demand` and parallel handlers
with `:concurrency`. Durable ACK state stays in the backend.

## Backend boundary

`Rheo.Backend` defines operations over an opaque `handle`, plus
`capabilities/0`. Implementations: `Rheo.Backend.Mongo` and `Rheo.Backend.ETS`.
Queries use portable `%Rheo.Query{}` with pagination (`query_page` /
`stream_query`). Replay/reset re-drive per-group deliveries without copying
events (ADR 015).

## Try it

- CLI: `mix rheo.demo`
- Interactive: [notebooks/rheo_demo.livemd](../notebooks/rheo_demo.livemd)
- Migrations: [0.3 → 0.4](migrations/0.3-to-0.4.md) · [0.1 → 0.2](migrations/0.1-to-0.2.md)
- Changelog: [CHANGELOG.md](../CHANGELOG.md)
- Roadmap: [roadmap.md](roadmap.md)
