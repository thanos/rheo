# Architecture

Rheo v0.8.0 is an embedded Elixir/OTP library. Durable truth lives in the
backend (MongoDB, PostgreSQL/SQLite via Ecto, or ETS for ephemeral use). OTP
owns process lifecycle and concurrency, not consumer-group correctness.

```text
Application Supervision Tree
        |
        +-- MyApp.Repo                    (when using Ecto — host-owned)
        +-- Rheo (named instance Supervisor)
        |     +-- Backend handle (Mongo | Ecto.Server | ETS)
        |     +-- Rheo.Instance           (backend module + handle)
        |     +-- Task.Supervisor         (handler tasks)
        |     +-- Rheo.GroupSupervisor    (dynamically started groups only)
        |
        +-- RiskConsumer          = Rheo.Group {stream, "risk"}
        |                               +-- handler Tasks
        +-- SurveillanceConsumer  = Rheo.Group {stream, "surveillance"}
```

`use Rheo.Consumer` produces a child spec for `Rheo.Group`, so the host
supervisor owns each local group runtime. One Group per `{rheo, stream, group}`
runs per node (ADR 022).

Broadway (or plain GenStage) is an alternative consumption surface for the same
durable group. `Rheo.Producer` replaces the Group's handler runtime rather than
wrapping it:

```text
        +-- MyApp.RiskBroadway (Broadway topology)
              +-- Rheo.Producer         (demand → fetch, renew inflight)
              +-- processors / batchers  (your work)
                    +-- Rheo.Broadway.Acknowledger → Producer.ack | nack | reject
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
groups. Ordering is guaranteed within a partition only.

## Delivery

At-least-once with lease fencing:

1. `fetch` claims work with a unique `lease_id` and an opaque `receipt`
2. `ack` succeeds only if the lease token (and receipt) still match
3. Inflight leases are renewed by the Group or Producer (about half `lease_ms`)
4. Expired leases become eligible for redelivery
5. Stale consumers cannot ACK a newer lease
6. Contiguous frontier advances on ACK/reject; holes block `Rheo.lag/3`

## Settlement

A handler can succeed and the ACK can still fail. `Rheo.Settle` classifies
every settle, renew, and fetch error into a portable vocabulary
(`:stale_lease`, `:receipt_mismatch`, `:backend_unavailable`,
`{:ambiguous, _}`, `{:failed, _}`, `{:invalid, _}`), and the runtime decides
from it: a lost lease is dropped, a definite failure is nacked for immediate
redelivery, an unavailable or ambiguous outcome is left to expire so fencing
protects a possibly-committed ACK. Telemetry carries the classified reason.

## Shared runtime pieces

`Rheo.Group` and `Rheo.Producer` are different processes with different
external contracts, but they share `Rheo.Inflight` (inflight set, capacity,
`renew_all/2`), `Rheo.Backoff` (fetch-failure schedule), and `Rheo.Settle`.
Draining in both is non-blocking: the caller is answered when inflight work
settles or the timeout fires, while renewals keep running.

## Demand and concurrency

`Rheo.Group` bounds outstanding leases with `:max_demand` and parallel handler
tasks with `:concurrency`. Groups may take a static `:partitions` assignment
(`:all` or a list); automatic rebalancing is deferred.

`Rheo.Producer` applies the same `:max_demand` bound to GenStage demand: it
fetches at most `min(demand, max_demand - inflight)` leases, renews what is
inflight, and releases entries through `Rheo.Producer.ack/3`, `nack/4`,
`reject/4` (or `confirm/2`). Pipeline concurrency belongs to Broadway
(ADR 018).

## Backend boundary

`Rheo.Backend` defines semantic operations over an opaque `handle` plus a
validated `Rheo.Backend.Capabilities` declaration (guarantees vs mechanisms,
ADR 023). Implementations: `Rheo.Backend.Mongo`, `Rheo.Backend.Ecto`
(PostgreSQL / SQLite), and `Rheo.Backend.ETS`; a native-stream test double
proves the contract does not assume delivery rows (ADR 024). Queries use
portable `%Rheo.Query{}` with pagination (`query_page` / `stream_query`).
Replay/reset re-drive per-group deliveries without copying events (ADR 015).
Partitions use per-partition sequences and a contiguous ACK frontier (ADR 016).
Ecto uses a host-owned Repo (ADR 017).

## Optional integrations

`rheo` is one package. `Rheo.Backend.Mongo`, `Rheo.Backend.Ecto`,
`Rheo.Producer`, and `Rheo.Broadway` are compiled only when the host lists
`mongodb_driver`, `ecto_sql`, `gen_stage`, or `broadway`; core depends on
`telemetry` and `jason` only. `mix core.check` builds a project on Rheo with
none of them (ADR 020).

## Try it

- CLI: `mix rheo.demo`
- Interactive: [Livebook demos](https://hexdocs.pm/rheo/rheo_demo.html)
  ([source index](https://github.com/thanos/rheo/blob/main/notebooks/rheo_demo.livemd))
- Migrations: [0.7 → 0.8](https://hexdocs.pm/rheo/0-7-to-0-8.html) ·
  [0.6 → 0.7](https://hexdocs.pm/rheo/0-6-to-0-7.html) ·
  [0.5 → 0.6](https://hexdocs.pm/rheo/0-5-to-0-6.html) ·
  [0.4 → 0.5](https://hexdocs.pm/rheo/0-4-to-0-5.html) ·
  [0.3 → 0.4](https://hexdocs.pm/rheo/0-3-to-0-4.html) ·
  [0.1 → 0.2](https://hexdocs.pm/rheo/0-1-to-0-2.html)
- Changelog: [CHANGELOG](https://hexdocs.pm/rheo/changelog.html)
- Roadmap: [roadmap](https://hexdocs.pm/rheo/roadmap.html)
