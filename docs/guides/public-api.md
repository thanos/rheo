# Public API (v1.0)

v1.0 is the SemVer baseline for the host- and adapter-facing surface frozen in
v0.12 (ADR 029 / ADR 030). This page is the short map; HexDocs module groups
are authoritative.

Breaking changes to modules listed under **Facade / Consume / Values /
Backend / Runtime** require a new major version. Optional ops may still evolve
in 1.x minors.

## Facade

`Rheo` — create streams/groups, append, read/query, fetch/settle, replay/reset,
lag, ops inspect (`list_streams`, `list_groups`, `dead_letters`, `group_info`),
`ping`, `ensure_indexes`.

Pass `:rheo` for a named instance (default `Rheo`).

## Consume

| Module | Role |
|---|---|
| `Rheo.Consumer` | `use` → child spec for a local `Rheo.Group` |
| `Rheo.Group` | Demand, fetch, renew, drain |
| `Rheo.GroupSupervisor` | Dynamic `start_group/2` |
| `Rheo.Producer` | GenStage producer (optional `gen_stage`) |
| `Rheo.Broadway` / `Acknowledger` | Broadway wiring (optional `broadway`) |

One Group (or Producer) per `{rheo, stream, group}` per node.

## Values

`Event`, `Event.Lineage`, `Lease`, `Query`, `Page`, `Lag`, `DeadLetter`,
`GroupInfo`, `Settle`, `Partition`.

## Backend authors

Implement `Rheo.Backend`. Declare `Rheo.Backend.Capabilities`. Optional ops
callbacks may return `{:error, :unsupported}`. Optional wakeup via
`Rheo.Backend.Wakeup`. Shipping adapters: ETS, Mnesia, Mongo, Ecto, Redis.

See [Building your own backend](building-your-own-backend.html).

## Runtime (shared, public)

`Inflight`, `Backoff`, `Clock` / `Clock.Frozen` / `Clock.System`, `Id`,
`Telemetry`, `Application`.

## Optional ops (not SemVer-core)

Mix inspect tasks, `Rheo.LiveDashboard.Page`, `Rheo.Telemetry.Metrics`. These
may evolve in 1.x minors without a Consumer / Event / Query break.

## Error atoms

Portable reasons returned by `Rheo` / backends (also in
[building-your-own-backend](building-your-own-backend.html)):

| Reason | Meaning |
|---|---|
| `:stream_not_found` | Stream missing (looked up before group) |
| `:group_not_found` | Stream exists; group does not |
| `:already_exists` | Duplicate stream or group |
| `:stale_lease` | Settle lost the fencing token |
| `:receipt_mismatch` | Native receipt no longer matches |
| `:cursor_not_found` | Dead-letter / page `:after` id absent |
| `:already_draining` | Second `drain/2` while one is pending |
| `:unsupported` | Optional ops callback not implemented |
| `:backend_unavailable` | Backend unreachable / process down |
| `:confirm_required` | `reset_group` without `confirm: true` |
| `{:ambiguous, cause}` | Write may have committed (e.g. timeout) |
| `{:failed, cause}` | Definite failure |

`Rheo.Settle.classify/1` maps settle errors for Group / Producer policy.

## Kept for compatibility

- Legacy page cursor `%{after_sequence: n}` (global lower bound)
- `{Rheo, url: "mongodb://…"}` when Mongo is available
