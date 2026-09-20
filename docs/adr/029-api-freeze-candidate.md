# ADR 029 — API freeze candidate

## Status

Accepted (v0.12.0)

## Context

v0.1–v0.11 proved the product: immutable logs, leases, partitions, frontiers,
replay, five shipping backends, GenStage/Broadway, and an ops inspect surface.
v0.8 corrected the abstractions; v0.9–v0.11 filled Redis, ops, and Mnesia.

Before 1.0 can SemVer `Rheo` / `Rheo.Consumer` / `Rheo.Backend`, hosts and
backend authors need a clear line between what is frozen, what is optional
ops, and what is internal. HexDocs should publish that line — not every
compiled module.

## Decision

1. **v0.12 is an API freeze candidate**, not 1.0. No new backend, no multi-node
   Mnesia, no Flow package, no purge/control plane.
2. **Keep 0.11.1 caller-visible behaviour.** No Consumer / Event / Query /
   Backend-callback meaning changes. Legacy `%{after_sequence: n}` page cursors
   and the `{Rheo, url: …}` Mongo shorthand stay.
3. **HexDocs is the freeze.** `groups_for_modules` + `filter_modules` publish
   only the modules hosts and adapter authors may depend on. Internal modules
   stay `@moduledoc false` or are filtered out.
4. **`test/support/backend_contract.ex` is the executable freeze** for adapters
   (ADR 012 / ADR 024), including an unavailable-handle group.
5. **Optional ops** (LiveDashboard, Mix inspect, `Rheo.Telemetry.Metrics`) may
   evolve in 0.12.x without a Consumer / Event / Query break.
6. **1.0** starts SemVer for the frozen surface. 0.12.x after this release is
   correctness-only.

## Module inventory

| Module | Tier | Notes |
|---|---|---|
| `Rheo` | **Frozen** — Facade | Public API entry |
| `Rheo.Consumer` | **Frozen** — Consume | Handler child spec |
| `Rheo.Group` | **Frozen** — Consume | Hosts may start / `drain/2` |
| `Rheo.GroupSupervisor` | **Frozen** — Consume | `start_group/2` |
| `Rheo.Producer` | **Frozen** — Consume | GenStage (optional dep) |
| `Rheo.Broadway` | **Frozen** — Consume | Optional dep |
| `Rheo.Broadway.Acknowledger` | **Frozen** — Consume | Optional dep |
| `Rheo.Event` | **Frozen** — Values | |
| `Rheo.Event.Lineage` | **Frozen** — Values | |
| `Rheo.Lease` | **Frozen** — Values | |
| `Rheo.Query` | **Frozen** — Values | |
| `Rheo.Page` | **Frozen** — Values | |
| `Rheo.Lag` | **Frozen** — Values | |
| `Rheo.DeadLetter` | **Frozen** — Values | |
| `Rheo.GroupInfo` | **Frozen** — Values | |
| `Rheo.Settle` | **Frozen** — Values | |
| `Rheo.Partition` | **Frozen** — Values | |
| `Rheo.Backend` | **Frozen** — Backend | Behaviour |
| `Rheo.Backend.Capabilities` | **Frozen** — Backend | |
| `Rheo.Backend.Wakeup` | **Frozen** — Backend | Optional `wait/2` |
| `Rheo.Backend.ETS` | **Frozen** — Backend | Shipping adapter |
| `Rheo.Backend.Mnesia` | **Frozen** — Backend | Shipping adapter |
| `Rheo.Backend.Mongo` | **Frozen** — Backend | Optional dep |
| `Rheo.Backend.Ecto` | **Frozen** — Backend | Optional dep |
| `Rheo.Backend.Redis` | **Frozen** — Backend | Optional dep |
| `Rheo.Inflight` | **Frozen** — Runtime | Shared by Group/Producer |
| `Rheo.Backoff` | **Frozen** — Runtime | Shared by Group/Producer |
| `Rheo.Clock` | **Frozen** — Runtime | |
| `Rheo.Clock.Frozen` | **Frozen** — Runtime | Tests / demos |
| `Rheo.Clock.System` | **Frozen** — Runtime | Default clock |
| `Rheo.Id` | **Frozen** — Runtime | Opaque id helper |
| `Rheo.Telemetry` | **Frozen** — Runtime | Event catalogue |
| `Rheo.Application` | **Frozen** — Runtime | OTP app / Registry |
| `Rheo.LiveDashboard.Page` | **Optional** — Ops | Not SemVer-core |
| `Rheo.Telemetry.Metrics` | **Optional** — Ops | Not SemVer-core |
| Mix tasks (`rheo.*`) | **Optional** — Ops | Published under Ops; not SemVer-core |
| `Mix.Tasks.Rheo.InspectOpts` | **Internal** | `@moduledoc false` helper |
| `Rheo.Names` | **Internal** | `@moduledoc false` |
| `Rheo.Instance` | **Internal** | `@moduledoc false` |
| `Rheo.Backend.TableEngine` | **Internal** | `@moduledoc false` |
| `Rheo.Backend.Table.Store` / `Table.ETS` / `Table.Mnesia` | **Internal** | `@moduledoc false` |
| `Rheo.Backend.Mnesia.Store` | **Internal** | `@moduledoc false` |
| `Rheo.Backend.Mnesia.Store.Mnesia` | **Internal** | OTP delegate |
| `*.Client` / `*.Codec` / `*.Keys` / `Ecto.Server` / `Migrations*` | **Internal** | Filtered from HexDocs |

## Non-goals (v0.12)

- Multi-node Mnesia `disc_copies`
- First-class Flow Hex integration
- Cluster coordination, auto-rebalance, exactly-once
- New inspect/mutate APIs (leased listing, purge, stream delete)
- Hiding `Inflight` / `Backoff` / `Lag.build/3`

## Consequences

- Migration `0.11.1-to-0.12` documents **no** intended breaks for hosts on
  0.11.1 APIs.
- HexDocs sidebar groups (Facade / Consume / Values / Backend / Runtime / Ops)
  are the published freeze.
- After 1.0, changing a frozen module's public contract requires a major
  version; optional ops may still move in minors.

## Related

ADR 006, 012, 019, 020, 023, 024, 027.
