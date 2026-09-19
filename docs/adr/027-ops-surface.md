# ADR 027: Ops surface for an embedded Rheo

## Status

Accepted (v0.10.0)

## Context

After Redis (v0.9), hosts need day-two visibility: which streams exist, how far
a group has progressed, and what sits in the dead-letter path. NATS CLI
JetStream (`stream ls/info`, `consumer info`, DLQ follow-up, `bench`) is a useful
checklist, but Rheo is an **embedded OTP library** (ADR 006), not a broker with
a standalone control plane.

ADR 006 said “no separate ops unit.” That still holds: ops must not invent a
Rheo server, second settle path, or destructive admin API. It **does** allow
portable inspect APIs, documented telemetry, Mix tasks, and an optional
LiveDashboard page in the same Hex package (ADR 020).

## Decision

### Layering

1. **Core inspect APIs** (no Phoenix): `Rheo.list_streams/1`,
   `Rheo.list_groups/2`, `Rheo.dead_letters/3`, `Rheo.group_info/3` (plus
   existing `Rheo.lag/3`).
2. **Backend callbacks** (optional on the behaviour so custom backends remain
   loadable): `list_streams/2`, `list_groups/3`, `dead_letters/4`,
   `group_info/4`. When absent, public APIs return `{:error, :unsupported}`.
3. **Telemetry** remains the metrics source of truth; ship optional
   `Rheo.Telemetry.Metrics` definitions for hosts that depend on
   `telemetry_metrics`.
4. **LiveDashboard** is an optional dep + `Code.ensure_loaded?` modules under
   `Rheo.LiveDashboard` — thin UI over inspect APIs, not a control plane.
5. **Mix tasks** (`mix rheo.streams`, `mix rheo.lag`, …) play the NATS CLI role
   for BEAM hosts.

### Read-only rule

Inspect APIs never settle leases. Mutation stays on `ack` / `nack` / `reject` /
`replay` / `reset_group` (`confirm: true`). Dashboard and Mix must not invent a
second settle path.

### Dead-letter shapes

| Backend | Storage | List path |
|---|---|---|
| ETS / Mongo / Ecto | Delivery row `status = rejected` | Scan / query rejected rows |
| Redis | Separate `…:dlq:{stream}:{group}` STREAM | `XRANGE` / `XLEN` |

Portable `%Rheo.DeadLetter{}` normalizes both shapes.

### Out of scope (v0.10)

Purge / delete stream, backup/restore, ad-hoc “consumer next” CLI, cluster/RAFT
reports, AuthN/AuthZ, standalone Rheo server, separate `rheo_dashboard` package.

### Mnesia

Deferred to **0.11** (durable ETS-shaped store; not this release).

## Consequences

- Shipping backends implement the four ops callbacks.
- Custom backends that omit them get honest `:unsupported`.
- README / roadmap: Ops is 0.10; Mnesia is 0.11.
- `mix core.check` asserts LiveDashboard modules absent without the optional dep.

## Alternatives

- Separate ops Hex package — rejected (ADR 020 cost).
- Enrich only telemetry, no list APIs — rejected; hosts need pull-based inspect.
- Destructive admin APIs matching NATS purge/backup — deferred with retention.
