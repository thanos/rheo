# ADR 030 — SemVer 1.0

## Status

Accepted (v1.0.0)

## Context

ADR 029 froze the host- and adapter-facing surface in v0.12.0. HexDocs
publishes Facade / Consume / Values / Backend / Runtime / Ops; the backend
conformance suite is the executable adapter contract. Hosts need a clear
SemVer baseline: what requires a major bump, what may move in a minor, and
what stays patch-only.

## Decision

1. **v1.0.0 starts SemVer** for the frozen inventory in ADR 029
   (`Rheo`, `Rheo.Consumer`, `Rheo.Backend`, values, shipping adapters, and the
   shared Runtime modules listed there).
2. **No meaning changes from 0.12.0.** Consumer outcomes, Event / Query /
   Lease / Settle shapes, and required Backend callbacks keep 0.12 semantics.
3. **Compatibility kept:** legacy `%{after_sequence: n}` page cursors and the
   `{Rheo, url: …}` Mongo shorthand remain supported.
4. **Version policy after 1.0:**
   - **Major** — change a frozen module's public contract or portable error
     atoms in a breaking way.
   - **Minor** — additive APIs on the frozen surface; optional ops evolution
     (LiveDashboard, Mix inspect, `Rheo.Telemetry.Metrics`).
   - **Patch** — correctness, docs, and internal (`@moduledoc false` /
     filtered) modules.
5. **Still deferred** (not implied by 1.0): multi-node Mnesia `disc_copies`,
   first-class Flow package, Rheo cluster coordination, auto-rebalance,
   exactly-once.

## Consequences

- Migration `0.12-to-1.0` documents **no** intended breaks; hosts bump
  `{:rheo, "~> 1.0"}`.
- Optional ops remain **not SemVer-core** (ADR 029 / 027): they may change in
  1.x minors without a Consumer / Event / Query break.
- After 1.0, architectural resets require a new major (see ADR 019 for the
  pre-1.0 precedent).

## Related

ADR 006, 019, 020, 024, 027, 029.
