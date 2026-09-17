# ADR 023: Backend capabilities v2

## Status

Accepted (v0.8.0)

## Context

ADR 011 introduced a flat boolean capability map. Redis-shaped backends need
flags like `native_consumer_groups` and `blocking_reads` that are not simply
Mongo feature toggles. Mixing guarantees with mechanisms made conformance
gating ambiguous.

## Decision

1. Introduce `Rheo.Backend.Capabilities` with two maps: **guarantees** and
   **mechanisms**.
2. Always imply `at_least_once` and `lease_fencing` unless explicitly false
   (they are Rheo product invariants).
3. Keep `capabilities/0` on backends but return either the struct or a legacy
   flat map normalized via `Capabilities.normalize/1`.
4. Conformance gates on guarantees; mechanism flags skip optional cases only.

## Consequences

- ADR 011 remains historical; new backends use ADR 023.
- Future Redis reports `native_*` mechanisms without claiming false Mongo
  equivalence.

## Related

Supersedes the *shape* guidance of ADR 011 (not the existence of capabilities).
