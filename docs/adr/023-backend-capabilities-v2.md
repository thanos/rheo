# ADR 023: Backend capabilities v2

## Status

Accepted (v0.8.0). Supersedes [ADR 011](011-backend-capabilities.html).

## Context

ADR 011 introduced a flat boolean capability map. Redis-shaped backends need
flags like `native_consumer_groups` and `blocking_reads` that are not simply
Mongo feature toggles. Mixing guarantees with mechanisms made conformance
gating ambiguous.

## Decision

1. `Rheo.Backend.Capabilities` is a validated struct with two maps:
   **guarantees** (`durable`, `distributed`, `at_least_once`, `lease_fencing`,
   `partitions`, `contiguous_frontier`, `replay`) and **mechanisms**
   (`atomic_compare_and_set`, `ordered_range_scan`, `secondary_indexes`,
   `batch_writes`, `notifications`, `change_feed`, `native_consumer_groups`,
   `native_pending_list`, `native_reclaim`, `blocking_reads`,
   `native_group_lag`). Every key is present; unknown keys and non-boolean
   values raise.
2. `at_least_once` and `lease_fencing` are Rheo invariants: they default to
   `true` and cannot be declared `false`.
3. `c:Rheo.Backend.capabilities/0` returns the struct. There is no legacy map.
4. The conformance suite gates optional cases on guarantees only; correctness
   cases always run.

## Consequences

- ADR 011 remains historical; new backends use ADR 023.
- Future Redis reports `native_*` mechanisms without claiming false Mongo
  equivalence.

## Related

Supersedes the *shape* guidance of ADR 011 (not the existence of capabilities).
