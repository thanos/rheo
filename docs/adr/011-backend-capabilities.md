# ADR 011 — Backend capabilities

## Status

Accepted (v0.3.0)

## Context

Backends differ in durability, indexing, and wakeup mechanisms. Without an
explicit capability map, callers and conformance tests either assume Mongo
guarantees or silently skip correctness.

## Decision

1. Every `Rheo.Backend` implements `capabilities/0` returning a map of known
   flags (at minimum: `durable`, `distributed`, `atomic_compare_and_set`,
   `notifications`, `change_feed`, `secondary_indexes`, `batch_writes`,
   `ordered_range_scan`).
2. Capabilities document guarantees and gate optional conformance cases.
   They must **not** make lease fencing or at-least-once semantics optional.
3. Mongo reports durable + secondary indexes; ETS reports ephemeral (not durable).

## Alternatives

- Infer capabilities from module attributes only
- Capability structs with versioned schemas (deferred)

## Consequences

- Conformance and docs can describe backends honestly
- Future backends (Ecto, Mnesia) extend the same map
- Unsupported product features are skipped by capability, not omitted quietly
