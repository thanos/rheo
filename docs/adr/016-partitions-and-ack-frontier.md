# ADR 016 — Partitions, ordering, and ACK frontier

## Status

Accepted (v0.5.0)

## Context

v0.4 treats `partition` as a field defaulting to `0` with a stream-global
sequence counter. Operators need real multi-partition streams, key routing,
ordered consume **within** a partition, and a contiguous committed frontier so
lag is meaningful. ACKs of later sequences must not advance past holes.

## Decision

1. **`partition_count`** on the stream is honored. Sequences are monotonic
   **per `(stream, partition)`**, stored as `next_sequences` on the stream doc
   (legacy single `next_sequence` is treated as partition `0` only).
2. **Routing:** `:erlang.phash2(key, partition_count)` when `:key` is set;
   explicit `:partition` overrides; otherwise partition `0`. Invalid partition
   → `{:error, :invalid_partition}`.
3. **No global ordering** across partitions.
4. **Materialization cursors** (`cursors`) are per-partition — how far deliveries
   have been offered. Distinct from the **committed frontier** (`frontiers`):
   highest `F` such that every sequence from the group start through `F` is
   terminal (`acked` or `rejected`). `:available` / `:leased` block advance.
5. **Lag** = high watermark − frontier per partition; aggregate lag is the
   **sum** of per-partition lags (`Rheo.lag/3`).
6. **Ownership groundwork:** Group/Consumer may pass static `:partitions`
   (`:all` or a list). No automatic rebalancing.
7. **Replay/reset** may be scoped with `:partition` / `:partitions`.

## Alternatives

- Stream-global sequence with partition as a tag only (rejected — breaks
  per-partition ordering)
- Frontier = max acked sequence ignoring holes (rejected — unsafe lag)

## Consequences

- Backends advertise `partitions: true` and `contiguous_frontier: true`
- Index deliveries for `(stream, group, partition, sequence)` walks
- Article 12 teaches the hole rule; migration 0.4→0.5 documents `partition_count > 1`
