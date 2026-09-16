# ADR 008: MongoDB schema and indexes

## Context

Lease acquisition, sequence allocation, and historical queries need a minimal but
correct schema.

## Decision

Collections:

- `streams` — `{name, partition_count, next_sequences, created_at}`  
  (`next_sequences` is a map of partition → last allocated sequence; legacy
  `next_sequence` is treated as partition `0` only)
- `events` — immutable event documents keyed by `(stream, partition, sequence)`
- `groups` — `{stream, name, cursors, frontiers, max_attempts, created_at}`  
  (`cursors` = materialization next-sequence per partition; `frontiers` =
  contiguous committed sequence per partition; legacy `next_sequence` maps to
  partition `0`)
- `deliveries` — per `(stream, group, event_id)` lease/ACK/DLQ state including
  `partition` and `sequence`

Sequence allocation uses atomic `$inc` on `next_sequences.<partition>`.
Fetch materializes delivery rows lazily per partition, then claims with
conditional updates. ACK/reject advance the contiguous frontier (ADR 016).

Indexes support stream/partition/sequence uniqueness, claim lookups,
`(stream, group, partition, sequence)` frontier walks, and common query fields
(`type`, `key`, `timestamp`, currency/curve, correlation id).

## Alternatives

- Embed consumer state on events (rejected by ADR 002)
- Separate retry queue collection (unnecessary for MVP)

## Consequences

- Correct competing-consumer semantics with Mongo atomic ops
- Queryability is a first-class indexed workload
- Multi-partition ordering is per partition only (ADR 016)
