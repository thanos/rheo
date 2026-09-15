# ADR 008: MongoDB schema and indexes

## Context

Lease acquisition, sequence allocation, and historical queries need a minimal but
correct schema.

## Decision

Collections:

- `streams` — `{name, partition_count, next_sequence}`
- `events` — immutable event documents
- `groups` — `{stream, name, next_sequence, max_attempts}`
- `deliveries` — per `(stream, group, event_id)` lease/ACK/DLQ state

Sequence allocation uses atomic `$inc` on the stream document.
Fetch materializes delivery rows lazily, then claims with conditional updates.

Indexes support stream/partition/sequence uniqueness, claim lookups, and common
query fields (`type`, `key`, `timestamp`, currency/curve, correlation id).

## Alternatives

- Embed consumer state on events (rejected by ADR 002)
- Separate retry queue collection (unnecessary for MVP)

## Consequences

- Correct competing-consumer semantics with Mongo atomic ops
- Queryability is a first-class indexed workload
