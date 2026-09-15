# ADR 002: Events immutable, consumer state separate

## Context

Putting a global `processed` flag on an event breaks independent consumer groups
and conflates durable history with consumption progress.

## Decision

Events in `events` are immutable. Leases, attempts, ACKs, retries, and
dead-letters live in `deliveries` keyed by `(stream, group, event_id)`.

## Alternatives

- Mutate events in place (rejected)
- Copy events into per-group queues (storage amplification)

## Consequences

- Historical query remains valid after consumption
- Groups progress independently
- Schema has more collections but clearer semantics
