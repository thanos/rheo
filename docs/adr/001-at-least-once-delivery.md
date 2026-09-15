# ADR 001: At-least-once delivery

## Context

Consumer-group systems must choose a delivery guarantee. Exactly-once processing
requires end-to-end idempotency and transactional coordination that databases and
handlers rarely provide for free.

## Decision

Rheo provides **at-least-once** delivery with leases and explicit acknowledgement.
Duplicates are possible after crashes or lease expiry. Event IDs are stable so
applications can implement idempotency.

## Alternatives

- Exactly-once claims (deferred; requires stronger coordination)
- At-most-once (rejects the recovery requirement)

## Consequences

- Handlers must tolerate duplicates
- Lease fencing is mandatory to prevent stale ACKs
- Documentation must state the guarantee prominently
