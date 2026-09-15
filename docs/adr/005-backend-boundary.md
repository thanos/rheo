# ADR 005: Backend boundary

## Context

Future databases may back Rheo. A behaviour helps isolate Mongo details without
pretending all databases are identical.

## Decision

Define `Rheo.Backend` with callbacks Rheo uses today: stream/group lifecycle,
append/read/query, fetch/ack/retry/reject, ping, indexes. Implement only Mongo.

## Alternatives

- No behaviour (harder to test seams)
- Large capability framework (deferred)

## Consequences

- Clear module boundary
- Second backend will validate or reshape the callbacks
