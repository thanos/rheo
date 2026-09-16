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

## Follow-up

`Rheo.Backend.ETS` (ADR 014) and `Rheo.Backend.Ecto` (ADR 017) implemented the
behaviour without reshaping it. SQL was the real test: it has no
`findOneAndUpdate` and no atomic increment inside a document, yet the callbacks
held. Backend-specific mechanics (`FOR UPDATE SKIP LOCKED`, `jsonb` vs JSON
text, `?` vs `$1` placeholders) stayed inside the implementation, which is the
boundary working as intended.
