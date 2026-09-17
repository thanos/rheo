# ADR 019: v0.8 architectural reset

## Status

Accepted (v0.8.0)

## Context

v0.1–v0.7 proved immutable logs, leases, partitions, frontiers, replay, ETS,
Mongo, Ecto, GenStage, and Broadway. The next planned backend, Redis Streams,
already implements consumer-group mechanics (`XGROUP`, `XREADGROUP`, `XACK`,
`XAUTOCLAIM`). Treating Redis like Mongo would force either a second core
rewrite or a dishonest abstraction.

Rheo is pre-1.0 with limited adoption. v0.8 is an intentional reset: keep
proven semantics, discard weak mechanisms, and prepare Redis and Flow without
shipping either in this release.

## Decision

1. Prefer breaking changes that clarify semantics over preserving ambiguous
   APIs.
2. Keep portable `event.sequence` (Model C) and add an opaque `receipt` on
   leases for native-stream settle identity ([ADR 021](021-logical-sequence-and-native-delivery-receipts.html)).
3. Keep one Hex package and make every integration optional: guarded modules
   plus `optional: true` dependencies, proven by a core-only build check
   ([ADR 020](020-package-and-dependency-boundaries.html)).
4. Make `Rheo.Consumer` handler context read-only and give each
   `{rheo, stream, group}` one local `Rheo.Group` owned by the host supervisor
   ([ADR 022](022-consumer-runtime-and-handler-state.html)).
5. Share inflight bookkeeping, renewal, and backoff between `Rheo.Group` and
   `Rheo.Producer` (`Rheo.Inflight`, `Rheo.Backoff`); make settlement
   failures explicit (`Rheo.Settle`).
6. Describe `Rheo.Backend` as semantic operations and evolve capabilities into
   guarantees vs mechanisms ([ADR 023](023-backend-capabilities-v2.html),
   [ADR 024](024-backend-contract-v2.html)).
7. Do not implement Redis, Flow, or wakeup mechanisms in v0.8; design spikes
   and the native-stream conformance double stand in for them.

## Final architectural test

*If `Rheo.Backend.Redis` were implemented tomorrow using Redis Streams consumer
groups natively, which core modules would need modification?*

None. The adapter implements `Rheo.Backend` with `fetch` on `XREADGROUP` /
`XAUTOCLAIM`, `ack` on a fenced `XACK`, `lag` on `XINFO GROUPS`, keeps a
logical sequence counter per partition, and sets `lease.receipt` to the stream
entry id. `Rheo.Consumer`, `Rheo.Group`, `Rheo.Producer`, `Rheo.Event`,
`Rheo.Query`, and the frontier arithmetic are unchanged; the native-stream
double in `test/support` already passes the conformance suite with exactly
that shape. Only the capability declaration and one optional dependency change.

*If a first-class Flow integration were added tomorrow, could it consume
`Rheo.Producer` through GenStage and reuse the same settlement model as
Broadway without changing Rheo core?*

Yes. `Flow.from_stages([producer])` receives `%Rheo.Lease{}` values;
`Rheo.Producer.ack/3`, `nack/4`, and `reject/4` settle durably and release the
producer's inflight entry in one call, from `map` or from `on_trigger` once an
aggregate is durable. `test/rheo/flow_readiness_test.exs` exercises map,
partition, reduce, window, and crash-before-settle with no Flow-specific
lease, renewal, or confirmation code.

## Consequences

- Migration guide `docs/migrations/0.7-to-0.8.md` is mandatory.
- Superseded or amended ADRs stay published with a status line pointing here or
  to their successor.
- v0.9 can add Redis without changing Consumer, Event, or Query meaning.

## Related

ADRs 005, 007, 009, 010, 011, 012, 016, 018; follow-ups 020–025.
