# ADR 019: v0.8 architectural reset

## Status

Accepted (v0.8.0)

## Context

v0.1–v0.7 proved immutable logs, leases, partitions, frontiers, replay, ETS,
Mongo, Ecto, GenStage, and Broadway. The next planned backend — **Redis
Streams** — already implements consumer-group mechanics (`XGROUP`,
`XREADGROUP`, `XACK`, `XCLAIM`, …). Treating Redis like Mongo forces either a
second core rewrite or a dishonest abstraction.

Rheo is pre-1.0 with limited adoption. v0.8 is an intentional reset: keep
proven **semantics**, discard weak **mechanisms**, and prepare Redis/Flow
without shipping either in this release.

## Decision

1. Prefer breaking changes that clarify semantics over preserving ambiguous APIs.
2. Keep portable `event.sequence` (Model C) and add opaque native delivery
   receipts on leases for stream backends (ADR 021).
3. Split Hex packages so core users do not pull Mongo/Ecto/Broadway (ADR 020).
4. Make `Rheo.Consumer` handler context read-only (Option A); one local group
   runtime per `{rheo, stream, group}` on a node (ADR 022).
5. Share lease-lifecycle bookkeeping between `Rheo.Group` and `Rheo.Producer`.
6. Thin `Rheo.Backend` toward semantic operations; evolve capabilities into
   guarantees vs mechanisms.
7. Do **not** implement Redis or a Flow package in v0.8 — design spikes only.

## Consequences

- Migration guide `docs/migrations/0.7-to-0.8.md` is mandatory.
- Old ADRs that are superseded stay published with links here / to successors.
- v0.9 can add Redis without changing Consumer / Event / Query meaning.

## Supersedes / related

Supersedes none wholesale. Relates to ADRs 005, 007, 010, 011, 012, 016, 018.
Follow-ups: 020–022 and later settlement/wakeup ADRs.
