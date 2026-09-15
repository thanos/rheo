# ADR 012 — Backend conformance suite

## Status

Accepted (v0.3.0)

## Context

A second backend only proves the abstraction if the **same** behavioural tests
run against both. Copy-pasted suites diverge; Mongo-only tests hide leaks.

## Decision

1. Provide `Rheo.BackendContract` (ExUnit case template under `test/support`)
   that injects required semantic tests.
2. Wire one contract module per backend (`ETS` always; `Mongo` tagged `:mongo`).
3. Required v0.3 cases: streams, append/batch, sequences, immutability, read,
   portable query, independent groups, fetch bounds, expiry/redelivery, stale
   fencing, duplicate ACK, retry/max_attempts, reject, renew, crash-before-ACK,
   consumption never deletes events.
4. Later features (replay, partitions, frontiers) are capability-skipped until
   implemented.

## Alternatives

- Shared helpers called from hand-written tests (weaker discipline)
- Ship contract in `lib/` for Hex consumers (deferred)

## Consequences

- Abstraction leaks surface as dual failures
- CI always exercises ETS; Mongo remains optional locally without Docker
