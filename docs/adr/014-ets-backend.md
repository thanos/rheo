# ADR 014 — ETS backend

## Status

Accepted (v0.3.0)

## Context

Developers need a zero-infrastructure Rheo for tests, Livebook, and ephemeral
apps. Without a second backend, `Rheo.Backend` risks remaining Mongo-shaped.

## Decision

1. Ship `Rheo.Backend.ETS`: a GenServer owner that creates per-instance ETS
   tables (`streams`, `events`, `groups`, `deliveries`).
2. The opaque handle is the owner process name (same supervision pattern as
   Mongo’s process handle).
3. All mutations are serialized through the owner for correct lease fencing in
   v0.3; tables die with the owner (`durable: false`).
4. Document that ETS does not survive owner/node restart and is not a production
   durable store.

## Alternatives

- Bare ETS tables without an owner (lifecycle/global leaks)
- Process heap maps only (harder to inspect; still ephemeral)

## Consequences

- `{Rheo, backend: Rheo.Backend.ETS}` needs no Docker/Mongo
- Conformance can run without external services
- Production deployments continue to use Mongo (or later durable backends)
