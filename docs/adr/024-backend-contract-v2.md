# ADR 024: Backend contract v2

## Status

Accepted (v0.8.0)

## Context

`Rheo.Backend` grew as a mirror of Mongo delivery-row algorithms. Redis Streams
already provide consumer-group primitives; a contract that requires "materialize
delivery documents" cannot host Redis honestly.

## Decision

1. Treat `Rheo.Backend` callbacks as **semantic**: log, claim, renew, settle,
   replay, lag, health.
2. Keep storage algorithms private to each adapter.
3. Require opaque lease `receipt` support for native-stream settle (ADR 021).
4. Executable contract remains the conformance suite (+ native-stream double).

## Consequences

- Backend authors map old callbacks 1:1 where semantics match; internals may
  rewrite freely.
- ADR 005 remains the origin of the boundary; this ADR refines its intent.

## Related

ADR 005, 012, 021, 023.
