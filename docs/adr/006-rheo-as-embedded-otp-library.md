# ADR 006: Rheo as embedded OTP library

## Context

A standalone Rheo daemon would add deployment surface and duplicate OTP strengths.

## Decision

Rheo is a library. Applications supervise `{Rheo, opts}` and `Rheo.Consumer`
children. Coordination state is durable in MongoDB; processes hold connections and
in-flight consumer loops only.

## Alternatives

- Standalone broker process / network protocol (rejected for MVP)

## Consequences

- Feels like GenServer/Broadway/Oban
- No separate ops unit for Rheo itself
- Correctness must not depend on an in-memory control plane
