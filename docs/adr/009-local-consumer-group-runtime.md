# ADR 009 — Local consumer group runtime

## Status

Accepted (v0.2.0). Amended by [ADR 022](022-consumer-runtime-and-handler-state.html) (v0.8.0): the bridge process is removed and handler state is read-only.

## Context

v0.1.0 `Rheo.Consumer` was a poll GenServer that fetched and handled work inline.
Documented `concurrency` was ignored; ACK failures were swallowed; leases were
not renewed for long handlers.

## Decision

1. One local `Rheo.Group` GenServer per `{instance, stream, group}` coordinates
   demand, fetch, inflight Tasks, renewals, backoff, and drain.
2. `use Rheo.Consumer` starts a bridge process that starts/joins that Group with
   the callback module; handlers stay pure outcome functions.
3. Durable ACK/lease truth remains in the backend. Local Groups on multiple nodes
   may compete; the backend arbitrates.

## Consequences

- Real `concurrency` and `max_demand` bounds.
- Persistence errors surface via telemetry and settle policy.
- Group crash loses only ephemeral inflight tracking; leases expire/redeliver.
