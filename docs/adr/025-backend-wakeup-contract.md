# ADR 025: Backend wakeup contract

## Status

Accepted (v0.8.0)

## Context

Groups and Producers poll on `:poll_ms`. Redis blocking reads (`XREADGROUP`
BLOCK) and Postgres `LISTEN/NOTIFY` can reduce latency, but wakeup must never
become authoritative for delivery correctness.

## Decision

1. Polling remains the universal fallback.
2. Optional backend hint: `Rheo.Backend.Wakeup.wait(handle, opts)` may block
   until work *might* be available, return `:ok`, or `{:error, reason}`.
3. Wakeup is a **hint**. Missed or spurious wakeups are safe; leases and fetch
   remain the source of truth.
4. Runtimes may call wakeup between polls when `Capabilities.mechanism?(caps,
   :blocking_reads)` or `:notifications` is true.

## Consequences

- No Redis blocking implementation in v0.8 — only the contract.
- Mongo/Ecto/ETS may stub wakeup as immediate `:ok`.

## Related

ADR 007, 018, 023.
