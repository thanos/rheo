# ADR 025: Backend wakeup contract

## Status

Proposed. Not implemented in v0.8.0.

## Context

Groups and Producers poll on `:poll_ms`. Redis blocking reads
(`XREADGROUP BLOCK`) and PostgreSQL `LISTEN/NOTIFY` can reduce latency. A
wakeup must never become authoritative for delivery correctness, and a
blocking backend call must never run inside the Group or Producer process.

v0.8 keeps fetch scheduling as a poll timer with exponential backoff
(`Rheo.Backoff`). No wakeup callback ships, because nothing would call it; a
contract with no caller is not a contract.

## Proposed design (v0.9)

1. Polling remains the universal fallback.
2. A backend may declare the `:blocking_reads` or `:notifications` mechanism
   and implement an optional `wait(handle, opts)` that returns when work may be
   available.
3. The runtime starts a reader `Task` under the instance's `Task.Supervisor`
   that calls `wait/2` and sends `:fetch` to the Group or Producer. The
   coordinator treats the message as a hint and keeps its poll timer; a lost or
   spurious wakeup changes latency only.
4. Conformance adds two doubles: one that delivers wakeups and one that never
   does; both must consume every event.

## Related

ADR 007, 018, 023.
