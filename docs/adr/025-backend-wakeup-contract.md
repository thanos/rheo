# ADR 025: Backend wakeup contract

## Status

Accepted (v0.9.0)

## Context

Groups and Producers poll on `:poll_ms`. Redis blocking reads
(`XREADGROUP BLOCK`) and PostgreSQL `LISTEN/NOTIFY` can reduce latency. A
wakeup must never become authoritative for delivery correctness, and a
blocking backend call must never run inside the Group or Producer process.

## Decision

1. Polling remains the universal fallback.
2. A backend may declare `:blocking_reads` or `:notifications` and implement
   `Rheo.Backend.Wakeup.wait/2` (optional callback module API).
3. When `function_exported?(backend, :wait, 2)`, the Group/Producer starts a
   reader `Task` under the instance `Task.Supervisor` that calls `wait/2` and
   sends `:fetch` to the coordinator. The coordinator treats the message as a
   hint and keeps its poll timer.
4. Conformance may use doubles that always/never wake; both must drain events.

## Related

ADR 007, 018, 023, 026.
