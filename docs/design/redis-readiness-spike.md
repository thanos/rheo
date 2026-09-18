# Redis Streams readiness spike (v0.8)

No Redis dependency in v0.8. Mapping for a v0.9 `Rheo.Backend.Redis` (guarded
on an optional `redix` dependency, ADR 020) under Model C (ADR 021):

| Redis | Rheo semantic |
|---|---|
| `XADD` | `append` — store Redis ID; assign portable `event.sequence` via script/counter |
| `XGROUP CREATE` | `create_group` |
| `XREADGROUP` | `fetch` — `lease.receipt` = Redis stream ID / claim token; new `lease_id` fencing token |
| `XPENDING` / `XAUTOCLAIM` | reclaim expired → new lease, possibly same receipt |
| `XACK` | `ack` using `receipt` (+ verify fencing) |
| `XCLAIM` | reclaim path for stale leases |
| `XRANGE` | `read` / `query` range (plus secondary indexes if any) |
| `XINFO GROUPS` | feed `lag/4` high-watermark / pending counts |

## Fencing exercise

1. Worker A reads ID `1700-0`, gets `lease_id=L1`, `receipt="1700-0"`.
2. Lease expires; Worker B `XAUTOCLAIM`s → `lease_id=L2`, same receipt.
3. Worker A `ack` with L1 must fail (`:stale_lease`).
4. Worker B `ack` with L2 + receipt succeeds via `XACK`.

## Gaps Rheo fills on top of Redis

| Gap | v0.8 contract |
|---|---|
| Fencing stronger than `XACK` (no token) | `lease_id` plus `receipt`; a Redis-side record or script rejects a stale token before `XACK` |
| Retry / reject policy | `retry/3` and `reject/3` are Rheo semantics; the adapter keeps attempt counts and a dead-letter stream |
| Portable sequence / frontier | logical sequence per partition kept by the adapter; frontier arithmetic stays in Rheo |
| Partition mapping | one Redis stream per partition |
| Query capability differences | `secondary_indexes: false`; `query` on `XRANGE` plus in-adapter filtering |
| Settlement ambiguity | connection loss maps to `:backend_unavailable`; the runtime leaves the lease to expire |

## Proof in v0.8

`Rheo.Backend.NativeStreamDouble` (test support) implements the contract with
exactly this shape — native ids as receipts, fencing on both identities,
native reclaim — and passes the full conformance suite. Adding it required no
change to `Rheo.Consumer`, `Rheo.Group`, `Rheo.Producer`, `Rheo.Event`, or
`Rheo.Query`.

## Exit criterion

**Met in v0.9:** `Rheo.Backend.Redis` ships without changing `Rheo.Consumer`,
`Rheo.Event` meaning, partition ordering, or at-least-once / fencing
invariants. See [ADR 026](https://hexdocs.pm/rheo/026-redis-streams-backend.html)
and the [Redis guide](https://hexdocs.pm/rheo/redis.html).
