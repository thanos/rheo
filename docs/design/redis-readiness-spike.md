# Redis Streams readiness spike (v0.8)

No Redis dependency in v0.8. Mapping for v0.9 `rheo_redis` under Model C
(ADR 021):

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

## Exit criterion

v0.9 can add Redis without changing `Rheo.Consumer`, `Rheo.Event` meaning,
partition ordering, or at-least-once / fencing invariants.
