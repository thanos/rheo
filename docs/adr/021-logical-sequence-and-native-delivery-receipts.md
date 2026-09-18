# ADR 021: Logical sequence and native delivery receipts

## Status

Accepted (v0.8.0)

## Context

Redis Streams identify entries with IDs like `"1700000000000-0"` and settle via
PEL + `XACK`, not Rheo delivery rows. Replacing `event.sequence` with opaque
positions (Model B) would break portable query/frontier APIs. Forcing Redis to
hide native IDs entirely (Model A only) fights the backend.

## Decision — Model C

1. **Keep** `event.sequence` as Rheo's portable per-partition logical order for
   query, replay cursors, and contiguous frontiers.
2. **Add** an optional opaque `receipt` on `%Rheo.Lease{}` (and settle path) for
   backend-native claim identity.
3. Domain/event code never depends on receipt structure.
4. Database backends (Mongo/Ecto/ETS) may set `receipt` to the same value as
   `lease_id` or a private delivery key; Redis v0.9 will store stream ID /
   consumer PEL identity.

### Redis mapping (design only)

| Redis | Rheo |
|---|---|
| `XADD` | append → assign logical `sequence` (script/counter) + store Redis ID |
| `XREADGROUP` | fetch → lease with `receipt` = Redis ID / claim token |
| `XACK` | ack using `receipt` + fencing |
| `XAUTOCLAIM` | reclaim expired → new lease_id, same or updated receipt |

## Consequences

- Frontiers remain integer contiguous arithmetic in Rheo semantics.
- Native-stream backends must maintain logical sequence (or prove an ADR change).
- Conformance suite includes a native-stream test double exercising receipt settle.

## Alternatives

- Model A only / Model B only — rejected per decision rule in the v0.8 prompt.
