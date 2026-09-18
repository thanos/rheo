# ADR 026: Redis Streams backend

## Status

Accepted (v0.9.0)

## Context

v0.8 prepared Model C (`event.sequence` + `lease.receipt`), typed capabilities,
and a native-stream conformance double. v0.9 ships the production adapter on
optional [Redix](https://hex.pm/packages/redix).

## Decision

### Package

One Hex package. `{:redix, "~> 1.5", optional: true}`. Modules under
`Rheo.Backend.Redis*` compile only when `Code.ensure_loaded?(Redix)`.

### Key layout

Prefix `rheo:{name}:` where `name` is the Redix process name atom string.

| Key | Type | Purpose |
|---|---|---|
| `…:meta:{stream}` | HASH | `partition_count` |
| `…:seq:{stream}:{p}` | STRING | monotonic logical sequence |
| `…:z:{stream}:{p}` | ZSET | `sequence` score → Redis stream entry id |
| `…:s:{stream}:{p}` | STREAM | immutable events (fields include `id`, `sequence`, payload JSON) |
| `…:gmeta:{stream}:{group}` | HASH | `max_attempts`, per-partition frontier / cursor |
| `…:fence:{stream}:{group}:{entry_id}` | HASH | `lease_id`, `attempt`, `expires_at` ms |
| `…:dlq:{stream}:{group}` | STREAM | rejected deliveries |

Redis consumer group name = Rheo group name (per partition stream).

### Model C

1. `INCR` sequence, then `XADD` with that sequence in the entry fields.
2. `ZADD` maps sequence → entry id for cursor / replay lookups.
3. `fetch` uses `XREADGROUP` plus reclaim via `XPENDING` / `XCLAIM` when the
   fence is expired (Rheo `lease_ms`, not Redis idle alone); lease `receipt` =
   entry id; `lease_id` = new Rheo token stored in the fence hash.
4. `ack`: fence `lease_id` (and receipt) must match, then `XACK` + delete fence
   + advance frontier if contiguous. Lua scripts are optional later; v0.9 uses
   multi-command authorize-then-settle.

### Query

`secondary_indexes: false`. `query` walks `XRANGE` / ZSET ranges and filters in
the adapter. Document cost for large streams.

### Minimum Redis

Redis **6.2+** (Streams consumer groups / `XCLAIM`). CI uses Redis 7.

### Wakeup

Implements ADR 025 via optional `wait/2` (`XREADGROUP BLOCK` on assigned
partitions). Group/Producer start a reader Task; polling remains fallback.

## Consequences

- Hosts add `{:redix, "~> 1.5"}` beside `:rheo`.
- Core Consumer / Event / Query unchanged.
- Conformance suite gains a `:redis`-tagged contract test.

## Alternatives

- Separate `rheo_redis` Hex package — rejected (ADR 020).
- Model B (opaque positions only) — rejected (ADR 021).
