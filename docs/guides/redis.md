# Redis

`Rheo.Backend.Redis` stores the immutable log in **Redis Streams** and uses
native consumer groups for delivery (ADR 026). Rheo still owns portable
`event.sequence`, lease fencing, contiguous frontiers, and handler semantics.

Requires Redis **6.2+** and:

```elixir
{:rheo, "~> 1.0"},
{:redix, "~> 1.5"}
```

## Supervision

```elixir
children = [
  {Rheo,
   name: MyRheo,
   backend:
     {Rheo.Backend.Redis,
      name: MyRheo.Redis,
      url: System.get_env("RHEO_REDIS_URL", "redis://localhost:6379")}}
]
```

Options: `:url` (`redis://host:port`), or `:host` / `:port`, plus `:name` for
the Redix process.

## Model C

| Rheo | Redis |
|---|---|
| `event.sequence` | Counter + ZSET index per partition |
| `lease.receipt` | Stream entry id |
| `lease.lease_id` | Fence hash (stale ACK → `:stale_lease`) |
| `fetch` | `XREADGROUP` + fenced reclaim via `XPENDING`/`XCLAIM` |
| `ack` | Fence check then `XACK` |

## Capabilities

Durable, distributed, partitioned, contiguous frontier, replay, native
consumer groups / pending list / reclaim / blocking reads. **No** secondary
indexes — `query` scans stream ranges and filters in the adapter.

## Wakeup

When Groups/Producers run, Rheo may start a reader Task that calls `wait/2`
(`XREAD` with `BLOCK`). Polling remains the fallback (ADR 025).

## Local Redis

```bash
docker compose up -d redis
export RHEO_REDIS_URL=redis://localhost:6379
```

See [0.8 → 0.9 migration](https://github.com/thanos/rheo/blob/main/docs/migrations/0.8-to-0.9.md),
[ADR 026](https://hexdocs.pm/rheo/026-redis-streams-backend.html), and
[Article 16](https://github.com/thanos/rheo/blob/main/docs/tutorials/16-rheo-on-redis-streams-portable-sequence-native-pel.md).
