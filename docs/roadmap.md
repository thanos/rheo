# Roadmap

Current release: **v0.6.0** (2026-09-16).

## Done in 0.1.0

- Mongo-backed immutable event log
- Bounded fetch + leases + ACK
- Crash/redelivery + stale lease fencing
- Competing consumers
- Independent consumer groups
- Historical query
- Idiomatic `Rheo.Consumer`
- Bounded demand (`max_demand`)
- Demo + docs/tutorials
- Livebook demo (`notebooks/rheo_demo.livemd`)

## Done in 0.2.0

- Named multi-instance Rheo + opaque backend handles
- Local `Rheo.Group` runtime (demand, concurrency, renew, drain)
- Portable `Rheo.Query`; Event codec moved to Mongo
- Explicit persistence-error telemetry / settle semantics
- Migration guide + Article 9

## Done in 0.3.0

- `Rheo.Backend.ETS` (ephemeral, per-instance tables)
- Backend capabilities map
- Shared conformance suite (ETS always; Mongo tagged)
- Docker-free demo / Livebook path
- ADRs 011, 012, 014 + Article 10

## Done in 0.4.0

- Query sequence bounds, `query_page` / `stream_query`
- Replay / `reset_group` (no event copies); `create_group` start cursors
- `Rheo.Event.Lineage` conventions
- ADR 015 + Article 11 + [0.3 → 0.4 migration](https://hexdocs.pm/rheo/0-3-to-0-4.html)

## Done in 0.4.1

- Hex README links use HexDocs / GitHub absolutes (relative `docs/` paths 404 on hex.pm)

## Done in 0.5.0

- Configurable partitions + `:erlang.phash2/2` key routing
- Per-partition sequences; ordered consume within a partition only
- Contiguous ACK frontier + `Rheo.lag/3`
- Static Group/Consumer `:partitions` assignment (no auto-rebalance)
- Partition-scoped `replay` / `reset_group`
- ADR 016 + Article 12 + [0.4 → 0.5 migration](https://hexdocs.pm/rheo/0-4-to-0-5.html)
- Docs/Livebook/CHANGELOG links use absolute HexDocs or GitHub URLs for hex.pm

## Done in 0.6.0

- `Rheo.Backend.Ecto` on host-owned Repo (PostgreSQL + SQLite)
- SQL migrations + `mix rheo.ecto.gen_migration`
- Postgres `FOR UPDATE SKIP LOCKED`; optional `NOTIFY`
- SQLite durable single-node path; `distributed: false`
- ADR 017 + Article 13 + [0.5 → 0.6 migration](https://hexdocs.pm/rheo/0-5-to-0-6.html)

## Next (0.7+)

- Broadway / GenStage interop (evaluate)
- Change-stream / richer wakeup integration
- Mnesia (later)
- Optional Hex package split if dependency hygiene demands it

## Explicitly deferred

- Distributed Rheo cluster coordination (beyond durable backend arbitration)
- Automatic partition rebalancing
- Cross-region replication
- Exactly-once claims
- Transactional consume-and-produce
- Schema registry
- Custom wire protocol
- Administrative web UI
- Kubernetes operator
- AuthN/AuthZ frameworks
- Multi-tenancy
- Retention tiers
- Standalone Rheo server
- `mongodb_ecto` as a Rheo backend path
