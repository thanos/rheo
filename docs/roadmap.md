# Roadmap

Current release: **v0.5.0** (2026-09-16).

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

## Next (0.6+)

- Additional durable backends (Ecto family)
- Broadway interop (evaluate)
- Change-stream wakeups
- Mnesia (later)

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
