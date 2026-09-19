# Roadmap

Current release: **v0.10.0**. Ops surface over the v0.9 backends.

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

## Done in 0.7.0

- `Rheo.Producer` — GenStage producer emitting `%Rheo.Lease{}` (demand → fetch,
  inflight renewal, idle poll, backoff, drain)
- `Rheo.Broadway.transform/2` + `Rheo.Broadway.Acknowledger` (ack / nack / reject)
- `Rheo.Producer.confirm/2` so settled leases stop renewing and free demand
- `Rheo.Consumer` / `Rheo.Group` unchanged — the producer is an alternative surface
- ADR 018 + Article 14 + [0.6 → 0.7 migration](https://hexdocs.pm/rheo/0-6-to-0-7.html)

## Done in 0.7.1

- HexDocs **Guides** (Introduction / Advanced / Cookbook) plus Design groups
  (Architecture, ADRs, Tutorials) and Migrating from previous versions
- Mermaid diagrams render on HexDocs
- Livebook Broadway demo aligned with the published v0.7 API

## Done in 0.8.0

- Architectural reset (ADR 019): read-only handler context and one Group owner
  per `{rheo, stream, group}` (ADR 022); lease `receipt` (ADR 021); typed
  capabilities (ADR 023); semantic backend contract and settlement vocabulary
  (ADR 024); `Rheo.Inflight` / `Rheo.Backoff` shared by Group and Producer
- `Rheo.Producer.ack/3`, `nack/4`, `reject/4` — one-call settle and release
- Conformance suite grouped by guarantee, run by ETS, Mongo, Ecto, and a
  native-stream double; failure-injection and property tests on ETS
- Optional integrations with compile-time guards and a core-only build check; Flow and Redis readiness spikes
- ADRs 019–025; Article 15; [0.7 → 0.8 migration](https://hexdocs.pm/rheo/0-7-to-0-8.html)

## Done in 0.9.0

- `Rheo.Backend.Redis` on optional `redix` (ADR 026) — Model C sequences +
  native receipts, fenced `XACK`, reclaim, replay
- Wakeup contract implemented (ADR 025): reader Task + `wait/2`; polling remains
  fallback
- Conformance suite tagged `:redis`; docker-compose Redis 7 service
- [0.8 → 0.9 migration](https://hexdocs.pm/rheo/0-8-to-0-9.html); Redis guide
- Article 16 (portable sequence, native PEL)

## Done in 0.10.0

- Ops surface (ADR 027): DLQ listing, stream/group inventory, group health,
  telemetry metrics helpers, optional LiveDashboard, Mix inspect tasks,
  formalized benchmarks
- [0.9 → 0.10 migration](https://hexdocs.pm/rheo/0-9-to-0-10.html)

## Next (0.11+)

- Mnesia / BEAM-native distributed backend (deferred from 0.10)
- API freeze candidate (0.12)
- Article candidate: Rheo + Flow settlement narrative

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
