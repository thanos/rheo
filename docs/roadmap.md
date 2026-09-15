# Roadmap

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

## Next (0.3+)

- ETS backend + conformance suite
- Search/replay API
- Partitions / frontier
- Additional durable backends
- Broadway interop (evaluate)

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
