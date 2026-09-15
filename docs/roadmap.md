# Roadmap

## Done in MVP

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

## Explicitly deferred

- Distributed Rheo cluster coordination
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
- PostgreSQL / Scylla / SQLite backends
- Mongo change-stream push optimization
- Retention tiers
- Standalone Rheo server
