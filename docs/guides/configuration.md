# Configuration

Rheo is usually started under **your** supervision tree. Application config sets
defaults; per-call and per-consumer options override them.

## Prefer explicit supervision

```elixir
children = [
  {Rheo, name: MyRheo, backend: {Rheo.Backend.Mongo, url: mongo_url}},
  {MyApp.RiskConsumer, rheo: MyRheo}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

Pass `:rheo` on public APIs when you use a non-default instance name.

## Application environment

```elixir
config :rheo,
  # Do not auto-start under Rheo.Application (default: false)
  start_on_application: false,

  # Defaults used by fetch / Group / Producer when opts omit them
  default_lease_ms: 30_000,
  default_max_attempts: 5,
  default_max_demand: 10,

  # Clock used for lease expiry (tests: Rheo.Clock.Frozen)
  clock: Rheo.Clock.System,

  # Backend timeouts
  ets_call_timeout: 5_000,
  mnesia_call_timeout: 5_000,
  ecto_call_timeout: 5_000
```

| Key | Purpose |
|---|---|
| `:start_on_application` | When `true` and a backend is configured, Rheo starts under `Rheo.Application` |
| `:backend` | Module or `{module, opts}` for auto-start / Application helper |
| `:mongo_url` | Mongo URL when auto-starting the Mongo backend |
| `:name` | Instance name when auto-starting (default `Rheo`) |
| `:default_lease_ms` | Lease TTL |
| `:default_max_attempts` | Attempts before dead-letter on nack |
| `:default_max_demand` | Outstanding lease bound |
| `:clock` | `Rheo.Clock` implementation |
| `:topology` | Legacy Mongo handle name (prefer instance `:backend` opts) |

## Auto-start (optional)

```elixir
config :rheo,
  start_on_application: true,
  backend: Rheo.Backend.ETS
# or: mongo_url: "mongodb://localhost:27017/rheo"
```

Most apps should leave this off and supervise `{Rheo, …}` themselves.

## Consumer / Producer options

`Rheo.Consumer` and `Rheo.Producer` accept:

| Option | Meaning |
|---|---|
| `:stream` / `:group` | Required identity |
| `:rheo` | Instance name |
| `:max_demand` | Max unsettled leases |
| `:lease_ms` | Lease TTL; renewal runs at half TTL |
| `:poll_ms` | Idle poll when demand is unmet |
| `:consumer_id` | Worker identity on leases |
| `:partitions` | `:all` or a list of partition ids |
| `:concurrency` | Handler concurrency (`Rheo.Consumer` only) |
| `:on_failure` | `:nack` or `:reject` (`Rheo.Producer` / Broadway) |

## Multiple instances

```elixir
children = [
  {Rheo, name: RheoRisk, backend: {Rheo.Backend.Mongo, url: url_a}},
  {Rheo, name: RheoAudit, backend: {Rheo.Backend.Ecto, repo: MyApp.Repo}}
]

Rheo.append("events", %{…}, rheo: RheoRisk)
Rheo.fetch("events", "risk", rheo: RheoAudit)
```

Each instance has its own backend handle, registry, and group supervisor.

## Multi-node BEAM deployments

Several BEAM nodes can run Rheo Groups against the **same** durable store.
Rheo does not manage cluster membership — the backend fences leases.

| Backend | `distributed` | Multi-node OK? |
|---|---|---|
| Redis | `true` | Yes |
| Ecto PostgreSQL | `true` | Yes |
| Ecto SQLite | `false` | No |
| Mongo | `false` (cap) | Shared Mongo works in practice; flag stays conservative |
| ETS | `false` | No |
| Mnesia (v0.11) | `false` | Single-node `disc_copies` only |

Details: [Building your own backend](building-your-own-backend.html#multi-node-several-beam-nodes).

