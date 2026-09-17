# Mongo

`Rheo.Backend.Mongo` is the original durable backend: searchable BSON event log
plus per-group deliveries in MongoDB.

## Start

```elixir
{Rheo,
 name: MyRheo,
 backend: {Rheo.Backend.Mongo, url: "mongodb://localhost:27017/rheo", pool_size: 5}}
```

Shorthand (defaults to Mongo when `:backend` is omitted):

```elixir
{Rheo, name: MyRheo, url: "mongodb://localhost:27017/rheo"}
```

Always run indexes once per deployment:

```elixir
:ok = Rheo.ensure_indexes(rheo: MyRheo)
```

## Capabilities

```elixir
Rheo.Backend.Mongo.capabilities()
# %Rheo.Backend.Capabilities{
#   guarantees: %{durable: true, distributed: false, partitions: true, …},
#   mechanisms: %{secondary_indexes: true, batch_writes: true, …}
# }
```

## Local development

```bash
docker compose up -d   # if the repo provides a Mongo service
export RHEO_MONGO_URL=mongodb://localhost:27017/rheo_dev
```

```elixir
config :rheo,
  start_on_application: true,
  mongo_url: System.get_env("RHEO_MONGO_URL")
```

Prefer explicit supervision in applications.

## Schema sketch

| Collection | Role |
|---|---|
| streams / sequences | Stream registry and per-partition counters |
| events | Immutable log (payload + lineage) |
| groups | Cursors and frontiers |
| deliveries | Leases, attempts, DLQ per group |

Details: [ADR 008](008-mongodb-schema-and-indexes.html),
[MongoDB as a searchable event log](06-mongodb-searchable-event-log.html).
