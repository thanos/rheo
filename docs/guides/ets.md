# ETS

`Rheo.Backend.ETS` is an **ephemeral** in-process backend with the same consumer
API as Mongo and Ecto. Ideal for tests, Livebook, and local demos — no Docker.

## Start

```elixir
{Rheo, name: MyRheo, backend: Rheo.Backend.ETS}
```

Or:

```elixir
{:ok, _} = Rheo.start_link(name: MyRheo, backend: Rheo.Backend.ETS)
:ok = Rheo.ensure_indexes(rheo: MyRheo)
```

## Capabilities

```elixir
Rheo.Backend.ETS.capabilities()
# %Rheo.Backend.Capabilities{
#   guarantees: %{durable: false, distributed: false, partitions: true, …},
#   mechanisms: %{atomic_compare_and_set: true, secondary_indexes: false, …}
# }
```

Data lives only as long as the backend owner process. Restarts wipe the store.

## When to use

| Use ETS | Prefer Mongo / Ecto |
|---|---|
| Unit / property tests | Production durability |
| Livebook / CI without services | Multi-node claims |
| Local scratchpads | Shared history across deploys |

## Same API

```elixir
:ok = Rheo.create_stream("demo", partition_count: 2, rheo: MyRheo)
:ok = Rheo.create_group("demo", "workers", rheo: MyRheo)
{:ok, _} = Rheo.append("demo", %{type: "t", key: "a"}, rheo: MyRheo)
{:ok, [lease]} = Rheo.fetch("demo", "workers", rheo: MyRheo)
:ok = Rheo.ack(lease, rheo: MyRheo)
```

Config timeout: `config :rheo, ets_call_timeout: 5_000`.

Background: [ADR 014](014-ets-backend.html),
[Prove it with ETS](10-if-rheo-is-database-agnostic-prove-it-with-ets.html).
