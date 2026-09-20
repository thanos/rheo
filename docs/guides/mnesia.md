# Mnesia

`Rheo.Backend.Mnesia` is a **durable**, ETS-shaped backend on OTP `:mnesia`
(single-node `disc_copies`). Same consumer API as ETS / Mongo / Ecto / Redis.
Ideal when you want restart-safe storage without Docker or an external database.

Rheo lists `:mnesia` as an **included** OTP application (loaded, not auto-started)
so the first `Rheo.Backend.Mnesia` process can set `:dir` and create `disc_copies`
before `:mnesia.start/0`.

## Start

```elixir
{Rheo, name: MyRheo, backend: {Rheo.Backend.Mnesia, dir: "/var/lib/rheo/mnesia"}}
```

Options:

| Option | Meaning |
|---|---|
| `:name` | Registered handle (default `Rheo.Mnesia`) |
| `:dir` | Mnesia directory (default under the system temp dir) |

Multiple Rheo instances on one BEAM node share the node schema but use **unique
table name prefixes** derived from `:name`.

## Capabilities

```elixir
Rheo.Backend.Mnesia.capabilities()
# guarantees: durable: true, distributed: false, partitions: true, …
# mechanisms: atomic_compare_and_set: false — dirty_write to disc_copies is not
# crash-durable the way :mnesia.sync_transaction is.
```

Data survives GenServer restart when `:dir` is preserved. v0.11 is **not**
multi-node — several BEAM nodes competing on one group still need Redis or
PostgreSQL (see [multi-node comparison](building-your-own-backend.html#multi-node-several-beam-nodes)).

## When to use

| Use Mnesia | Prefer |
|---|---|
| Single-node durable BEAM-native store | Redis / Postgres for multi-node |
| Docker-free durable demos | ETS for ephemeral tests |
| Same state machine as ETS with disc | Mongo when you need document search |

## Same API

```elixir
:ok = Rheo.create_stream("demo", partition_count: 2, rheo: MyRheo)
:ok = Rheo.create_group("demo", "workers", rheo: MyRheo)
{:ok, _} = Rheo.append("demo", %{type: "t", key: "a"}, rheo: MyRheo)
{:ok, [lease]} = Rheo.fetch("demo", "workers", rheo: MyRheo)
:ok = Rheo.ack(lease, rheo: MyRheo)
```

Background: [ADR 028](028-mnesia-backend.html).
