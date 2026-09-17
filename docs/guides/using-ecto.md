# Using Ecto

`Rheo.Backend.Ecto` runs the same Rheo API on a **host-owned** `Ecto.Repo`
(PostgreSQL or SQLite). Rheo does not own your Repo supervision or migrations
layout — you do.

## Dependencies

Rheo already depends on `ecto` / `ecto_sql`. Add the adapter your app uses:

```elixir
{:postgrex, "~> 0.19"}      # PostgreSQL
# or
{:ecto_sqlite3, "~> 0.17"}  # SQLite
```

## Supervise Repo then Rheo

```elixir
children = [
  MyApp.Repo,
  {Rheo, name: MyRheo, backend: {Rheo.Backend.Ecto, repo: MyApp.Repo}},
  {MyApp.RiskConsumer, rheo: MyRheo}
]
```

```elixir
:ok = Rheo.ensure_indexes(rheo: MyRheo)
```

Generate migrations with:

```bash
mix rheo.ecto.gen_migration
```

## PostgreSQL vs SQLite

| | PostgreSQL | SQLite |
|---|---|---|
| `distributed` | `true` (`FOR UPDATE SKIP LOCKED`) | `false` |
| Multi-node claims | Yes | No — single writer |
| JSON | `jsonb` | JSON text |
| Typical use | Production SQL | Embedded / local durable |

```elixir
Rheo.Backend.Ecto.capabilities(:postgres)
Rheo.Backend.Ecto.capabilities(:sqlite)
```

## Same consumer code

Switching backends does not change `Rheo.Consumer`, `fetch`/`ack`, query, or
lag APIs — only the supervised `{Rheo, backend: …}` child.

## Not Mongo-via-Ecto

Mongo stays on `Rheo.Backend.Mongo`. The Ecto backend is **SQL only** (ADR 017).

Tutorial: [One consumer API, PostgreSQL and SQLite](13-one-consumer-api-postgresql-and-sqlite.html).
