# One Consumer API, PostgreSQL and SQLite Underneath

Rheo’s public surface — `Rheo.Consumer`, leases, ACK, query, replay, lag — does
not change when you switch backends. v0.6 adds `Rheo.Backend.Ecto` so the same
OTP tree can sit on PostgreSQL or SQLite through a host-owned `Ecto.Repo`.

## Same API

```elixir
children = [
  MyApp.Repo,
  {Rheo, name: MyRheo, backend: {Rheo.Backend.Ecto, repo: MyApp.Repo}},
  {MyApp.RiskConsumer, rheo: MyRheo, concurrency: 8, max_demand: 100}
]
```

Handlers still return `{:ack, state}`, `{:retry, reason, state}`, or
`{:reject, reason, state}`. Partitions, the contiguous frontier, and
`Rheo.lag/3` behave as in v0.5 — only the durable store changes.

## Host owns the Repo

Rheo never starts the connection pool. You supervise `MyApp.Repo` (or reuse an
existing one) and pass it in. Schema comes from `Rheo.ensure_indexes/1` or
`mix rheo.ecto.gen_migration` so production releases migrate once under your
usual `ecto.migrate` step.

## PostgreSQL vs SQLite

| Concern | PostgreSQL | SQLite |
|---|---|---|
| Multi-node | Shared DB; `distributed: true` | Single-writer; `distributed: false` |
| Claim | `FOR UPDATE SKIP LOCKED` | Transactional select + update |
| Payload columns | `jsonb` | JSON text |
| Wakeup hint | Optional `notify: true` → `NOTIFY` | Not available |

Use PostgreSQL when more than one BEAM node competes for the same group. Use
SQLite for durable local demos, tests, and single-node apps without standing up
a database service.

## What stays true

- At-least-once delivery and `lease_id` fencing
- Immutable events; consumption never deletes history
- Ordering **within** a partition only
- Contiguous ACK frontier (holes block lag)
- Mongo stays on `Rheo.Backend.Mongo` — Ecto here means **SQL**, not
  [`mongodb_ecto`](https://github.com/elixir-mongo/mongodb_ecto)

## Further reading

- [ADR 017](https://hexdocs.pm/rheo/017-ecto-backend.html)
- [0.5 → 0.6 migration](https://hexdocs.pm/rheo/0-5-to-0-6.html)
- [Article 12 — ACKs are not a cursor](https://hexdocs.pm/rheo/12-acks-are-not-a-cursor.html)
