# ADR 017 — Ecto SQL backend

## Status

Accepted (v0.6.0)

## Context

Mongo (ADR 003) and ETS (ADR 014) proved the `Rheo.Backend` boundary but left the
most common Elixir persistence layer unaddressed. Teams that already run
PostgreSQL do not want a second datastore just to get consumer groups, and
`Rheo.Backend.ETS` is not durable enough to stand in for one. A SQL backend is
also the strongest test of whether the boundary is genuinely portable: relational
storage has no `$max`, no `findOneAndUpdate`, and no document-shaped sequence
allocator.

## Decision

1. Ship `Rheo.Backend.Ecto` supporting PostgreSQL (`Ecto.Adapters.Postgres`) and
   SQLite (`Ecto.Adapters.SQLite3`). SQL only — document stores behind Ecto
   (e.g. `mongodb_ecto`) are out of scope because Rheo already has a native
   Mongo backend.
2. **The host owns the `Ecto.Repo`.** Rheo never starts, configures, or migrates
   someone else's repo pool:

       {Rheo, backend: {Rheo.Backend.Ecto, repo: MyApp.Repo}}

   `child_spec/1` starts `Rheo.Backend.Ecto.Server`, a small GenServer that
   resolves `repo.__adapter__()` into a dialect and answers configuration
   lookups. Its registered name is the opaque handle, exactly like the Mongo
   process name and the ETS owner name (ADR 010).
3. Five tables (ADR 008's collections, normalized):
   `rheo_streams`, `rheo_stream_sequences`, `rheo_events`, `rheo_groups`,
   `rheo_deliveries`. Sequence allocation moves into its own
   `(stream, partition)` row rather than a nested map, because SQL cannot
   atomically increment a key inside a JSON document.
4. Claiming is dialect-specific but the semantics are not. PostgreSQL selects
   candidates `FOR UPDATE SKIP LOCKED` so many nodes fetch concurrently; SQLite
   selects and updates inside one immediate transaction, relying on its single
   writer. Both then re-check the claimable predicate in the `UPDATE`, so a lost
   race yields fewer leases rather than a double delivery.
5. Lease fencing, contiguous frontier, replay, reset, and lag reuse the Mongo
   semantics verbatim: every mutation is gated on `lease_id` **and**
   `status = 'leased'`, and ACK/reject walk terminal deliveries forward from the
   stored frontier (ADR 016).
6. Dialect differences live in `Rheo.Backend.Ecto.Codec` and
   `Rheo.Backend.Ecto.Migrations`, not in the callback bodies. PostgreSQL uses
   `jsonb` and `timestamptz`; SQLite uses `TEXT` holding JSON and fixed-width
   ISO 8601 (so string comparison matches chronological order).
7. Queries are written once with portable `?` placeholders and rewritten to
   `$1..$n` for PostgreSQL. Payload and metadata filters compile to `->>` on
   PostgreSQL and `json_extract/2` on SQLite.
8. Schema creation is idempotent DDL (`CREATE TABLE IF NOT EXISTS`), so
   `Rheo.ensure_indexes/1` works out of the box. Hosts that prefer explicit
   migrations run `mix rheo.ecto.gen_migration`, which delegates to
   `Rheo.Backend.Ecto.Migrations` so future schema changes ship as library code.
9. `NOTIFY` is opt-in (`notify: true`, PostgreSQL only) and reflected in
   `capabilities/1`. It is a wakeup hint, never a delivery guarantee.

## Capabilities

| Flag | PostgreSQL | SQLite |
|---|---|---|
| `durable` | true | true |
| `distributed` | true | false |
| `atomic_compare_and_set` | true | true |
| `notifications` | `notify: true` | false |
| `change_feed` | false | false |
| `secondary_indexes` | true | true |
| `batch_writes` | true | true |
| `ordered_range_scan` | true | true |
| `replay` | true | true |
| `partitions` | true | true |
| `contiguous_frontier` | true | true |

SQLite is declared `distributed: false` because concurrent claims are safe on
one node only; it is a development and single-node story, not a cluster one.

## Alternatives

- **Ecto schemas and `Ecto.Query` instead of raw SQL.** Rejected: the hot paths
  need `FOR UPDATE SKIP LOCKED`, `RETURNING`, `ON CONFLICT DO NOTHING`, and
  dialect-specific JSON operators. Raw SQL behind a codec is smaller and clearer
  than fighting the query builder, and Rheo defines no user-facing schemas.
- **Normalizing group progress into `rheo_group_partitions`.** Deferred:
  `cursors` / `frontiers` stay JSON maps to match the Mongo and ETS record shape,
  and both are mutated inside a transaction that locks the group row.
- **Advisory locks instead of `SKIP LOCKED`.** Rejected: extra state to leak on
  crash, with no ordering benefit.
- **Encoding timestamps as integer microseconds in both dialects.** Rejected:
  it would make the event log meaningfully less searchable by hand, which is the
  point of putting it in your database.
- **All four Ecto packages `optional: true`.** Rejected for `ecto` /
  `ecto_sql`: `Rheo.Backend.Ecto.Migrations.V1` needs `Ecto.Migration` at compile
  time, and guarding whole modules behind `Code.ensure_loaded?/1` is fragile.
  The drivers (`postgrex`, `ecto_sqlite3`) stay optional because the host picks
  one. Splitting Rheo into per-backend packages is deferred.

## Consequences

- Teams on PostgreSQL get durable, distributed consumer groups with no new
  infrastructure, and a `jsonb` event log they can query directly.
- SQLite gives contract-suite coverage of a real SQL dialect with no service to
  start, so the conformance suite (ADR 012) runs on durable storage in CI.
- Mongo users now pull `ecto` and `ecto_sql` transitively. Acceptable for 0.6;
  revisit if the package is split.
- A third backend confirmed the callbacks in ADR 005 need no reshaping.
