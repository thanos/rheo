# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.6.0] - 2026-09-16

Ecto SQL backend for PostgreSQL and SQLite.
See [0.5 → 0.6 migration](https://hexdocs.pm/rheo/0-5-to-0-6.html).

### Added

- `Rheo.Backend.Ecto` — full `Rheo.Backend` on a **host-owned** `Ecto.Repo`:
  `{Rheo, backend: {Rheo.Backend.Ecto, repo: MyApp.Repo}}`
- PostgreSQL (`Ecto.Adapters.Postgres`) with `FOR UPDATE SKIP LOCKED` claims and
  `jsonb` payload/metadata columns; SQLite (`Ecto.Adapters.SQLite3`) for durable
  zero-service local runs
- `Rheo.Backend.Ecto.Server` — configuration holder whose registered name is the
  opaque backend handle (same pattern as Mongo and ETS, ADR 010)
- `Rheo.Backend.Ecto.Migrations` — idempotent dialect-aware DDL for
  `rheo_streams`, `rheo_stream_sequences`, `rheo_events`, `rheo_groups`, and
  `rheo_deliveries`, plus `Rheo.Backend.Ecto.Migrations.V1` for `mix ecto.migrate`
- `mix rheo.ecto.gen_migration` — generates a host migration that delegates to
  the library, so schema changes ship as Rheo code
- `Rheo.Backend.Ecto.Codec` — JSON and timestamp encoding per dialect
- `Rheo.Backend.Ecto.capabilities/1` — dialect- and option-aware flags
  (`distributed` follows the dialect, `notifications` follows `notify: true`)
- Optional `notify: true` → `NOTIFY rheo_events` on append (PostgreSQL only)
- Optional `prefix:` to hold the Rheo tables in a PostgreSQL schema
- Backend conformance runs on SQLite by default and on PostgreSQL when
  `RHEO_POSTGRES_URL` (or `DATABASE_URL`) is set
- ADR [017](https://hexdocs.pm/rheo/017-ecto-backend.html); tutorial
  [article 13](https://hexdocs.pm/rheo/13-one-consumer-api-postgresql-and-sqlite.html)
- Livebook optional SQLite/Ecto section; [0.5 → 0.6 migration](https://hexdocs.pm/rheo/0-5-to-0-6.html)

### Changed

- `ecto` and `ecto_sql` are now dependencies; `postgrex` and `ecto_sqlite3` are
  optional so the host picks its own driver. Splitting Rheo into per-backend
  packages is deferred — see ADR 017 "Alternatives".
- ADR 011 allows an optional `capabilities/1` for backends whose flags depend on
  runtime configuration; `capabilities/0` remains the static self-description
- `docker-compose.yml` and CI add a PostgreSQL service

## [0.5.0] - 2026-09-16

Partitions, per-partition sequences, contiguous ACK frontier, and lag.
See [0.4 → 0.5 migration](https://hexdocs.pm/rheo/0-4-to-0-5.html).

### Added

- Configurable `partition_count` with per-partition sequence allocation
- Deterministic key routing via `:erlang.phash2/2` (`Rheo.Partition`)
- Contiguous committed frontier per `(stream, group, partition)` (ADR 016)
- `Rheo.lag/3` and `%Rheo.Lag{}` (sum of per-partition HW − frontier)
- Static Group/Consumer `:partitions` assignment (`:all` or list)
- Partition-scoped `replay` / `reset_group` (`:partition` / `:partitions`)
- Capabilities `partitions: true`, `contiguous_frontier: true`
- ADR [016](https://hexdocs.pm/rheo/016-partitions-and-ack-frontier.html);
  tutorial [article 12](https://hexdocs.pm/rheo/12-acks-are-not-a-cursor.html)
- Livebook section for multi-partition publish, frontier hole, and lag

### Changed

- Group docs store per-partition `cursors` / `frontiers` (legacy `next_sequence`
  maps to partition `0`)
- Stream docs store `next_sequences` map
- README / Livebook / changelog doc links stay on absolute HexDocs or GitHub
  URLs so [hex.pm/packages/rheo](https://hex.pm/packages/rheo) does not 404
  (same class of fix as 0.4.1)

## [0.4.1] - 2026-09-16

### Fixed

- README documentation links use absolute [HexDocs](https://hexdocs.pm/rheo/) /
  [GitHub](https://github.com/thanos/rheo) URLs. Relative `docs/…` and
  `notebooks/…` paths were rewritten by Hex to
  `repo.hex.pm/preview/rheo/…` and 404'd because those files are not in the
  package tarball ([hex.pm/packages/rheo](https://hex.pm/packages/rheo)).

## [0.4.0] - 2026-09-16

Search, pagination/streaming, replay/reset, and event lineage — additive over
v0.3.0. See [0.3 → 0.4 migration](https://hexdocs.pm/rheo/0-3-to-0-4.html).

### Added

- Query sequence bounds: `:after_sequence` (exclusive), `:until_sequence` (inclusive)
- Opaque page cursors on `%Rheo.Query{}`; `%Rheo.Page{events, next_cursor}`
- `Rheo.query_page/2` and `Rheo.stream_query/2` (bounded page size)
- `Rheo.create_group/3` start cursors: `:start_after`, `:start_at`
- `Rheo.replay/3` — reopen deliveries (`from_sequence:`, `from:`, `query:`)
- `Rheo.reset_group/3` — destructive per-group delivery reset (`confirm: true`)
- Backend callbacks `replay/4`, `reset_group/4`; capability `replay: true`
- `Rheo.Event.Lineage` helpers for `correlation_id`, `causation_id`, `producer`,
  `schema`, `schema_version`
- Telemetry: `[:rheo, :group, :replay]`, `[:rheo, :group, :reset]`
- ADR [015](https://hexdocs.pm/rheo/015-replay-semantics.html); tutorial
  [article 11](https://hexdocs.pm/rheo/11-search-and-replay-the-event-history.html)
- Conformance + unit coverage for search/replay on ETS and Mongo
- [0.3 → 0.4 migration](https://hexdocs.pm/rheo/0-3-to-0-4.html)

### Changed

- Expanded `Rheo.Query` documentation with filters, ranges, pagination examples
- Livebook demo documents search/replay APIs (still defaults to ETS)

### Migration

- **Non-breaking** for v0.3 callers: existing `Rheo.query/2` `{:ok, list}` unchanged
- Prefer a **new group** with `:start_after` / `:start_at` for safe replay isolation
- Never call `reset_group` without `confirm: true`

## [0.3.0] - 2026-09-15

### Added

- `Rheo.Backend.ETS` — ephemeral per-instance ETS backend (no Docker)
- Backend `capabilities/0` callback; Mongo and ETS implementations
- Backend conformance suite (BackendContract ExUnit template) for ETS and Mongo
- ADRs 011, 012, 014; tutorial article 10
- Docker-free `mix rheo.demo` (default ETS; `RHEO_BACKEND=mongo` for Mongo)

### Changed

- Application auto-start can use `config :rheo, backend: Rheo.Backend.ETS`
- Livebook demo defaults to ETS

## [0.2.0] - 2026-09-15

### Added

- Named Rheo instances (`{Rheo, name:, backend: {Mod, opts}}`) and `rheo:` opts
- `Rheo.Instance`, `Rheo.Group`, `Rheo.GroupSupervisor`
- Real consumer `concurrency`, lease renewal, fetch backoff, drain
- Portable `%Rheo.Query{}` and `Rheo.Backend.Mongo.Codec`
- `Rheo.renew/2`; persistence-error telemetry
- ADRs 009, 010, 013; migration guide; tutorial article 9

### Changed

- `Rheo.Consumer` starts a Group via a bridge process (no poll GenServer)
- Backend callbacks take opaque `handle` instead of topology GenServer
- `Rheo.Event` no longer decodes Mongo documents

### Breaking

- Supervision options: prefer `name` + `backend` tuple; drop `supervisor_name`
- Query `:sort` removed; use `:order_by`
- Event document decoding removed from `Rheo.Event`; use
  `Rheo.Backend.Mongo.Codec.event_from_doc/1`

## [0.1.0]

Initial Mongo-backed MVP: streams, groups, leases, Consumer, docs, Livebook.

[0.6.0]: https://github.com/thanos/rheo/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/thanos/rheo/compare/v0.4.1...v0.5.0
[0.4.1]: https://github.com/thanos/rheo/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/thanos/rheo/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/thanos/rheo/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/thanos/rheo/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/thanos/rheo/releases/tag/v0.1.0
