# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - 2026-09-16

Search, pagination/streaming, replay/reset, and event lineage — additive over
v0.3.0. See [0.3 → 0.4 migration](docs/migrations/0.3-to-0.4.md).

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
- ADR [015](docs/adr/015-replay-semantics.md); tutorial
  [article 11](docs/tutorials/11-search-and-replay-the-event-history.md)
- Conformance + unit coverage for search/replay on ETS and Mongo

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
- Backend conformance suite (`Rheo.BackendContract`) for ETS and Mongo
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

[0.4.0]: https://github.com/thanos/rheo/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/thanos/rheo/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/thanos/rheo/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/thanos/rheo/releases/tag/v0.1.0
