# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
