# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.10.0] - 2026-09-18

Ops surface for an embedded Rheo. Inventory, dead-letter (DLQ) listing, group
health, optional metrics/LiveDashboard, and Mix inspect tasks — without changing
Consumer / Event / Query semantics or inventing a control plane.

See [0.9 → 0.10 migration](https://hexdocs.pm/rheo/0-9-to-0-10.html) and
[ADR 027](https://hexdocs.pm/rheo/027-ops-surface.html).

### Added

- `Rheo.list_streams/1`, `Rheo.list_groups/2`, `Rheo.dead_letters/3`,
  `Rheo.group_info/3` plus `%Rheo.DeadLetter{}` / `%Rheo.GroupInfo{}`
- Backend optional callbacks implemented on ETS, Mongo, Ecto, Redis
- Optional `Rheo.Telemetry.Metrics` (requires `telemetry_metrics`)
- Optional `Rheo.LiveDashboard.Page` (requires `phoenix_live_dashboard`)
- Mix tasks: `rheo.streams`, `rheo.lag`, `rheo.dead_letters`, `rheo.group_info`,
  `rheo.bench`
- Ops Livebook (`notebooks/ops.livemd`); LiveDashboard Playground demo
  (`notebooks/live_dashboard.livemd`, `examples/live_dashboard_ops.exs`);
  ops guide, ADR 027, migration `0.9-to-0.10`

### Changed

- Roadmap: Ops is 0.10; Mnesia deferred to 0.11
- Version bump only for hosts that ignore the new inspect APIs

## [0.9.0] - 2026-09-18

Redis Streams native backend. v0.8 prepared Model C receipts and the semantic
contract; v0.9 proves them on Redis without changing `Rheo.Consumer`,
`Rheo.Event`, or `Rheo.Query` meaning.

See [0.8 → 0.9 migration](https://hexdocs.pm/rheo/0-8-to-0-9.html) and
[ADR 026](https://hexdocs.pm/rheo/026-redis-streams-backend.html).

### Added

- `Rheo.Backend.Redis` — optional `{:redix, "~> 1.5"}`; Redis Streams consumer
  groups with portable `event.sequence` and `lease.receipt` = entry id
- Fenced settle (fence hash + `XACK`); reclaim via `XPENDING`/`XCLAIM`
- `Rheo.Backend.Wakeup` + Group reader Task (ADR 025); polling remains fallback
- Redis guide, ADR 026, conformance suite tagged `:redis`
- Redis property + Broadway/Producer smoke suites; Article 16
- docker-compose `redis:7` service; `RHEO_REDIS_URL`

### Changed

- Version bump only for non-Redis hosts — no Consumer API breaks from 0.8

## [0.8.0] - 2026-09-17

Architectural reset. v0.1–v0.7 were successful discovery releases; v0.8
consolidates the abstractions learned from MongoDB, ETS, Ecto, partitions,
replay, GenStage, and Broadway, and validates that the same settlement model
can support Flow and native-stream backends such as Redis Streams — without
shipping Redis or Flow yet.

See [0.7 → 0.8 migration](https://hexdocs.pm/rheo/0-7-to-0-8.html) and
[ADR 019](https://hexdocs.pm/rheo/019-v0-8-architectural-reset.html).

### Added

- `%Rheo.Lease{}.receipt` — opaque backend-native settle identity (ADR 021)
- `Rheo.Backend.Capabilities` — validated guarantees vs mechanisms struct
  (ADR 023); unknown keys, non-boolean values, and disabled invariants raise
- `Rheo.Settle` — portable settlement vocabulary (`:stale_lease`,
  `:receipt_mismatch`, `:backend_unavailable`, `{:ambiguous, _}`,
  `{:failed, _}`, `{:invalid, _}`) and the `nack_after_failed_ack?/1` policy
- `Rheo.Inflight` (`renew_all/2`, `pop/2`, capacity) and `Rheo.Backoff`,
  shared by `Rheo.Group` and `Rheo.Producer`
- `Rheo.Producer.ack/3`, `nack/4`, `reject/4` — settle durably and release the
  producer's inflight entry in one call
- Telemetry `[:rheo, :handler, :error]` when a handler raises or throws
- Conformance suite grouped by guarantee; native-stream test double
  (`Rheo.Backend.NativeStreamDouble`) runs the full suite; flaky-backend double
  for settle failure injection
- Property tests on ETS (partition routing, sequences, frontier, group
  isolation, fencing, replay, paging); failure-injection tests (settle failures,
  backend crash, drain)
- Flow readiness tests (`Rheo.Producer` → Flow map / partition / reduce /
  window / crash-before-settle)
- Multi-instance ETS + SQLite isolation tests
- Livebook demos split into Quickstart, Concepts, Pipelines, and Backends
  (`notebooks/*.livemd`; `rheo_demo.livemd` is the index)
- Optional integrations (ADR 020): `mongodb_driver`, `ecto` / `ecto_sql`,
  `gen_stage`, and `broadway` are `optional: true`; `Rheo.Backend.Mongo`,
  `Rheo.Backend.Ecto`, `Rheo.Producer`, and `Rheo.Broadway` compile only when
  their dependency is present. `mix core.check` proves the core-only build
- ADRs 019–025; Flow and Redis readiness spikes; Article 15

### Changed

- `Rheo.Consumer` handlers return `:ack | {:retry, reason} | {:reject, reason}`
  and receive a read-only context; `setup/1` must return a map (ADR 022)
- `use Rheo.Consumer` is a child spec for `Rheo.Group`; the host supervisor owns
  the group and a second start returns `{:error, {:already_started, pid}}`
- Groups are registered under a local name; the per-instance `Registry` is gone
- `Rheo.Group.drain/2` and `Rheo.Producer.drain/2` no longer block the process
  while waiting; the group traps exits and drains on supervisor shutdown
- After a failed ACK the group nacks only definite failures; unavailable or
  ambiguous outcomes are left to lease expiry (fencing protects a committed ACK)
- `c:Rheo.Backend.capabilities/0` returns the struct; `Rheo.Backend.Ecto.capabilities/1` too
- Backends map driver errors into `Rheo.Settle` reasons instead of returning
  exception structs; `[:rheo, :fetch, :error]`, `[:rheo, :ack | :retry | :reject, :error]`,
  and renew `:result` metadata carry the classified reason
- `Rheo.Broadway.Acknowledger` settles through the producer helpers
- `{Rheo, opts}` requires `:backend`; the `url:` shorthand raises unless
  `Rheo.Backend.Mongo` is available
- `Rheo.Query.new/2` normalizes `order_by` directions `1` / `-1`
- Stored nack / reject reasons are truncated diagnostics, not verbatim terms
- Logs carry stream / group / event ids, not handler return values

### Removed

- `Rheo.Consumer.Bridge` and the silent multi-bridge join of a shared group
- Flat capability maps (`to_legacy_map`, `normalize`)
- The unused `Rheo.Backend.Wakeup` contract (ADR 025 is a proposal for v0.9)

## [0.7.1] - 2026-09-17

Documentation-only release. No runtime API changes. Prefer
`{:rheo, "~> 0.7.0"}` (or `"~> 0.7.1"`).

### Added

- Practical HexDocs **Guides**: Quick Start, Configuration, Consumer Groups,
  Enqueuing, Dequeuing, Replay, Querying, Partitions and lag, ETS, Mongo,
  Using Ecto, Broadway, GenStage, Building your own backend

### Changed

- HexDocs extras regrouped into collapsible groups:
  **Guides: Introduction / Advanced / Cookbook**, **Migrating from previous
  versions**, **Design: Architecture / ADRs / Tutorials**
- Mermaid diagrams render on HexDocs via ExDoc's `before_closing_body_tag` hook
- README documentation index updated for the new guide layout
- Livebook demo titled/versioned for **v0.7.1** (Broadway + Ecto sections match
  the v0.7 API)

## [0.7.0] - 2026-09-16

GenStage / Broadway interoperability. Additive — `Rheo.Consumer` and
`Rheo.Group` are unchanged. See
[0.6 → 0.7 migration](https://hexdocs.pm/rheo/0-6-to-0-7.html).

### Added

- `Rheo.Producer` — `GenStage` producer that turns demand into `Rheo.fetch/3`
  and emits `%Rheo.Lease{}`. Bounds unsettled leases with `:max_demand`, renews
  inflight leases every `lease_ms / 2`, drops stale leases for redelivery, polls
  when idle, and backs off exponentially on fetch errors. Options: `:rheo`,
  `:stream`, `:group`, `:max_demand`, `:lease_ms`, `:poll_ms`, `:consumer_id`,
  `:partitions`, `:on_failure`, `:name`
- `Rheo.Producer.confirm/2` — tells the producer a lease was settled durably so
  renewal stops and a demand slot is released
- `Rheo.Producer.drain/2`, `inflight_count/1`, `config/0`, and Broadway's
  `prepare_for_draining/1` callback
- `Rheo.Broadway.transform/2` — Broadway `:transformer` building a
  `%Broadway.Message{}` with `data: lease.event` and
  `metadata: %{lease:, stream:, group:, partition:, attempt:}`
- `Rheo.Broadway.Acknowledger` — successful → `Rheo.ack/2`; failed →
  `Rheo.nack/3`, or `Rheo.reject/3` with `on_failure: :reject` (settable per
  message via `Broadway.Message.configure_ack/2`)
- Telemetry `[:rheo, :producer, :start | :stop]` and
  `[:rheo, :broadway, :ack | :retry | :reject]`
- ADR [018](https://hexdocs.pm/rheo/018-broadway-genstage-interop.html); tutorial
  [article 14](https://hexdocs.pm/rheo/14-rheo-is-not-broadway-it-feeds-broadway.html)
- Livebook Broadway + ETS section;
  [0.6 → 0.7 migration](https://hexdocs.pm/rheo/0-6-to-0-7.html)

### Changed

- `gen_stage` and `broadway` are dependencies, because `Rheo.Producer` and
  `Rheo.Broadway.Acknowledger` compile against those behaviours rather than
  `Code.ensure_loaded?/1` guards. A `rheo_broadway` package split is deferred —
  see ADR 018 "Alternatives".
- ADR 007 now points at ADR 018: GenStage is still not used internally, but it is
  a supported consumption surface

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

[0.9.0]: https://github.com/thanos/rheo/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/thanos/rheo/compare/v0.7.1...v0.8.0
[0.7.1]: https://github.com/thanos/rheo/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/thanos/rheo/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/thanos/rheo/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/thanos/rheo/compare/v0.4.1...v0.5.0
[0.4.1]: https://github.com/thanos/rheo/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/thanos/rheo/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/thanos/rheo/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/thanos/rheo/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/thanos/rheo/releases/tag/v0.1.0
