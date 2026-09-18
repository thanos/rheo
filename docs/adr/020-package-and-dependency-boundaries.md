# ADR 020: Package and dependency boundaries

## Status

Accepted (v0.8.0)

## Context

v0.7 made `mongodb_driver`, `ecto_sql`, `gen_stage`, and `broadway` hard
dependencies so integration modules compile unconditionally. Every ETS-only
user downloaded the Mongo, SQL, and Broadway stacks, and each new backend
(Redis in v0.9) would add another driver for everyone.

Two ways to stop that: publish separate Hex packages (`rheo_mongo`,
`rheo_ecto`, `rheo_broadway`), or keep one package with optional dependencies
and compile-time guards, the pattern `ex_arrow` uses for Explorer, Nx, Flow,
GenStage, Broadway, and ADBC.

## Decision

One Hex package, optional integrations:

| Integration | Enable with | Modules compiled |
|---|---|---|
| MongoDB | `{:mongodb_driver, "~> 1.5"}` | `Rheo.Backend.Mongo` and `Rheo.Backend.Mongo.*` |
| PostgreSQL / SQLite | `{:ecto_sql, "~> 3.11"}` plus `postgrex` or `ecto_sqlite3` | `Rheo.Backend.Ecto`, `Rheo.Backend.Ecto.*`, `mix rheo.ecto.gen_migration` |
| GenStage | `{:gen_stage, "~> 1.2"}` | `Rheo.Producer` |
| Broadway | `{:broadway, "~> 1.2"}` | `Rheo.Broadway`, `Rheo.Broadway.Acknowledger`, and the `Broadway.Producer` callback on `Rheo.Producer` |

Each integration file is wrapped in `if Code.ensure_loaded?(Dep) do … end`,
so the module exists only when the host lists the dependency. Mix recompiles
`rheo` when a host adds an optional dependency later. Core (`Rheo`,
`Rheo.Consumer`, `Rheo.Group`, `Rheo.Query`, `Rheo.Backend`,
`Rheo.Backend.ETS`, `Rheo.Inflight`, `Rheo.Settle`) depends on `telemetry`
and `jason` only, and references an integration module only through the
`Rheo.Backend` behaviour or the runtime-resolved `{Rheo, url: ...}` shorthand,
which raises `ArgumentError` when `Rheo.Backend.Mongo` is absent.

The guard is proven, not asserted: `mix core.check` (`priv/scripts/check_core_only.sh`)
builds a throwaway project that depends on this checkout with no optional
dependency, compiles it with warnings as errors, verifies that the guarded
modules are absent, and runs the ETS backend. It is part of `mix ci` and CI.

Dependency direction:

```text
telemetry, jason  <--  rheo core  <--  guarded integrations  <--  optional deps
```

A v0.9 Redis backend follows the same rule: `Rheo.Backend.Redis` guarded on
`Redix`, `{:redix, ..., optional: true}`.

## Consequences

- `{:rheo, "~> 0.8.0"}` plus the integrations a host actually uses; nothing
  else is downloaded.
- Calling a guarded module without its dependency raises
  `UndefinedFunctionError`; each guarded moduledoc states the requirement.
- Development and test environments of Rheo itself fetch every optional
  dependency, so `mix ci` exercises all integrations.
- No multi-package repository, no per-package versioning.

## Alternatives

- Separate Hex packages — rejected: four mix files, four publishes, version
  pinning between them, and a first-release ordering problem (a package cannot
  lock against an unpublished core), for a library whose integrations are a
  few files each.
- Required dependencies (v0.7) — rejected: the cost lands on every user.
