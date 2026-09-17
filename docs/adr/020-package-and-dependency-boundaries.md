# ADR 020: Package and dependency boundaries

## Status

Accepted (v0.8.0)

## Context

v0.7 made `ecto_sql`, `gen_stage`, and `broadway` hard dependencies so
behaviours compile without guards. That forces every ETS-only user to download
Mongo/SQL/Broadway stacks. Redis in v0.9 would worsen the problem.

## Decision

Publish four Hex packages from one repository:

| Package | Contains |
|---|---|
| `rheo` | Core OTP, Query, Consumer/Group, ETS backend |
| `rheo_mongo` | `Rheo.Backend.Mongo` + `mongodb_driver` |
| `rheo_ecto` | `Rheo.Backend.Ecto` + `ecto` / `ecto_sql` |
| `rheo_broadway` | `Rheo.Producer`, `Rheo.Broadway`, deps on `gen_stage` + `broadway` |

Dependency direction:

```text
rheo_mongo ----\
                --> rheo
rheo_ecto -----/
rheo_broadway --> rheo   (and gen_stage/broadway)
```

No `rheo_flow` package in v0.8.

Root CI remains a single entry (`mix test` / `mix ci`) via umbrella or path
deps + aliases.

During the mechanical move, the monolithic tree may temporarily keep optional
deps while packages are extracted — end state is separate Hex packages.

## Consequences

- App `mix.exs` lists only needed integrations.
- v0.9 `rheo_redis` depends on `rheo` only.
- Migration guide documents before/after deps.

## Alternatives

- Single package with optional deps (rejected as end state; acceptable only as
  transitional scaffolding).
- Many micro-packages (rejected — too much ceremony).
