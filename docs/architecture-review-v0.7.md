# v0.7 architecture review (Stage 0)

Baseline: **v0.7.1** (runtime API = v0.7.0). Unit/ETS suite: 273 passed
(mongo/integration/postgres excluded).

## What exists and is correct

- Opaque `Rheo.Backend.handle`
- Named multi-instance `Rheo` + `Rheo.Instance`
- Partitions, frontier, `Rheo.lag/3`
- Portable `Rheo.Query` / `query_page` / `stream_query`
- Conformance suite for ETS / Mongo / Ecto
- Broadway acknowledger + `Rheo.Producer`

## Concrete refactor targets

| Target | Evidence | v0.8 action |
|---|---|---|
| Fat backends | 1.2–1.4k LOC × 3 | Thin contract; keep algorithms private |
| Duplicate lease runtime | Group + Producer renew/inflight/drain | `Rheo.Inflight` (+ related) shared |
| Hard integration deps | mix.exs | Package split / optional |
| Handler state | `:ack` + concurrency | Option A context |
| Bridge sharing | `shared: true` join | Single owner per node |
| Native stream settle | Lease has only `lease_id` | Add `:receipt` |

## Proposed breaking changes

1. Consumer callback outcomes and state model
2. Duplicate local consumer → error
3. Dependency / package layout
4. Capabilities shape (guarantees vs mechanisms)
5. Possible settlement error atoms / telemetry

## First implementation unit after Stage 0 docs

ADR 020 + 021 (package + Model C receipt field on `%Rheo.Lease{}` with
backend pass-through), then Capabilities module.
