# v0.7 architecture review (Stage 0)

Baseline: **v0.7.1** (runtime API = v0.7.0), tag `v0.7.1`, commit `74af418`.

## Baseline verification

| Check | Result |
|---|---|
| `mix compile --warnings-as-errors` | clean |
| `mix format --check-formatted` | clean |
| `mix credo --strict` | no issues |
| `mix dialyzer` | clean (two `pattern_match` ignores for `mongodb_driver` typespecs in `.dialyzer_ignore.exs`) |
| `mix docs --warnings-as-errors` | clean |
| `mix test` (Mongo and PostgreSQL up) | 366 passed, 21 excluded (`:integration`), about 37 s |
| `mix test` (no Mongo) | 273 passed; Mongo-tagged cases excluded |
| Coverage | 91.4% (`mix coveralls`) |
| Integration dependencies | MongoDB 7, PostgreSQL 16 (`docker-compose.yml`); SQLite always |

## What exists and is correct

- Opaque `Rheo.Backend.handle`
- Named multi-instance `Rheo` + `Rheo.Instance`
- Partitions, frontier, `Rheo.lag/3`
- Portable `Rheo.Query` / `query_page` / `stream_query`
- Conformance suite for ETS / Mongo / Ecto
- Broadway acknowledger + `Rheo.Producer`

## Findings by priority

| Priority | Finding | Evidence | v0.8 action |
|---|---|---|---|
| P0 correctness | None found; at-least-once, fencing, frontier held under the suite | conformance + property tests | keep |
| P1 architectural | Group and Producer duplicate renew / backoff / drain | `lib/rheo/group.ex`, `lib/rheo/producer.ex` | `Rheo.Inflight`, `Rheo.Backoff` |
| P1 architectural | Backend behaviour reads as Mongo delivery-row algorithms | `lib/rheo/backend.ex` docs | semantic contract text, native-stream double |
| P2 public API | Handler returns `{outcome, state}` under `concurrency > 1` | `Rheo.Consumer`, ADR 009 | read-only context (ADR 022) |
| P2 public API | Bridge silently joins an existing group (`shared: true`) | `Rheo.Consumer.Bridge` | one owner per node, no bridge |
| P3 portability | Lease has only `lease_id`; no place for a native claim id | `Rheo.Lease` | `receipt` (ADR 021) |
| P3 portability | Flat boolean capabilities mix guarantees with mechanisms | `capabilities/0` | typed struct (ADR 023) |
| P4 OTP lifecycle | Two supervisors own one group runtime (bridge + DynamicSupervisor) | `Rheo.Consumer.Bridge` | host-owned `Rheo.Group` |
| P5 dependencies | `mongodb_driver`, `ecto_sql`, `gen_stage`, `broadway` are hard deps | `mix.exs` | stage `apps/*` layout (ADR 020) |
| P6 observability | Settle errors emitted as raw driver terms | telemetry metadata | `Rheo.Settle` vocabulary |
| P8 documentation | Prose warns about state races instead of the API preventing them | consumer docs | callback shape change |

## Proposed breaking changes

1. Consumer callback outcomes and context
2. Duplicate local consumer → error
3. Capabilities shape (guarantees vs mechanisms)
4. Settlement error atoms in telemetry
5. Explicit `:backend` on the instance

## First implementation unit after Stage 0 docs

ADR 020 + 021 (package layout and the `receipt` field with backend
pass-through), then the capabilities struct.
