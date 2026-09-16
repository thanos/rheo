# If Rheo Really Is Database-Agnostic, Prove It with ETS

Rheo 0.2 cleaned the architecture: opaque backend handles, a local `Rheo.Group`,
and a portable `%Rheo.Query{}`. That is necessary but not sufficient. An
abstraction that only has one implementation is a hope.

**v0.3.0** ships `Rheo.Backend.ETS` and a shared conformance suite so the
contract is exercised twice.

## Why a second backend is the real test

Mongo shaped the first callbacks: collections, indexes, `find_one_and_update`.
If ETS cannot implement the same public flows — create stream, append, fetch,
renew, ACK, retry, reject, query — then the behaviour still models Mongo, not
Rheo.

ETS is deliberately weak as a store (`durable: false`). That is the point: if
the *semantics* still hold in memory, the semantics belong to Rheo.

## Opaque handles vs topology

A Rheo instance starts whatever `child_spec/1` the backend returns. Mongo’s
handle is a driver process name. ETS’s handle is an owner GenServer that owns
per-instance tables. Callers never pass table refs or Mongo URLs through
`Rheo.fetch/3` — only `rheo:` instance names.

## Capabilities without forked correctness

`c:Rheo.Backend.capabilities/0` declares honest differences:

| Flag | Mongo | ETS |
|---|---|---|
| `durable` | true | false |
| `secondary_indexes` | true | false |
| `atomic_compare_and_set` | true | true |

Capabilities gate **optional** future suites (replay, partitions). They must not
make lease fencing optional. Both backends must refuse a stale ACK.

## Conformance, not copy-paste

The BackendContract ExUnit template (`test/support/backend_contract.ex`) injects
the same cases into:

- ETS contract tests (always on in CI)
- Mongo contract tests (`@tag :mongo`)

When a test fails on one backend only, you found an abstraction leak.

## Ownership and restart

ETS tables die with the owner process. Restarting the ETS child empties the
log. Document that. Do not pretend ETS is a production durable backend.

## What we still refuse to weaken

- At-least-once delivery with fencing tokens
- Immutable events; mutable deliveries per group
- Portable query shape (`order_by`, not Mongo `:sort`)
- Consumption never deletes events

## What comes next

Search, replay, and lineage (0.4) build on a contract that two backends already
satisfy. See [ADR 011](../adr/011-backend-capabilities.md),
[ADR 012](../adr/012-backend-conformance-suite.md), and
[ADR 014](../adr/014-ets-backend.md).
