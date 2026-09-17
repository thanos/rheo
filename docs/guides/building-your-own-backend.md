# Building your own backend

Backends implement `Rheo.Backend`. Application code calls `Rheo`; the instance
dispatches to your adapter through an opaque **handle**.

## What you must implement

See `@callback` docs on `Rheo.Backend`. At minimum:

- `child_spec/1`, `capabilities/0`, `ensure_indexes/1`, `ping/1`
- Stream / group lifecycle: `create_stream/3`, `create_group/4`
- Log: `append/4`, `append_batch/4`, `read/3`, `query/2`
- Delivery: `fetch/4`, `renew/3`, `ack/2`, `retry/3`, `reject/3`
- Replay / lag helpers required by the version you target

Declare capabilities honestly (`:durable`, `:distributed`, `:partitions`,
`:contiguous_frontier`, …). Capabilities document and gate conformance — they
must not weaken fencing.

## Invariants

1. **Events are immutable** — ACK never deletes log rows.
2. **Leases are fenced** — settle only when `lease_id` matches.
3. **At-least-once** — expiry / crash may redeliver; do not claim exactly-once.
4. **Partitions** (if advertised) — per-partition sequences; contiguous frontier
   for lag.

## Conformance

Run the shared backend contract tests against your adapter (see
`test/support/backend_contract.ex` and ADR 012). Shipping a backend without the
contract suite is unsupported.

## Reference implementations

| Module | Role |
|---|---|
| `Rheo.Backend.ETS` | In-memory reference (ephemeral) |
| `Rheo.Backend.Mongo` | Durable document store |
| `Rheo.Backend.Ecto` | Durable SQL via host Repo |

Start from ETS to learn the state machine, then map storage primitives onto your
database’s atomic compare-and-set / locking tools.

ADRs: [005](005-backend-boundary.html), [011](011-backend-capabilities.html),
[012](012-backend-conformance-suite.html).
