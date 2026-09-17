# Building your own backend

Backends implement `Rheo.Backend`. Application code calls `Rheo`; the instance
dispatches to your adapter through an opaque **handle**.

## What you must implement

See the `@callback` docs on `Rheo.Backend`:

- Lifecycle and health: `child_spec/1`, `capabilities/0`, `ensure_indexes/1`,
  `ping/1`
- Streams and groups: `create_stream/3`, `create_group/4`
- Log: `append/4`, `append_batch/4`, `read/3`, `query/2`
- Delivery: `fetch/4`, `renew/3`, `ack/2`, `retry/3`, `reject/3`
- Group progress: `replay/4`, `reset_group/4`, `lag/4`

The callbacks name semantic operations. How you store deliveries is private: a
SQL table with `SKIP LOCKED`, a document collection with compare-and-set, or a
native consumer group such as Redis Streams' PEL.

## Capabilities

Return a validated `Rheo.Backend.Capabilities` struct:

```elixir
@impl true
def capabilities do
  Rheo.Backend.Capabilities.new(
    durable: true,
    distributed: true,
    partitions: true,
    contiguous_frontier: true,
    replay: true,
    atomic_compare_and_set: true,
    ordered_range_scan: true
  )
end
```

Guarantees gate conformance cases; mechanisms describe how you implement
delivery. `at_least_once` and `lease_fencing` cannot be declared `false`.

## Invariants

1. **Events are immutable** — ACK never deletes log rows.
2. **Leases are fenced** — every settle callback fails with
   `{:error, :stale_lease}` unless `lease_id` still matches the current claim.
   If you set `lease.receipt`, compare it with term equality and fail with
   `{:error, :receipt_mismatch}` when it differs.
3. **At-least-once** — expiry or crash may redeliver; do not claim exactly-once.
4. **Partitions** (if declared) — per-partition sequences and a contiguous
   frontier for `lag/4`.
5. **Errors are portable** — map driver failures into the `Rheo.Settle`
   vocabulary: `:backend_unavailable` when the store cannot be reached,
   `{:failed, cause}` for definite failures, `{:ambiguous, cause}` when the
   outcome is unknown. Never return driver exception structs.

## Conformance

Run the shared contract against your adapter:

```elixir
defmodule MyBackendContractTest do
  use ExUnit.Case, async: false
  use Rheo.BackendContract, backend: MyBackend, backend_opts: [name: :my_backend]
end
```

The suite (`test/support/backend_contract.ex`, ADR 012 / ADR 024) is grouped by
guarantee: lifecycle, event log, queries, consumer groups, leases and fencing,
retry and reject, replay, partitions and frontier. Correctness cases always
run; only cases for guarantees you do not declare are skipped. Shipping a
backend without the contract suite is unsupported.

## Reference implementations

| Module | Role |
|---|---|
| `Rheo.Backend.ETS` | In-memory reference (ephemeral) |
| `Rheo.Backend.Mongo` | Durable document store |
| `Rheo.Backend.Ecto` | Durable SQL via a host-owned Repo |
| `Rheo.Backend.NativeStreamDouble` (test support) | Native-stream shape: receipts, native reclaim |

Start from ETS to learn the state machine. For a native-stream store, start
from the double: it keeps the portable `event.sequence` and fences on both
`lease_id` and `receipt`.

ADRs: [005](005-backend-boundary.html), [021](021-logical-sequence-and-native-delivery-receipts.html),
[023](023-backend-capabilities-v2.html), [024](024-backend-contract-v2.html).
