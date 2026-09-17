# ADR 024: Backend contract v2

## Status

Accepted (v0.8.0)

## Context

`Rheo.Backend` grew as a mirror of Mongo delivery-row algorithms. Redis Streams
already provide consumer-group primitives; a contract that requires "materialize
delivery documents" cannot host Redis honestly.

## Decision

1. `Rheo.Backend` callbacks are semantic: log (`append`, `read`, `query`),
   delivery (`fetch`, `renew`, `ack`, `retry`, `reject`), group progress
   (`replay`, `reset_group`, `lag`), lifecycle and health (`child_spec`,
   `capabilities`, `ensure_indexes`, `ping`). The callback list is unchanged
   from v0.7; the contract text now states what each must guarantee.
2. Storage algorithms stay private to each adapter.
3. Settle callbacks fence on `lease_id` and, when the adapter sets it, on
   `lease.receipt` with term equality ([ADR 021](021-logical-sequence-and-native-delivery-receipts.html)).
4. Adapters map driver errors into the `Rheo.Settle` vocabulary
   (`:backend_unavailable`, `{:failed, cause}`, `{:ambiguous, cause}`) instead
   of returning driver exceptions.
5. The executable contract is `test/support/backend_contract.ex`, organized by
   guarantee, run by ETS, Mongo, Ecto (SQLite and PostgreSQL), and the
   native-stream double.

## Consequences

- Backend authors map old callbacks 1:1 where semantics match; internals may
  rewrite freely.
- ADR 005 remains the origin of the boundary; this ADR refines its intent.

## Related

ADR 005, 012, 021, 023.
