# ADR 004: Lease and fencing model

## Context

Without fencing, a slow consumer can ACK after another worker has already claimed
the same delivery, corrupting progress.

## Decision

Each fetch issues a new opaque `lease_id`. ACK/NACK/reject require matching
`lease_id` and `status == leased`. Expired leases are reclaimable via conditional
`findOneAndUpdate`.

## Alternatives

- Soft locks without tokens (unsafe)
- Fencing tokens monotonically increasing per delivery (equivalent; UUID is enough)

## Consequences

- Stale ACK returns `{:error, :stale_lease}`
- Redelivery increments `attempt`
- Tests inject a frozen clock instead of sleeping
