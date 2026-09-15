# ADR 003: MongoDB first backend

## Context

Rheo's hypothesis needs one working searchable database backend. MongoDB provides
document storage, secondary indexes, and atomic find-and-modify operations suitable
for leases and sequence allocation.

## Decision

Ship `Rheo.Backend.Mongo` only for the MVP. Introduce a small `Rheo.Backend`
behaviour for operations Rheo actually uses. Do not build capability negotiation
until a second backend exists.

## Alternatives

- PostgreSQL first (also viable; deferred)
- Abstract multi-backend framework up front (premature)

## Consequences

- Faster MVP proof
- Adapter boundary may need revision when adding PostgreSQL/Scylla
- Mongo-specific details stay inside the adapter
