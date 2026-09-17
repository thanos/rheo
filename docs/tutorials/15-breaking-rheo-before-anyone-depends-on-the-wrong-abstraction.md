# Breaking Rheo Before Anyone Depends on the Wrong Abstraction

v0.1–v0.7 were successful discovery releases. They proved consumer groups over
searchable databases, leases, partitions, frontiers, ETS, Mongo, Ecto, GenStage,
and Broadway.

v0.8 is not a feature dump. It is the moment to **break the abstractions that
would not survive Redis Streams** — while almost nobody depends on them yet.

## What stays

- Immutable events
- At-least-once delivery and lease fencing
- Per-partition logical `sequence`
- Contiguous ACK frontiers and lag
- Replay without copying events
- `Rheo.Consumer` as the friendly OTP surface
- Broadway as a pipeline, Rheo as the durable source

## What changes

- Handler callbacks return `:ack` / `{:retry, _}` / `{:reject, _}` — no fake
  GenServer state under concurrency
- One local group runtime per `{rheo, stream, group}` per node
- Lease `receipt` for native-stream settle identity
- Capabilities split into guarantees vs mechanisms
- Integration packages: `rheo_mongo`, `rheo_ecto`, `rheo_broadway`
- Shared inflight bookkeeping between Group and Producer

## Why Redis forces honesty

Redis already implements consumer groups. If Rheo's backend contract secretly
means "Mongo delivery rows", Redis becomes a second product. Model C keeps
portable sequences and lets Redis keep native IDs on the lease receipt.

## Read next

- [0.7 → 0.8 migration](https://hexdocs.pm/rheo/0-7-to-0-8.html)
- [ADR 019](https://hexdocs.pm/rheo/019-v0-8-architectural-reset.html)
- [Redis readiness spike](https://hexdocs.pm/rheo/redis-readiness-spike.html)
- [Flow readiness spike](https://hexdocs.pm/rheo/flow-readiness-spike.html)
