# Architecture

Rheo is an embedded Elixir/OTP library. Durable truth lives in MongoDB. OTP owns
process lifecycle and concurrency, not consumer-group correctness.

```text
Application Supervision Tree
        |
        +-- Rheo (Supervisor)
        |     |
        |     +-- Mongo topology
        |
        +-- RiskConsumer (Rheo.Consumer)
        +-- SurveillanceConsumer (Rheo.Consumer)
                 |
                 v
           Rheo.Backend.Mongo
                 |
                 v
              MongoDB
```

## Core invariant

| Concept | Storage | Mutability |
|---|---|---|
| Event | `events` | Immutable |
| Stream sequence | `streams` | Monotonic counter |
| Consumer group | `groups` | Cursor / policy |
| Lease / ACK / DLQ | `deliveries` | Mutable per group+event |

Consumption never deletes events. An event may be processed independently by many
groups.

## Delivery

At-least-once with lease fencing:

1. `fetch` claims work with a unique `lease_id`
2. `ack` succeeds only if the lease token still matches
3. Expired leases become eligible for redelivery
4. Stale consumers cannot ACK a newer lease

## Demand

MVP consumers bound outstanding work with `:max_demand` / fetch `limit`.
Rheo does not reimplement GenStage.

## Backend boundary

`Rheo.Backend` defines the operations Rheo uses. `Rheo.Backend.Mongo` is the only
MVP implementation.

## Try it

- CLI: `mix rheo.demo`
- Interactive: [notebooks/rheo_demo.livemd](../notebooks/rheo_demo.livemd)
