# ADR 007: Demand and backpressure

## Status

Accepted (v0.1.0). GenStage interop is [ADR 018](018-broadway-genstage-interop.html); the settlement boundary is refined by [ADR 022](022-consumer-runtime-and-handler-state.html) (v0.8.0).

## Context

Unbounded polling floods consumers and holds too many leases. GenStage already
solves demand-driven pipelines, but wrapping Rheo in GenStage is not required to
prove the core hypothesis.

## Decision

MVP consumers use bounded fetch via `:max_demand` / `limit`. Poll when idle.
Do not integrate GenStage internally for the first release.

## Alternatives

- GenStage producer-consumer integration
- Mongo change streams push model

## Consequences

- Simple, explicit capacity control
- Architecture leaves room for demand evolution
- Not a GenStage clone

## Superseded in part by ADR 018

v0.7.0 adds `Rheo.Producer`, a GenStage producer that converts demand into
`Rheo.fetch/3` calls, plus a Broadway transformer and acknowledger. GenStage is
still not used *internally* — `Rheo.Consumer` / `Rheo.Group` keep the bounded
fetch and idle poll described above. The producer is an additional consumption
surface for the same durable group. See
[ADR 018](https://hexdocs.pm/rheo/018-broadway-genstage-interop.html).
