# ADR 007: Demand and backpressure

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
