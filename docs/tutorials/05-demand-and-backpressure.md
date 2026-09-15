# Article 5 — Demand, Backpressure, and Database Consumers

Uncontrolled polling can:

- hold thousands of leases
- overload handlers
- create avoidable redelivery storms

Rheo MVP bounds work:

```elixir
use Rheo.Consumer,
  stream: "market-events",
  group: "risk",
  max_demand: 10
```

`max_demand` maps to fetch `limit`. Consumers poll when idle; they do not open an
unbounded cursor of leased work.

GenStage remains a possible future integration, not an internal dependency. See
ADR 007.
