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

GenStage is still not used *internally* — `Rheo.Group` keeps the bounded fetch and
idle poll described above (ADR 007). Since v0.7 there is also a GenStage producer,
`Rheo.Producer`, which applies the same `max_demand` bound to downstream demand so
a Broadway pipeline can consume the group. See
[ADR 018](https://hexdocs.pm/rheo/018-broadway-genstage-interop.html) and
[Article 14](https://hexdocs.pm/rheo/14-rheo-is-not-broadway-it-feeds-broadway.html).
