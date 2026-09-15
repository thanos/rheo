# Article 4 — Building Rheo as an Elixir/OTP Library

Rheo embeds into an application supervision tree:

```elixir
children = [
  {Rheo, url: "mongodb://localhost:27017/rheo"},
  {MyApp.RiskConsumer, max_demand: 8}
]
```

## What belongs in processes

| Process | Owns |
|---|---|
| Mongo topology | Connection pool / topology |
| `Rheo.Consumer` GenServer | Poll loop, demand bound, handler state |

## What does not

Consumer-group truth: cursors, leases, ACKs. Those are in MongoDB so a process
restart does not lose correctness.

This mirrors libraries like Oban: OTP for lifecycle, database for durable
coordination.
