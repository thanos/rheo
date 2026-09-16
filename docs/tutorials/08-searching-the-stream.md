# Article 8 — Searching the Stream

After consumers ACK events, Rheo can still answer investigative questions:

```elixir
Rheo.query("market-events",
  type: "curve_update",
  currency: "EUR",
  curve: "EUR-EURIBOR-6M"
)

Rheo.query("market-events", correlation_id: "abc")
Rheo.query("market-events", producer: "pricing-service-v3")
```

This is Rheo's differentiator versus “queue then delete” systems: the log remains
the system of record for what happened.

For pagination, streaming, and re-consuming history without copying events, see
[Article 11 — Search and Replay](11-search-and-replay-the-event-history.md).
