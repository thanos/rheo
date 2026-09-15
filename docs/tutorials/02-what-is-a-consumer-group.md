# Article 2 — What Is a Consumer Group?

In Rheo:

- a **stream** is an ordered (per partition) append-only event log
- a **consumer group** independently tracks progress on that stream
- workers in the same group **compete** for leases
- workers in different groups each see the full stream

```text
market-events
  ├── risk
  │     ├── worker A
  │     └── worker B
  └── surveillance
        ├── worker C
        └── worker D
```

## Lifecycle

1. `Rheo.create_stream/1`
2. `Rheo.append/2` writes immutable events
3. `Rheo.create_group/2` registers group state
4. `Rheo.fetch/3` leases work
5. `Rheo.ack/1` / `Rheo.nack/2` / `Rheo.reject/2` record outcomes

Cursors and leases live in Mongo `groups` and `deliveries` collections — see
`Rheo.Backend.Mongo`.
