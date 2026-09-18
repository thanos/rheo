# Dequeuing

“Dequeuing” in Rheo means **fetching a lease**, doing work, then settling it.
Events stay in the log; only per-group delivery state changes.

## Low-level loop

```elixir
{:ok, leases} =
  Rheo.fetch("market-events", "risk",
    limit: 10,
    consumer_id: "risk-1",
    lease_ms: 30_000
  )

Enum.each(leases, fn lease ->
  case Risk.process(lease.event) do
    :ok -> Rheo.ack(lease)
    {:retry, reason} -> Rheo.nack(lease, reason)
    {:reject, reason} -> Rheo.reject(lease, reason)
  end
end)
```

## Leases and fencing

A lease carries a `lease_id` fencing token. Only the current holder can ACK /
NACK / reject. After expiry, another worker may claim the same event; a stale
ACK returns an error (typically `{:error, :stale_lease}`).

Renew long work explicitly, or let `Rheo.Group` / `Rheo.Producer` renew for you:

```elixir
{:ok, lease} = Rheo.renew(lease)
```

## Outcomes

| Call | Meaning |
|---|---|
| `Rheo.ack/2` | Success — advances contiguous frontier when no holes remain |
| `Rheo.nack/3` | Temporary failure — retry until `max_attempts`, then DLQ |
| `Rheo.reject/3` | Permanent failure — dead-letter immediately for this group |

Delivery is **at-least-once**. After crashes or expiry the same event may be
delivered again — make handlers idempotent on `event.id`.

## Prefer `Rheo.Consumer`

```elixir
def handle_event(event, _context) do
  case Risk.process(event) do
    :ok -> :ack
    {:temporary, reason} -> {:retry, reason}
    {:permanent, reason} -> {:reject, reason}
  end
end
```

The Group owns fetch, demand, concurrency, and renewal.

## Partitions

```elixir
Rheo.fetch(stream, group, partition: 0, limit: 10)
# or Consumer/Producer: partitions: [0, 1]
```

## Lag and the frontier

ACKs are **not** a free cursor. A hole (ACK 1 and 3 while 2 is inflight) keeps
the frontier at 1 until 2 is terminal:

```elixir
{:ok, lag} = Rheo.lag(stream, group)
# lag.partitions[p].frontier — contiguous committed progress
# lag.lag — aggregate high-water minus frontier
```

See [ACKs are not a cursor](12-acks-are-not-a-cursor.html).
