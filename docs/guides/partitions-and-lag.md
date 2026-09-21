# Partitions and lag

From v0.5, a stream may declare multiple partitions. Ordering and sequences are
**per partition**; there is no global order across partitions.

## Create a partitioned stream

```elixir
:ok = Rheo.create_stream("market-events", partition_count: 4)
```

Append with a routing key (or explicit `:partition`):

```elixir
{:ok, event} =
  Rheo.append("market-events", %{type: "curve_update", key: "EUR-1", price: 1.0})

event.partition
event.sequence
```

Key routing uses `:erlang.phash2/2`. Same key → same partition.

## Contiguous frontier

Group progress is the highest contiguous terminal sequence per partition. ACK
holes do not advance the frontier:

```text
ACK 1001, inflight 1002, ACK 1003  →  frontier stays at 1001
```

```elixir
{:ok, lag} = Rheo.lag("market-events", "risk")

lag.partitions[0].frontier
lag.partitions[0].high_water
lag.lag
```

## Fetch by partition

```elixir
Rheo.fetch(stream, group, partition: 0, limit: 10)

# Consumer / Producer
partitions: [0, 1]
# or partitions: :all
```

Design: [ADR 016](016-partitions-and-ack-frontier.html),
tutorial [ACKs are not a cursor](12-acks-are-not-a-cursor.html),
and the partitions diagram in [Architecture](architecture.html).
