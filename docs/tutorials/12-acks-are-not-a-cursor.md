# ACKs Are Not a Cursor: Building a Correct Commit Frontier

At-least-once delivery means you can ACK event 1003 while 1002 is still
inflight. If “progress” were “max acked sequence,” lag would lie and replay
would skip holes. Rheo v0.5 keeps two ideas separate.

## Materialization cursor vs committed frontier

| Concept | Meaning |
|---|---|
| **Materialization cursor** | How far the group has **offered** events into deliveries |
| **Committed frontier** | Highest `F` such that every sequence through `F` is terminal (`acked` or `rejected`) |

```text
1001 ACK
1002 inflight
1003 ACK
1004 ACK

frontier = 1001   # blocked by 1002
```

When 1002 ACKs (or is dead-lettered), the frontier walks forward to 1004.

## Partitions

Sequences are **per partition**. Key routing uses `:erlang.phash2/2`:

```elixir
Rheo.create_stream("market-events", partition_count: 4)

Rheo.append("market-events", %{type: "curve_update", key: "EUR-EURIBOR-6M", price: 2.9})
# → partition = phash2("EUR-EURIBOR-6M", 4), sequence local to that partition
```

There is **no global order** across partitions. A Group may own a static set:

```elixir
{MyApp.RiskConsumer, partitions: [0, 1], concurrency: 4}
```

## Lag

```elixir
{:ok, lag} = Rheo.lag("market-events", "risk")
lag.partitions[0]
# => %{frontier: 10, high_watermark: 40, lag: 30}
lag.lag
# => sum of per-partition lags
```

## Replay stays partition-aware

```elixir
Rheo.replay("market-events", "risk", from_sequence: 1000, partition: 2)
Rheo.reset_group("market-events", "risk", confirm: true, partition: 2)
```

## What we still defer

Automatic partition rebalancing and cluster ownership — static `:partitions`
is ownership groundwork only. See
[ADR 016](https://hexdocs.pm/rheo/016-partitions-and-ack-frontier.html).
