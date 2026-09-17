# Consumer Groups

A **consumer group** is named progress over a stream: leases, retries,
dead-letters, and a contiguous ACK frontier — separate from the immutable event
log. Many groups can read the same events independently.

## Create a group

```elixir
:ok = Rheo.create_stream("market-events", partition_count: 4)
:ok = Rheo.create_group("market-events", "risk")
:ok = Rheo.create_group("market-events", "surveillance")
```

Optional start cursor (preferred replay path):

```elixir
:ok = Rheo.create_group("market-events", "risk-replay", start_after: 1_000)
```

## Competing consumers (same group)

Workers that share `{stream, group}` compete for leases. Each event is offered
to **one** worker at a time (until lease expiry / nack).

```elixir
{:ok, a} = Rheo.fetch("market-events", "risk", limit: 10, consumer_id: "risk-1")
{:ok, b} = Rheo.fetch("market-events", "risk", limit: 10, consumer_id: "risk-2")
# a and b claim disjoint available work
```

## Independent groups

Different group names progress separately on the same stream:

```elixir
{:ok, risk} = Rheo.fetch("market-events", "risk", limit: 5)
{:ok, surv} = Rheo.fetch("market-events", "surveillance", limit: 5)
# Same event ids can appear in both
```

## OTP surface

Prefer `Rheo.Consumer` so a local `Rheo.Group` owns demand, concurrency, and
lease renewal:

```elixir
defmodule MyApp.RiskConsumer do
  use Rheo.Consumer, stream: "market-events", group: "risk"

  @impl true
  def handle_event(event, state) do
    case Risk.process(event) do
      :ok -> :ack
      {:temporary, reason} -> {:retry, reason}
      {:permanent, reason} -> {:reject, reason}
    end
  end
end
```

Alternatively feed Broadway / GenStage with `Rheo.Producer` — still one group,
different runtime. Do **not** run both a Consumer and a Producer on the same
`{rheo, stream, group}` unless you want competing consumers by design.

## Partitions and lag

With `partition_count > 1`, sequences and ordering are **per partition**. Group
progress is a contiguous frontier; holes block lag advancement:

```elixir
{:ok, lag} = Rheo.lag("market-events", "risk")
lag.lag
lag.partitions[0].frontier
```

See [Dequeuing](dequeuing.html) and the tutorial
[ACKs are not a cursor](12-acks-are-not-a-cursor.html).
