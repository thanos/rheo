# Enqueuing

Rheo does not have a separate “queue”: you **append** immutable events to a
stream. Consumption never deletes them.

## Append one event

```elixir
{:ok, event} =
  Rheo.append("market-events", %{
    type: "curve_update",
    key: "EUR-1",
    price: 1.02
  })

event.id
event.sequence
event.partition
```

## Routing

| Option | Effect |
|---|---|
| `:key` in payload (or opts) | Routed with `:erlang.phash2/2` into a partition |
| `:partition` | Explicit partition id |
| neither | Partition `0` on single-partition streams; still valid on multi |

Sequences are **per partition**. There is no global order across partitions.

```elixir
:ok = Rheo.create_stream("market-events", partition_count: 4)

{:ok, e1} = Rheo.append("market-events", %{type: "t", key: "EUR-A"})
{:ok, e2} = Rheo.append("market-events", %{type: "t", key: "EUR-A"})
# same key → same partition; sequences 1 then 2 on that partition
```

## Batch append

```elixir
{:ok, events} =
  Rheo.append_batch("market-events", [
    %{type: "curve_update", key: "USD-1", price: 1.0},
    %{type: "curve_update", key: "USD-1", price: 1.1}
  ])
```

Batch writes assign contiguous sequences within each partition touched.

## Lineage metadata

Tag events for later search with `Rheo.Event.Lineage`:

```elixir
alias Rheo.Event.Lineage

meta =
  Lineage.put(%{},
    correlation_id: "corr-42",
    causation_id: "cmd-7",
    producer: "pricing-service",
    schema: "curve_update",
    schema_version: "1"
  )

{:ok, _} =
  Rheo.append("market-events", %{
    type: "curve_update",
    currency: "EUR",
    metadata: meta
  })
```

Lineage fields are queryable — see [Querying](querying.html).

## Read without consuming

```elixir
{:ok, events} = Rheo.read("market-events", after: 0, limit: 100)
```

`read/2` does not touch group state. Use `query` for filters.
