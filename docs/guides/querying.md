# Querying

Events remain searchable after ACK. Query by payload fields, lineage metadata,
and sequence bounds — without touching consumer-group state.

## Filters and bounds

```elixir
{:ok, eur} =
  Rheo.query("market-events",
    type: "curve_update",
    currency: "EUR",
    limit: 50
  )

{:ok, mid} =
  Rheo.query("market-events",
    after_sequence: 10,
    until_sequence: 15,
    order_by: [sequence: :asc]
  )

{:ok, by_corr} =
  Rheo.query("market-events", correlation_id: "corr-1", limit: 20)
```

Filters are portable across backends via `Rheo.Query` (see ADR 013). Capability
flags describe what each backend indexes well; fencing semantics stay the same.

## Pagination

`query_page/2` returns an opaque cursor (no SQL `OFFSET`):

```elixir
{:ok, page1} = Rheo.query_page("market-events", type: "curve_update", limit: 100)

{:ok, page2} =
  Rheo.query_page("market-events",
    type: "curve_update",
    limit: 100,
    cursor: page1.next_cursor
  )
```

## Streaming

```elixir
"market-events"
|> Rheo.stream_query(type: "curve_update", limit: 200)
|> Stream.map(& &1.id)
|> Enum.take(1_000)
```

`stream_query/2` pages internally with a bounded page size.

## Lineage helpers

```elixir
alias Rheo.Event.Lineage

Lineage.get(event, :correlation_id)
Lineage.get(event, :producer)
```

## Read vs query vs fetch

| API | Mutates group state? | Use for |
|---|---|---|
| `read/2` | No | Sequence-range scan |
| `query` / `query_page` / `stream_query` | No | Filtered history |
| `fetch/3` | Yes (leases) | Consumption |

Deeper walkthrough: [Searching the stream](08-searching-the-stream.html).
