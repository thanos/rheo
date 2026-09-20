# Search and Replay the Event History

Queues that delete on ACK cannot answer “what happened last Tuesday?” Rheo keeps
an immutable event log and separate per-group delivery state, so search and
replay are first-class.

## Search

```elixir
Rheo.query("market-events",
  type: "curve_update",
  currency: "EUR",
  after_sequence: 100,
  until_sequence: 500,
  from: ~U[2026-01-01 00:00:00Z],
  order_by: [sequence: :asc],
  limit: 50
)
```

Pagination without SQL `OFFSET`:

```elixir
{:ok, page} = Rheo.query_page("market-events", type: "curve_update", limit: 100)
# page.next_cursor => %{0 => ..., 1 => ...}

{:ok, page2} =
  Rheo.query_page("market-events", type: "curve_update", limit: 100, cursor: page.next_cursor)
```

Or stream:

```elixir
Rheo.stream_query("market-events", type: "curve_update", limit: 100)
|> Enum.take(250)
```

Lineage stays in metadata (see `Rheo.Event.Lineage`):

```elixir
meta = Rheo.Event.Lineage.put(%{},
  correlation_id: "trade-42",
  causation_id: "cmd-9",
  producer: "pricing-v3",
  schema: "curve_update",
  schema_version: "1"
)

Rheo.append("market-events", %{type: "curve_update", currency: "EUR", metadata: meta})
Rheo.query("market-events", correlation_id: "trade-42")
```

## Replay without copying events

```text
events (immutable)          deliveries (per group)
+------------------+        +------------------------+
| seq 1..N         | <----- | risk: available/leased |
| never deleted    |        | risk-replay-…: fresh   |
+------------------+        +------------------------+
```

**Preferred:** new group with a start cursor:

```elixir
Rheo.create_group("market-events", "risk-replay-2026-01", start_after: 1_000)
# or start_at: ~U[2026-01-15 00:00:00Z]
```

**Existing group:** reopen deliveries (at-least-once duplicates expected):

```elixir
Rheo.replay("market-events", "risk", from_sequence: 1_000)
Rheo.replay("market-events", "risk", from: ~U[2026-01-15 00:00:00Z])
Rheo.replay("market-events", "risk", query: [type: "curve_update", currency: "EUR"])
```

**Destructive reset** (loud on purpose):

```elixir
Rheo.reset_group("market-events", "risk", confirm: true)
# optional: start_after: 500
```

Reset clears that group’s deliveries only. Events remain. Other groups are
untouched. Without `confirm: true`, Rheo returns `{:error, :confirm_required}`.

## Failure cases

- Replay after a partial bugfix: duplicates for already-fixed handlers — make
  handlers idempotent on `event.id`
- Query-selected replay upserts deliveries for matching ids; unrelated groups
  never change
- Streaming raises if a mid-stream page fails (no silent truncation)

## What comes next

v0.5 shipped partitions and a contiguous ACK frontier — “ACKs are not a cursor.”
See [Article 12](https://hexdocs.pm/rheo/12-acks-are-not-a-cursor.html) and
[ADR 016](https://hexdocs.pm/rheo/016-partitions-and-ack-frontier.html).
