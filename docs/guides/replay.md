# Replay

Replay re-drives **per-group delivery state**. The event log is never copied or
deleted.

## Preferred: new group + start cursor

```elixir
:ok = Rheo.create_group("market-events", "risk-replay", start_after: 20)

{:ok, leases} = Rheo.fetch("market-events", "risk-replay", limit: 50)
# Only sequences after 20 — production "risk" is untouched
```

Use this when you want a parallel consumer without disturbing live progress.

## Reopen an existing group

```elixir
:ok = Rheo.replay("market-events", "risk", from_sequence: 0)
```

Or filter which events become available again:

```elixir
:ok =
  Rheo.replay("market-events", "risk",
    query: [type: "curve_update", currency: "EUR"]
  )
```

Expect duplicates relative to earlier ACKs — handlers must stay idempotent.

## Destructive reset

```elixir
# Requires explicit confirmation
:ok =
  Rheo.reset_group("market-events", "risk",
    confirm: true,
    start_after: 25
  )
```

Without `confirm: true`, reset is rejected. Events remain in the log; only this
group's delivery state is wiped / repositioned. Other groups are unaffected.

## Choosing a path

| Goal | API |
|---|---|
| Side-by-side reprocess | `create_group(..., start_after: …)` |
| Re-drive same group | `replay/3` |
| Wipe group progress | `reset_group(..., confirm: true)` |

Design details: [ADR 015](015-replay-semantics.html) and the tutorial
[Search and replay](11-search-and-replay-the-event-history.html).
