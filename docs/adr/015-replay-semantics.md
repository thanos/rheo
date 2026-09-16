# ADR 015 — Replay semantics

## Status

Accepted (v0.4.0)

## Context

Search alone is not enough. Operators need to re-consume history for a group
after bugs, schema fixes, or backfills — without copying events into a second
log, and without silently rewriting production cursors from `Rheo.query/2`.

## Decision

1. **Events stay immutable.** Replay mutates **per-group delivery state** only.
2. **Preferred path:** `create_group/3` with `:start_after` (exclusive sequence)
   or `:start_at` (timestamp → first sequence). A new group name is the safest
   replay isolation.
3. **`Rheo.replay/3`** re-opens deliveries for an existing group:
   - `from_sequence:` — exclusive lower bound; deliveries at/after the next
     sequence become available again; group materialization cursor is set
   - `from:` — `DateTime`; resolved to a sequence via query, then same as above
   - `query:` — `%Rheo.Query{}`; matching events’ deliveries for that group are
     set back to `available` (at-least-once duplicates expected)
4. **`Rheo.reset_group/3`** destructively clears all deliveries for one group and
   resets its cursor (default to beginning). Requires `confirm: true`. Never
   deletes events. Other groups are untouched.
5. **Pagination:** additive `Rheo.query_page/2` returning `%Rheo.Page{}` with an
   opaque `next_cursor`; `Rheo.stream_query/2` pages via that API.
6. **Lineage** stays in `Event.metadata` with documented keys (see
   `Rheo.Event.Lineage`).

## Alternatives

- Copy events into a replay stream (rejected — duplicates the system of record)
- Auto-reset group on query (rejected — too dangerous)
- Promote lineage to top-level Event fields (deferred)

## Consequences

- Backends implement `replay/4` and `reset_group/4`
- Capabilities include `replay: true`
- Commit frontier / partition-aware replay remain v0.5+
