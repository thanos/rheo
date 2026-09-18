# ADR 022: Consumer runtime and handler state

## Status

Accepted (v0.8.0). Amends [ADR 009](009-local-consumer-group-runtime.html).

## Context

v0.7 `handle_event/2` returned `{:ack, state}` while `concurrency > 1` ran
handlers as concurrent tasks; whichever task finished last replaced the group
state. The documentation told users to keep shared state in an Agent, which is
evidence the callback shape was wrong.

`use Rheo.Consumer` also started a bridge process that started or joined a
`Rheo.Group` under the instance's `DynamicSupervisor`. Two supervisors owned
one runtime: after a Group crash the DynamicSupervisor restarted it while the
bridge exited and was restarted by the host, and the new bridge could not
reclaim the group.

## Decision

### Handler state: read-only context

```elixir
@callback handle_event(Rheo.Event.t(), context :: map()) ::
            :ack | {:retry, term()} | {:reject, term()}
```

`setup/1` builds the context once when the Group starts and receives every
child-spec option. Handlers run as `Task`s under the instance's
`Task.Supervisor`, up to `:concurrency` at a time. There is no callback state;
mutable state belongs in application-owned processes referenced from the
context. Returning a non-map from `setup/1` stops the Group with
`{:invalid_context, value}`.

### One owner per local group

`use Rheo.Consumer` produces a child spec for `Rheo.Group` itself. The host
supervisor owns the Group; there is no bridge. A Group is named
by a local name derived from `{rheo, stream, group}` (not a Registry entry, so a Group does not
share the instance's process lifetime), and a second start for the same
`{rheo, stream, group}` returns `{:error, {:already_started, pid}}`.
`Rheo.GroupSupervisor` remains for dynamically started groups.

### Answers to the v0.8 review questions

1. One local runtime per `{rheo, stream, group}` per node.
2. `:concurrency` bounds handler tasks; `:max_demand` bounds leases.
3. A second child spec for the same identity fails to start.
4. The bridge is not needed and is removed.
5. There is no handler state under any concurrency.
6. The Group owns setup and teardown: `setup/1` runs in `init/1`; on shutdown
   the Group traps exits and drains inflight tasks for up to five seconds.
7. Workers are tasks. Long-lived workers would only be justified by per-worker
   state, which this ADR removes.

## Consequences

- Breaking for every consumer returning `{outcome, state}`.
- Supervision is one level flatter; restarts have a single owner.
- Draining is non-blocking: `Rheo.Group.drain/2` replies when inflight work
  settles or the timeout fires, while the Group keeps serving renewals.
