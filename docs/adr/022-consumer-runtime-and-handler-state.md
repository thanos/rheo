# ADR 022: Consumer runtime and handler state

## Status

Accepted (v0.8.0)

## Context

v0.7 `handle_event/2` returned `:ack` while `concurrency > 1` ran
handlers as concurrent tasks. State updates raced; docs said “use an Agent”
but the API looked like GenServer state.

The Consumer Bridge also joined an existing local `Rheo.Group` when a second
child targeted the same `{rheo, stream, group}`, which made ownership unclear.

## Decision

### Handler state — Option A

```elixir
@callback handle_event(Rheo.Event.t(), context :: map()) ::
            :ack | {:retry, term()} | {:reject, term()}
```

Context is read-only configuration (rheo, stream, group, consumer_id, and any
opts the app stores in `setup/1`). Mutable state belongs in app-owned OTP
processes.

`setup/1` still initializes context once per local group runtime.

### Local group ownership

Within one Rheo instance on one BEAM node, exactly one local group runtime
exists for `{stream, group}`. Scale with `:concurrency`. A second
`Rheo.Consumer` child for the same identity returns an error instead of
silently sharing.

## Consequences

- Breaking change for all consumers returning `{outcome, state}`.
- Simpler mental model; matches Broadway/Flow “no hidden callback state”.

## Related

ADR 009 (local group runtime) — ownership rule tightened here.
