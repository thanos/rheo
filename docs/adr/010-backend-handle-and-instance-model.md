# ADR 010 — Backend handle and instance model

## Status

Accepted (v0.2.0)

## Context

v0.1.0 treated the backend connection as a global `GenServer.server()` “topology”
resolved via `Application.get_env(:rheo, :topology)`. That blocked multiple Rheo
instances in one BEAM and forced every backend to look like Mongo’s process name.

## Decision

1. Each supervised `{Rheo, name: Name, backend: {Mod, opts}}` starts a named
   instance tree: Registry, backend child, `Rheo.Instance`, Task.Supervisor, and
   `Rheo.GroupSupervisor`.
2. `Rheo.Backend` callbacks take an opaque `t:Rheo.Backend.handle/0` (`term()`).
   Mongo’s handle remains the Mongo process name/pid.
3. Public APIs accept optional `:rheo` (default `Rheo`) and resolve
   `{backend, handle}` via `Rheo.Instance.fetch!/1`.

## Consequences

- Host apps can run several independently configured Rheo instances.
- Future ETS/Ecto backends are not forced into a GenServer topology shape.
- Global `:topology` env is legacy; prefer instance opts.
