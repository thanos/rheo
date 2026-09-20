# Freezing Rheo Before 1.0

v0.8 corrected the abstractions. v0.9–v0.11 filled Redis Streams, an ops
inspect surface, and a Mnesia backend. v0.12 does not add a backend. It draws
the line hosts and adapter authors may depend on before SemVer starts at 1.0.

## What "API freeze candidate" means

A freeze candidate is not a promise that nothing will ever change. It is a
promise that:

1. **Consumer / Event / Query / Backend callback meanings** stay as documented
   on HexDocs for this release.
2. **Optional ops** (Mix inspect, LiveDashboard, Telemetry.Metrics) may still
   move without a Consumer break.
3. **Internal modules** (`Names`, `Instance`, table engine, driver clients) are
   not part of the published surface — do not couple to them.

After **1.0.0**, changing a frozen module's public contract requires a major
version.

## The published surface

HexDocs groups modules into Facade, Consume, Values, Backend, Runtime, and
Ops. That list *is* the freeze (ADR 029). If a module is missing from HexDocs,
treat it as internal even if it compiles in your project.

```text
Rheo  ──► Backend (ETS | Mnesia | Mongo | Ecto | Redis)
  ▲
  │
Consumer / Group   or   Producer / Broadway
```

## What we will not change after 1.0 without a major bump

- `Rheo.Consumer` handler outcomes (`:ack` / `{:retry, _}` / `{:reject, _}`)
  and read-only context
- `%Rheo.Event{}` / `%Rheo.Lease{}` / `%Rheo.Query{}` field meanings
- `Rheo.Backend` required callbacks and fencing / settle vocabulary
- At-least-once delivery with lease fencing (not exactly-once)
- One local Group owner per `{rheo, stream, group}` per node

## What stays deliberately flexible

- Shipping adapter internals (indexes, claim algorithms, codecs)
- Optional ops pages and Mix tasks
- Capability *mechanisms* (how a backend claims) as long as *guarantees* hold
- Multi-node Mnesia and a first-class Flow package — still deferred, not part
  of the freeze

## How to upgrade

From 0.11.1: no code changes required if you already use the public APIs.
See [0.11.1 → 0.12](https://hexdocs.pm/rheo/0-11-1-to-0-12.html).

## Related

- [ADR 029](https://hexdocs.pm/rheo/029-api-freeze-candidate.html)
- [Public API guide](https://hexdocs.pm/rheo/public-api.html)
- [Tutorial 15](https://github.com/thanos/rheo/blob/main/docs/tutorials/15-breaking-rheo-before-anyone-depends-on-the-wrong-abstraction.md) — why v0.8 broke early
