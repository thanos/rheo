# ADR 028 — Mnesia backend

## Status

Accepted (v0.11.0)

## Context

Hosts that want BEAM-native durability without Mongo, Postgres, or Redis need a
store that mirrors the ETS state machine but survives restarts. OTP `:mnesia`
provides `disc_copies` on a single node. Multi-node table copies are a later
milestone (shared backend arbitration); Rheo does not form a control-plane
cluster (ADR 006, roadmap deferred list).

## Decision

1. Ship `Rheo.Backend.Mnesia` in **core** (no Hex optional dep — `:mnesia` is OTP).
2. Reuse the ETS row shape: `streams` / `events` / `groups` / `deliveries`, GenServer
   owner handle, Model C DB-style receipts (`receipt ≈ lease_id`).
3. Capabilities: `durable: true`, `distributed: false` for v0.11.
4. Start opts: `:name`, `:dir` (Mnesia directory). First starter on a node creates
   the disc schema; instances isolate via unique table name prefixes.
5. Do **not** auto-start `:mnesia` via `extra_applications` — that creates a ram
   schema, and recycle-via-`stop` floods Logger / breaks Livebook (`:epipe`).
   List `:mnesia` under `included_applications` so Mix loads it; `Rheo.Backend.Mnesia`
   starts it only after `:dir` is set.
6. Implement the full `Rheo.Backend` contract including ADR 027 ops callbacks.

## Non-goals (v0.11)

- Multi-node `disc_copies` / netsplit policy
- Auto cluster formation or fragment tables as default
- Native PEL pretence
- Rheo membership / rebalance

## Consequences

- Docker-free durable demos and single-node production paths
- Competing Groups on several BEAM nodes still need Redis or Postgres (or a
  future multi-node Mnesia release)
- Conformance runs without external services (like ETS)
