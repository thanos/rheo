# ADR 013 — Portable query model

## Status

Accepted (v0.2.0)

## Context

v0.1.0 `Rheo.query/2` accepted a Mongo sort document (`:sort`), leaking backend
shape into the public API. Decoding Mongo documents on `Rheo.Event` coupled the
domain struct to persistence.

## Decision

1. Introduce `%Rheo.Query{}` (`stream`, `where`, `from`, `to`, `order_by`, `limit`).
2. `Rheo.query/1` and `Rheo.query/2` accept a Query or stream+keywords (converted).
3. Mongo translates Query → filter/sort internally.
4. Decode lives in `Rheo.Backend.Mongo.Codec`; `Rheo.Event` is domain-only.

## Consequences

- Call sites using `:sort` must migrate to `:order_by`.
- Call sites that decoded Mongo docs via Event helpers must use
  `Rheo.Backend.Mongo.Codec.event_from_doc/1` (or stop decoding docs in app code).
