# ADR 018 — GenStage / Broadway interoperability

## Status

Accepted (v0.7.0)

## Context

ADR 007 deliberately kept GenStage out of the MVP: bounded `fetch` plus idle
polling was enough to prove that leases, fencing, and a contiguous frontier work
on a database. That decision has held, but it left Rheo looking like a competitor
to Broadway rather than a source for it. Teams that already run Broadway
pipelines — batching to a warehouse, rate-limiting an upstream API, fanning out
to several batchers — had to choose between Broadway's topology and Rheo's
durable consumer groups.

They should not have to. Broadway is good at topology; Rheo is good at durable
event-source semantics. The question for 0.7 was where to put the seam, not
whether to integrate.

The hard part is acknowledgement. Rheo's settle calls are fenced on `lease_id`
(ADR 004), and something has to renew a lease while a message is inflight. In
Broadway, the process that fetches (the producer) is not the process that
acknowledges (an acknowledger invoked from a processor or batcher), so lease
ownership and lease settlement are split across processes by construction.

## Decision

1. **One producer, not two implementations.** `Rheo.Producer` is a `GenStage`
   producer whose events are `%Rheo.Lease{}` structs. Broadway starts it through
   `producer: [module: {Rheo.Producer, opts}]` like any
   [custom producer](https://hexdocs.pm/broadway/custom-producers.html). There is
   no separate `Rheo.Broadway.Producer` reimplementing fetch.
2. **Leases on the wire, messages at the seam.** The producer emits leases so
   plain GenStage consumers work without Broadway. `Rheo.Broadway.transform/2`
   is Broadway's `:transformer` and builds a `%Broadway.Message{}` with
   `data: lease.event` and `metadata: %{lease: lease, stream:, group:,
   partition:, attempt:}`. The full lease travels in metadata because it is the
   fencing token, and `attempt` is how a handler distinguishes a redelivery.
3. **The producer owns fetch and renewal; the acknowledger owns settle.**
   `handle_demand/2` accumulates demand and fetches at most
   `min(demand, max_demand - inflight)` leases, so `:max_demand` bounds unsettled
   leases exactly as it does for `Rheo.Group`. A timer renews inflight leases
   every `lease_ms / 2`; a renewal that comes back `:stale_lease` drops the lease
   so the backend can redeliver it.
4. **Settlement is reported back to the producer.** Because the acknowledger runs
   in another process, `Rheo.Broadway.transform/2` sets the message's `ack_ref` to
   `self()` — Broadway invokes the transformer *inside* the producer process, so
   that pid is the producer. `Rheo.Broadway.Acknowledger` settles the lease and
   then calls `Rheo.Producer.confirm/2`, which removes the lease from the inflight
   set, stops renewal, and frees a demand slot. Confirmation happens even when the
   settle call fails, since a lease that cannot be settled is already fenced.
5. **Failure mapping.** Successful → `Rheo.ack/2`. Failed → `Rheo.nack/3`
   (default) or `Rheo.reject/3` when `on_failure: :reject`, set on the producer or
   per message with `Broadway.Message.configure_ack/2`. Retry budget and
   dead-lettering stay in the group's `max_attempts` policy.
6. **Broadway does not go through `Rheo.Group`.** Routing Broadway messages
   through the Group's handler and `Task` concurrency would double-schedule the
   same work. `Rheo.Consumer` remains the OTP handler API; the producer is an
   alternative consumption surface for the same durable group. Running both
   against one `{rheo, stream, group}` is competing consumers, and the backend
   arbitrates as usual.
7. **Drain.** `Rheo.Producer` implements Broadway's `prepare_for_draining/1`
   callback to stop fetching while continuing to renew what is still inflight, and exposes
   `drain/2` for plain GenStage pipelines.
8. **`gen_stage` and `broadway` are hard dependencies.** Same reasoning as ADR
   017's stance on `ecto_sql`: `Rheo.Producer` needs `GenStage` and
   `Rheo.Broadway.Acknowledger` needs the `Broadway.Acknowledger` behaviour at
   compile time, and guarding whole modules behind `Code.ensure_loaded?/1` is
   fragile. Applications that never start a producer pay two small, widely used
   dependencies and nothing else.

## Consequences

- A Broadway pipeline can consume a durable Rheo group with correct ACK, NACK,
  redelivery, dead-lettering, partition scoping, and graceful drain.
- `:max_demand` on the producer and `:concurrency` on Broadway's processors are
  now separate knobs: the first bounds leases held, the second bounds work in
  flight. Setting `:concurrency` far above `:max_demand` starves processors.
- Batching is available for free. A Broadway batcher acknowledges a whole batch,
  which is one settle call per lease but one round of coordination.
- Rheo now ships two consumption surfaces to document and test. Article 14 and
  the module docs are explicit that you pick one per group.
- Broadway's own telemetry covers the pipeline; Rheo's covers the log. The
  acknowledger emits `[:rheo, :broadway, :ack | :retry | :reject]` plus the
  existing `[:rheo, :ack | :retry | :reject, :error]` events, and the producer
  emits `[:rheo, :producer, :start | :stop]` and reuses
  `[:rheo, :lease, :renew]` / `[:rheo, :fetch, :error]`.

## Alternatives

- **Wrap `Rheo.Group` and push into Broadway.** Rejected: the Group already runs
  handlers on its own `Task.Supervisor`, so Broadway's processors would schedule
  work a second time. Two schedulers over one lease set is a source of duplicate
  processing and unclear backpressure.
- **A second `Rheo.Broadway.Producer` with its own fetch loop.** Rejected: two
  implementations of the same claim path would drift, and Broadway already accepts
  any GenStage producer.
- **Emit `%Broadway.Message{}` directly from the producer.** Rejected: it would
  make `broadway` mandatory for GenStage-only users and duplicate the transformer
  mechanism Broadway already provides for custom producers.
- **Let the acknowledger renew leases.** Rejected: acknowledgers are stateless
  functions invoked per batch, with no timer and no view of the inflight set.
  Renewal belongs where lease ownership lives.
- **Pass the producer pid through the message metadata instead of `ack_ref`.**
  Rejected: `ack_ref` is exactly Broadway's mechanism for grouping messages by
  origin, so batches from one producer settle in one call.
- **Have the acknowledger only settle, and let renewal discover the outcome.**
  Rejected: the producer would keep renewing settled leases until a renewal
  failed, wasting a round trip per lease and holding demand slots for up to
  `lease_ms / 2`.
- **Optional `gen_stage` / `broadway` deps, or a separate `rheo_broadway`
  package.** Deferred, as in ADR 017. Revisit together with the per-backend
  package split.
- **Push wakeups (change streams, `NOTIFY`) as the demand trigger.** Out of
  scope: they stay hints. Demand still drives fetch, and idle polling remains the
  contract.
