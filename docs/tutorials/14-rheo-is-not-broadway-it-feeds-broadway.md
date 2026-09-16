# Rheo Is Not Broadway — It Feeds Broadway

Every time Rheo gets described as "consumer groups in Elixir", someone asks the
same question: isn't that Broadway? It is a fair question, and the answer is the
point of v0.7.

Broadway is a **topology**. It gives you processors, batchers, concurrency,
rate limiting, partitioning, and graceful draining, and it expects something else
to be the source of data — SQS, Kafka, RabbitMQ, a custom GenStage producer.

Rheo is a **durable event source**. It gives you an immutable, queryable log in a
database you already run, plus leases with fencing tokens, retries,
dead-lettering, per-partition sequences, and a contiguous ACK frontier. It has
never had opinions about how you schedule work.

Those are complementary, so v0.7 stops asking you to choose.

## The seam

```elixir
defmodule MyApp.RiskBroadway do
  use Broadway

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module:
          {Rheo.Producer,
           rheo: MyRheo, stream: "market-events", group: "risk", max_demand: 50},
        transformer: {Rheo.Broadway, :transform, []},
        concurrency: 1
      ],
      processors: [default: [concurrency: 8]],
      batchers: [warehouse: [batch_size: 100, concurrency: 2]]
    )
  end

  @impl true
  def handle_message(_processor, message, _context) do
    case Risk.process(message.data) do
      :ok -> Broadway.Message.put_batcher(message, :warehouse)
      {:error, reason} -> Broadway.Message.failed(message, reason)
    end
  end

  @impl true
  def handle_batch(:warehouse, messages, _info, _context) do
    :ok = Warehouse.insert_all(Enum.map(messages, & &1.data))
    messages
  end
end
```

`Rheo.Producer` is a plain GenStage producer whose events are `%Rheo.Lease{}`
structs. `Rheo.Broadway.transform/2` turns each lease into a
`%Broadway.Message{}` where `data` is the `%Rheo.Event{}` and the lease rides
along in `metadata`. Returning the message ACKs the lease;
`Broadway.Message.failed/2` sends it back for retry.

That is the whole integration. Broadway owns the pipeline shape. Rheo owns what
"processed" means.

## The interesting problem: who renews the lease?

Rheo leases are fenced. A `lease_id` is the only thing that authorizes an ACK,
and a lease that is not renewed expires so another worker can pick the event up.
Something has to renew a lease while its message is inflight.

In Broadway, the process that fetches is not the process that acknowledges. The
producer fetches; an acknowledger, invoked from a processor or a batcher,
acknowledges. Lease ownership and lease settlement are split across processes by
construction.

Rheo resolves this in two moves:

1. **The producer owns fetch and renewal.** It tracks inflight leases and renews
   them on a timer at half the TTL, exactly as `Rheo.Group` does. A renewal that
   comes back `:stale_lease` drops the lease so the backend can redeliver it.
2. **The acknowledger reports back.** Broadway invokes the transformer *inside*
   the producer process, so `Rheo.Broadway.transform/2` can capture the producer's
   pid as the message's `ack_ref`. After settling a lease,
   `Rheo.Broadway.Acknowledger` calls `Rheo.Producer.confirm/2`, which stops
   renewal and frees a demand slot.

If you write your own GenStage consumer instead of using Broadway, you do that
second step yourself:

```elixir
def handle_events(leases, _from, state) do
  Enum.each(leases, fn lease ->
    :ok = Risk.process(lease.event)
    :ok = Rheo.ack(lease, rheo: MyRheo)
    Rheo.Producer.confirm(state.producer, lease.lease_id)
  end)

  {:noreply, [], state}
end
```

## Two knobs that look like one

| Option | Bounds |
|---|---|
| `Rheo.Producer` `:max_demand` | unsettled leases held at once |
| Broadway `processors: [default: [concurrency: n]]` | messages being worked on |

They are not the same number. `:max_demand` is a claim on the database; Broadway's
`:concurrency` is a claim on schedulers. Setting concurrency far above
`:max_demand` just starves processors.

## Failure mapping

| Broadway outcome | Rheo settle | Effect |
|---|---|---|
| message returned | `Rheo.ack/2` | frontier advances |
| `Message.failed/2` | `Rheo.nack/3` | retried until `max_attempts`, then dead-lettered |
| `Message.failed/2` with `on_failure: :reject` | `Rheo.reject/3` | dead-lettered immediately |

Set the default on the producer, or per message:

```elixir
Broadway.Message.configure_ack(message, on_failure: :reject)
```

## What did not change

`Rheo.Consumer` is untouched, and it is still the right answer for most
applications: a `handle_event/2` callback, a supervised `Rheo.Group` that owns
fetch, concurrency, renewal, and settle, and no extra dependency to reason about.
Reach for the producer when you actually want Broadway's topology — batching, rate
limiting, or fanning out into several batchers.

Pick one per `{rheo, stream, group}`. Running a `Rheo.Consumer` and a
`Rheo.Producer` against the same durable group is not an error — it is competing
consumers, and the backend arbitrates leases the way it always does — but it is
rarely what you meant.

Delivery is still at-least-once. `message.metadata.attempt` tells you whether
this is a first delivery or a redelivery; keep handlers idempotent on
`Rheo.Event.id`.

## Further reading

- [ADR 018](https://hexdocs.pm/rheo/018-broadway-genstage-interop.html)
- [ADR 007 — demand and backpressure](https://hexdocs.pm/rheo/007-demand-and-backpressure.html)
- [0.6 → 0.7 migration](https://hexdocs.pm/rheo/0-6-to-0-7.html)
- [Broadway custom producers](https://hexdocs.pm/broadway/custom-producers.html)
