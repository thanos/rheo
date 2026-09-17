# GenStage

`Rheo.Producer` is a plain `GenStage` producer. Emitted events are
`%Rheo.Lease{}` structs. Broadway is optional sugar on top.

## Start a producer

```elixir
{:ok, producer} =
  Rheo.Producer.start_link(
    rheo: MyRheo,
    stream: "market-events",
    group: "risk",
    max_demand: 20,
    poll_ms: 200
  )
```

Ensure the group exists (`Rheo.create_group/3`).

## Consumer that settles leases

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

Settling in the backend is only half the job: call `Rheo.Producer.confirm/2` so
the producer stops renewing the lease and frees a `:max_demand` slot.
`Rheo.Broadway.Acknowledger` does this automatically for Broadway.

## Options

| Option | Role |
|---|---|
| `:max_demand` | Cap on unsettled leases |
| `:lease_ms` | TTL; renewals every `lease_ms / 2` |
| `:poll_ms` | Idle poll when demand exceeds available work |
| `:partitions` | `:all` or partition id list |
| `:on_failure` | Default Broadway failure settle (`:nack` / `:reject`) |

## When to use GenStage vs Consumer

| Surface | Use when |
|---|---|
| `Rheo.Consumer` | Simple OTP handlers, Rheo owns concurrency |
| `Rheo.Producer` + GenStage | Custom demand topology |
| `Rheo.Producer` + Broadway | Processors, batchers, rate limits |

See [Broadway](broadway.html) and [ADR 018](018-broadway-genstage-interop.html).
