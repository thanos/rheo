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
    case Risk.process(lease.event) do
      :ok -> :ok = Rheo.Producer.ack(state.producer, lease, rheo: MyRheo)
      {:error, reason} -> :ok = Rheo.Producer.nack(state.producer, lease, reason, rheo: MyRheo)
    end
  end)

  {:noreply, [], state}
end
```

`Rheo.Producer.ack/3`, `nack/4`, and `reject/4` settle the lease in the backend
and release the producer's inflight entry in one call, so renewal stops and a
`:max_demand` slot is freed. Settling with `Rheo.ack/2` directly requires a
separate `Rheo.Producer.confirm/2`. `Rheo.Broadway.Acknowledger` uses the same
helpers.

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
