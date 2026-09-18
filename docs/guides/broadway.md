# Broadway

Rheo is a **durable event source**; Broadway is a **pipeline topology**.
`Rheo.Producer` and `Rheo.Broadway` wire them together.

## Pipeline

```elixir
defmodule MyApp.RiskBroadway do
  use Broadway

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module:
          {Rheo.Producer,
           rheo: MyRheo,
           stream: "market-events",
           group: "risk",
           max_demand: 50},
        transformer: {Rheo.Broadway, :transform, []},
        concurrency: 1
      ],
      processors: [default: [concurrency: 8]]
    )
  end

  @impl true
  def handle_message(_processor, message, _context) do
    case Risk.process(message.data) do
      :ok -> message
      {:error, reason} -> Broadway.Message.failed(message, reason)
    end
  end
end
```

Create the group first (`Rheo.create_group/3`). Successful messages ACK the
lease; failures NACK (retry) by default.

## Failure modes

```elixir
# Immediate dead-letter for this group:
Broadway.Message.configure_ack(message, on_failure: :reject)

# Or set producer default:
{Rheo.Producer, …, on_failure: :reject}
```

## Ownership split

| Component | Owns |
|---|---|
| `Rheo.Producer` | Fetch, lease renewal, `:max_demand` |
| Broadway processors / batchers | Concurrency and business work |
| `Rheo.Broadway.Acknowledger` | `Rheo.Producer.ack/3`, `nack/4`, `reject/4` per message |

Pick **one** consumption surface per `{rheo, stream, group}`:
`Rheo.Consumer` **or** `Rheo.Producer` — not both unless competing by design.

`:max_demand` bounds unsettled **leases**; Broadway `:concurrency` bounds
pipeline work.

Deep dive: [Rheo is not Broadway — it feeds Broadway](14-rheo-is-not-broadway-it-feeds-broadway.html),
[ADR 018](018-broadway-genstage-interop.html).
