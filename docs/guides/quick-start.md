# Quick Start

Rheo is an Elixir/OTP library: supervise a Rheo instance, append events to a
stream, and consume them with a durable consumer group. Delivery is
**at-least-once** — use event IDs for idempotency.

## Install

```elixir
def deps do
  [{:rheo, "~> 0.7.0"}]
end
```

Pick a backend when you start Rheo:

| Backend | When |
|---|---|
| `Rheo.Backend.ETS` | Tests, Livebook, ephemeral apps |
| `{Rheo.Backend.Mongo, url: ...}` | Durable MongoDB |
| `{Rheo.Backend.Ecto, repo: MyApp.Repo}` | Durable PostgreSQL or SQLite |

## Minimal ETS example

```elixir
children = [
  {Rheo, name: MyRheo, backend: Rheo.Backend.ETS}
]

Supervisor.start_link(children, strategy: :one_for_one)

:ok = Rheo.create_stream("orders", rheo: MyRheo)
:ok = Rheo.create_group("orders", "fulfillment", rheo: MyRheo)

{:ok, _event} =
  Rheo.append("orders", %{type: "order_created", key: "cust-1"}, rheo: MyRheo)

{:ok, [lease]} = Rheo.fetch("orders", "fulfillment", limit: 1, rheo: MyRheo)
:ok = Rheo.ack(lease, rheo: MyRheo)
```

## Idiomatic consumer

```elixir
defmodule MyApp.FulfillmentConsumer do
  use Rheo.Consumer, stream: "orders", group: "fulfillment"

  @impl true
  def handle_event(event, state) do
    :ok = MyApp.Fulfillment.process(event)
    {:ack, state}
  end
end

children = [
  {Rheo, name: MyRheo, backend: Rheo.Backend.ETS},
  {MyApp.FulfillmentConsumer, rheo: MyRheo, concurrency: 4, max_demand: 50}
]
```

## Next steps

- [Configuration](configuration.html) — leases, demand, clocks, auto-start
- [Consumer Groups](consumer-groups.html) — competing vs independent groups
- [ETS](ets.html) / [Mongo](mongo.html) / [Using Ecto](using-ecto.html)
- [Broadway](broadway.html) / [GenStage](genstage.html)
- [Livebook demo](rheo_demo.html)
