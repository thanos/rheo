# Flow readiness spike (v0.8)

Rheo does **not** ship a Flow integration package in v0.8. This note records
how `Rheo.Producer` should compose with `Flow.from_stages/2` for v0.9+.

## Shape

```elixir
{:ok, producer} =
  Rheo.Producer.start_link(rheo: MyRheo, stream: "events", group: "flow")

Flow.from_stages([producer])
|> Flow.map(fn %Rheo.Lease{} = lease -> {lease, transform(lease.event)} end)
|> Flow.partition()
|> Flow.reduce(fn -> %{} end, fn {lease, row}, acc ->
  Map.update(acc, row.key, [lease], &[lease | &1])
end)
|> Flow.on_trigger(fn acc ->
  Enum.each(acc, fn {_key, leases} ->
    Enum.each(leases, fn lease ->
      :ok = Rheo.ack(lease, rheo: MyRheo)
      Rheo.Producer.confirm(producer, lease.lease_id)
    end)
  end)

  {[], %{}}
end)
```

## Settlement rules

| Stage | When to ACK |
|---|---|
| `map` 1:1 | After successful map side-effect / emit |
| `partition` | Still holds lease through the key; settle after downstream work |
| `reduce` | **After** reduce materialization (on_trigger / emit) |
| `window` | **After** window closes and aggregate is durable |

Crashing before settle ⇒ at-least-once redelivery (lease expiry). Never ACK in
`reduce` before the aggregate is safely stored.

## Exit criterion

No Flow-specific lease type is required. The same `%Rheo.Lease{}` +
`confirm/2` boundary used by Broadway works if reduce/window retain leases
until trigger.

Executable coverage: `test/rheo/flow_readiness_test.exs` (map, partition+reduce,
window trigger, crash-before-settle redelivery). Flow is a **test-only**
dependency (`{:flow, "~> 1.2", only: :test}`) — not a Hex package in v0.8.
