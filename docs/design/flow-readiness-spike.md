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
    Enum.each(leases, &(:ok = Rheo.Producer.ack(producer, &1, rheo: MyRheo)))
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

`Enum.take/2` only keeps N results; GenStage demand can still pull (and settle)
more events. Livebook demos use a fresh group and a finite append per example
(`notebooks/pipelines.livemd`).

## Case answers

| Case | Lease carrier | Renewal | Success event | Settles | Crash before success |
|---|---|---|---|---|---|
| one-to-one map | the lease itself | producer | map side effect done | `Producer.ack/3` in `map` | lease expires, redelivered |
| filter | the lease | producer | filtered out is success | `Producer.ack/3` on drop | same |
| partition by key | the lease travels with the key | producer | downstream stage's success | downstream | same |
| reduce | accumulator holds the leases | producer | `on_trigger` after aggregate stored | `Producer.ack/3` per lease in `on_trigger` | all leases in the accumulator expire and are redelivered |
| window | accumulator holds up to a window of leases | producer; `lease_ms` must exceed the window | window close plus durable sink write | same | same |
| sink failure | accumulator | producer | none | `Producer.nack/4` per lease | redelivery |
| pipeline restart | nothing survives | producer restarts with empty inflight | first trigger after restart | as above | at-least-once replay from the backend |

Memory and renewal work are bounded by the producer's `:max_demand`: a
window cannot hold more leases than the producer will hand out unsettled.

## Exit criterion

No Flow-specific lease type is required. The same `%Rheo.Lease{}` and
`Rheo.Producer.ack/3` / `nack/4` / `reject/4` boundary used by Broadway works
if reduce/window retain leases until trigger. No `rheo_flow` package is
justified in v0.8.

Executable coverage: `test/rheo/flow_readiness_test.exs` (map, partition+reduce,
window trigger, crash-before-settle redelivery). Flow is a **test-only**
dependency (`{:flow, "~> 1.2", only: :test}`) — not a Hex package in v0.8.
