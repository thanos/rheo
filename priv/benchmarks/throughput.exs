# Benchmark notes (run after semantics are correct)
#
#   mix run priv/benchmarks/throughput.exs
#
# Report methodology with every number:
# hardware, Elixir/OTP, MongoDB version, event size, consumer count, indexes, dataset size.

Mix.ensure_application!(:logger)

url = System.get_env("RHEO_MONGO_URL", "mongodb://localhost:27017/rheo_bench")
{:ok, _} = Application.ensure_all_started(:mongodb_driver)
{:ok, _} = Rheo.start_link(url: url, name: Rheo.Mongo)

Enum.each(["streams", "events", "groups", "deliveries"], fn coll ->
  _ = Mongo.delete_many(Rheo.Mongo, coll, %{})
end)

:ok = Rheo.ensure_indexes()
:ok = Rheo.create_stream("bench")
:ok = Rheo.create_group("bench", "workers")

payload = %{type: "bench", data: String.duplicate("x", 256)}
n = String.to_integer(System.get_env("RHEO_BENCH_N", "1000"))

{append_us, {:ok, _}} =
  :timer.tc(fn ->
    Rheo.append_batch("bench", List.duplicate(payload, n))
  end)

IO.puts(
  "append_batch #{n} events: #{append_us / 1000} ms (#{Float.round(n / (append_us / 1_000_000), 1)}/s)"
)

{fetch_us, {:ok, leases}} =
  :timer.tc(fn ->
    Rheo.fetch("bench", "workers", limit: n, consumer_id: "bench")
  end)

IO.puts("fetch #{length(leases)}: #{fetch_us / 1000} ms")

{ack_us, _} =
  :timer.tc(fn ->
    Enum.each(leases, &Rheo.ack/1)
  end)

IO.puts("ack #{length(leases)}: #{ack_us / 1000} ms")

{query_us, {:ok, _}} =
  :timer.tc(fn ->
    Rheo.query("bench", type: "bench", limit: 100)
  end)

IO.puts("query: #{query_us / 1000} ms")

IO.puts("""
methodology:
  elixir: #{System.version()}
  otp: #{:erlang.system_info(:otp_release)}
  n: #{n}
  payload_bytes: ~256
  mongo_url: #{url}
""")
