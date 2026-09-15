# Rheo demo
#
# Run: docker compose up -d && mix rheo.demo

Mix.ensure_application!(:logger)

url = System.get_env("RHEO_MONGO_URL", "mongodb://localhost:27017/rheo_demo")
Application.put_env(:rheo, :mongo_url, url)
Application.put_env(:rheo, :clock, Rheo.Clock.System)
Application.put_env(:rheo, :start_on_application, false)

{:ok, _} = Application.ensure_all_started(:mongodb_driver)

case Rheo.start_link(url: url, name: Rheo.Mongo) do
  {:ok, _} -> :ok
  {:error, {:already_started, _}} -> :ok
end

Enum.each(["streams", "events", "groups", "deliveries"], fn coll ->
  _ = Mongo.delete_many(Rheo.Mongo, coll, %{})
end)

:ok = Rheo.ensure_indexes()

stream = "market-events"
IO.puts("==> creating stream #{stream}")
:ok = Rheo.create_stream(stream)
:ok = Rheo.create_group(stream, "risk")
:ok = Rheo.create_group(stream, "surveillance")

currencies = ["EUR", "USD", "GBP"]
curves = ["EUR-EURIBOR-6M", "USD-SOFR", "GBP-SONIA"]

IO.puts("==> appending 200 market events")

events =
  for i <- 1..200 do
    %{
      type: "curve_update",
      currency: Enum.at(currencies, rem(i, 3)),
      curve: Enum.at(curves, rem(i, 3)),
      price: 1.0 + :rand.uniform() * 3.0,
      metadata: %{
        correlation_id: "corr-#{rem(i, 17)}",
        producer: "pricing-service-v3"
      }
    }
  end

{:ok, written} = Rheo.append_batch(stream, events)
IO.puts("    wrote #{length(written)} events")

IO.puts("==> risk and surveillance fetch independently")
{:ok, risk} = Rheo.fetch(stream, "risk", limit: 25, consumer_id: "risk-1", lease_ms: 2_000)

{:ok, surv} =
  Rheo.fetch(stream, "surveillance", limit: 25, consumer_id: "surv-1", lease_ms: 30_000)

IO.puts("    risk leased #{length(risk)}, surveillance leased #{length(surv)}")

IO.puts("==> acknowledging surveillance; abandoning risk leases (simulate crash)")
Enum.each(surv, &Rheo.ack/1)
# risk leases intentionally not ACKed

IO.puts("==> waiting for risk leases to expire…")
Process.sleep(2_500)

{:ok, redelivered} =
  Rheo.fetch(stream, "risk", limit: 25, consumer_id: "risk-2", lease_ms: 30_000)

IO.puts("    redelivered #{length(redelivered)} events to risk-2")
Enum.each(redelivered, &Rheo.ack/1)

target = Enum.at(written, 41)

IO.puts("==> querying historical event after consumption")

{:ok, found} =
  Rheo.query(stream,
    type: "curve_update",
    currency: target.payload["currency"],
    curve: target.payload["curve"],
    limit: 5
  )

IO.puts("    found #{length(found)} matching events (sample id=#{hd(found).id})")
IO.puts("==> demo complete")
