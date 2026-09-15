defmodule Rheo.LeaseTest do
  use Rheo.Case, async: false

  @moduletag :mongo

  setup do
    stream = unique_stream("lease")
    :ok = Rheo.create_stream(stream)
    :ok = Rheo.create_group(stream, "risk", max_attempts: 3)
    %{stream: stream}
  end

  test "fetch leases events and ack records durable progress", %{stream: stream} do
    {:ok, event} = Rheo.append(stream, %{type: "order", id: 1})
    {:ok, [lease]} = Rheo.fetch(stream, "risk", limit: 10, consumer_id: "c1")

    assert lease.event.id == event.id
    assert lease.attempt == 1
    assert lease.consumer_id == "c1"

    assert :ok = Rheo.ack(lease)
    assert {:ok, []} = Rheo.fetch(stream, "risk", limit: 10, consumer_id: "c1")
  end

  test "fetch is bounded by limit", %{stream: stream} do
    {:ok, _} = Rheo.append_batch(stream, for(i <- 1..5, do: %{type: "n", i: i}))
    {:ok, leases} = Rheo.fetch(stream, "risk", limit: 2, consumer_id: "c1")
    assert length(leases) == 2
  end

  test "stale ack is rejected after lease replaced", %{stream: stream} do
    {:ok, _} = Rheo.append(stream, %{type: "x"})
    {:ok, [lease1]} = Rheo.fetch(stream, "risk", limit: 1, consumer_id: "c1", lease_ms: 1_000)

    Rheo.Clock.Frozen.set(DateTime.add(Rheo.Clock.utc_now(), 2_000, :millisecond))

    {:ok, [lease2]} = Rheo.fetch(stream, "risk", limit: 1, consumer_id: "c2", lease_ms: 5_000)
    assert lease2.lease_id != lease1.lease_id
    assert lease2.attempt == 2

    assert {:error, :stale_lease} = Rheo.ack(lease1)
    assert :ok = Rheo.ack(lease2)
  end

  test "expired lease becomes redeliverable", %{stream: stream} do
    {:ok, event} = Rheo.append(stream, %{type: "re"})
    {:ok, [lease]} = Rheo.fetch(stream, "risk", limit: 1, consumer_id: "c1", lease_ms: 500)

    Rheo.Clock.Frozen.advance(1_000)

    {:ok, [again]} = Rheo.fetch(stream, "risk", limit: 1, consumer_id: "c2", lease_ms: 5_000)
    assert again.event.id == event.id
    assert again.lease_id != lease.lease_id
    assert again.attempt == 2
  end

  test "nack makes event available again", %{stream: stream} do
    {:ok, _} = Rheo.append(stream, %{type: "tmp"})
    {:ok, [lease]} = Rheo.fetch(stream, "risk", limit: 1, consumer_id: "c1")
    assert :ok = Rheo.nack(lease, :temporary)

    {:ok, [again]} = Rheo.fetch(stream, "risk", limit: 1, consumer_id: "c2")
    assert again.attempt == 2
  end

  test "max attempts dead-letters the delivery", %{stream: stream} do
    {:ok, _} = Rheo.append(stream, %{type: "bad"})

    for attempt <- 1..3 do
      {:ok, [lease]} = Rheo.fetch(stream, "risk", limit: 1, consumer_id: "c#{attempt}")
      assert lease.attempt == attempt
      assert :ok = Rheo.nack(lease, :fail)
    end

    assert {:ok, []} = Rheo.fetch(stream, "risk", limit: 1, consumer_id: "final")
  end

  test "reject dead-letters immediately", %{stream: stream} do
    {:ok, _} = Rheo.append(stream, %{type: "poison"})
    {:ok, [lease]} = Rheo.fetch(stream, "risk", limit: 1, consumer_id: "c1")
    assert :ok = Rheo.reject(lease, :poison)
    assert {:ok, []} = Rheo.fetch(stream, "risk", limit: 1, consumer_id: "c2")
  end

  test "duplicate ack of same lease fails after first", %{stream: stream} do
    {:ok, _} = Rheo.append(stream, %{type: "once"})
    {:ok, [lease]} = Rheo.fetch(stream, "risk", limit: 1, consumer_id: "c1")
    assert :ok = Rheo.ack(lease)
    assert {:error, :stale_lease} = Rheo.ack(lease)
  end
end
