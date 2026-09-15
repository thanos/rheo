defmodule Rheo.EventLogTest do
  use Rheo.Case, async: false

  @moduletag :mongo

  alias Rheo.Event

  setup do
    stream = unique_stream("market")
    :ok = Rheo.create_stream(stream)
    %{stream: stream}
  end

  test "create_stream is idempotent-safe", %{stream: stream} do
    assert {:error, :already_exists} = Rheo.create_stream(stream)
  end

  test "append assigns monotonic sequences", %{stream: stream} do
    assert {:ok, %Event{sequence: 1, stream: ^stream} = e1} =
             Rheo.append(stream, %{type: "curve_update", currency: "EUR", price: 1.0})

    assert {:ok, %Event{sequence: 2}} =
             Rheo.append(stream, %{type: "curve_update", currency: "USD", price: 2.0})

    assert is_binary(e1.id)
    assert e1.type == "curve_update"
    assert e1.payload["currency"] == "EUR"
  end

  test "append_batch allocates contiguous sequences", %{stream: stream} do
    payloads = for i <- 1..5, do: %{type: "tick", n: i}

    assert {:ok, events} = Rheo.append_batch(stream, payloads)
    assert Enum.map(events, & &1.sequence) == [1, 2, 3, 4, 5]
  end

  test "read by sequence does not consume", %{stream: stream} do
    {:ok, _} = Rheo.append_batch(stream, [%{type: "a"}, %{type: "b"}, %{type: "c"}])

    assert {:ok, [a, b]} = Rheo.read(stream, after: 0, limit: 2)
    assert a.sequence == 1
    assert b.sequence == 2

    assert {:ok, [c]} = Rheo.read(stream, after: 2, limit: 10)
    assert c.sequence == 3
  end

  test "query finds historical events by payload fields", %{stream: stream} do
    {:ok, _} =
      Rheo.append(stream, %{
        type: "curve_update",
        currency: "EUR",
        curve: "EUR-EURIBOR-6M",
        price: 2.913
      })

    {:ok, _} =
      Rheo.append(stream, %{
        type: "curve_update",
        currency: "USD",
        curve: "USD-SOFR",
        price: 4.1
      })

    assert {:ok, [event]} =
             Rheo.query(stream, type: "curve_update", currency: "EUR", curve: "EUR-EURIBOR-6M")

    assert event.payload["price"] == 2.913
  end

  test "consumption never deletes events", %{stream: stream} do
    {:ok, event} = Rheo.append(stream, %{type: "keep_me"})
    :ok = Rheo.create_group(stream, "risk")
    {:ok, [lease]} = Rheo.fetch(stream, "risk", limit: 1)
    :ok = Rheo.ack(lease)

    assert {:ok, [^event]} = Rheo.query(stream, type: "keep_me")
    assert {:ok, [_]} = Rheo.read(stream, after: 0, limit: 10)
  end
end
