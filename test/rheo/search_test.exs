defmodule Rheo.SearchTest do
  use Rheo.Case, async: false

  @moduletag :mongo

  setup do
    stream = unique_stream("search")
    :ok = Rheo.create_stream(stream)
    :ok = Rheo.create_group(stream, "risk")

    events =
      [
        %{
          type: "curve_update",
          currency: "EUR",
          curve: "EUR-EURIBOR-6M",
          price: 2.913,
          metadata: %{correlation_id: "abc", producer: "pricing-service-v3"}
        },
        %{
          type: "curve_update",
          currency: "EUR",
          curve: "EUR-ESTR",
          price: 1.2,
          metadata: %{correlation_id: "def", producer: "pricing-service-v3"}
        },
        %{
          type: "trade",
          currency: "USD",
          trade: "12345",
          metadata: %{correlation_id: "abc", producer: "oms"}
        }
      ]

    {:ok, stored} = Rheo.append_batch(stream, events)
    %{stream: stream, stored: stored}
  end

  test "query by correlation id after consume", %{stream: stream} do
    {:ok, leases} = Rheo.fetch(stream, "risk", limit: 10, consumer_id: "c1")
    Enum.each(leases, &Rheo.ack/1)

    assert {:ok, found} = Rheo.query(stream, correlation_id: "abc")
    assert length(found) == 2
    assert Enum.all?(found, &(&1.metadata["correlation_id"] == "abc"))
  end

  test "query by time range and curve", %{stream: stream, stored: stored} do
    from = DateTime.add(hd(stored).timestamp, -1, :second)
    to = DateTime.add(List.last(stored).timestamp, 1, :second)

    assert {:ok, [event]} =
             Rheo.query(stream,
               type: "curve_update",
               currency: "EUR",
               curve: "EUR-EURIBOR-6M",
               from: from,
               to: to
             )

    assert event.payload["price"] == 2.913
  end

  test "query by producer", %{stream: stream} do
    assert {:ok, events} = Rheo.query(stream, producer: "pricing-service-v3")
    assert length(events) == 2
  end
end
