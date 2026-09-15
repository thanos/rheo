defmodule Rheo.IndependentGroupsTest do
  use Rheo.Case, async: false

  @moduletag :mongo

  setup do
    stream = unique_stream("groups")
    :ok = Rheo.create_stream(stream)
    :ok = Rheo.create_group(stream, "risk")
    :ok = Rheo.create_group(stream, "surveillance")
    %{stream: stream}
  end

  test "independent groups consume the same events separately", %{stream: stream} do
    {:ok, events} =
      Rheo.append_batch(stream, [
        %{type: "curve_update", currency: "EUR"},
        %{type: "curve_update", currency: "USD"}
      ])

    {:ok, risk_leases} = Rheo.fetch(stream, "risk", limit: 10, consumer_id: "risk-1")
    {:ok, surv_leases} = Rheo.fetch(stream, "surveillance", limit: 10, consumer_id: "surv-1")

    assert length(risk_leases) == 2
    assert length(surv_leases) == 2
    assert Enum.map(risk_leases, & &1.event_id) == Enum.map(events, & &1.id)
    assert Enum.map(surv_leases, & &1.event_id) == Enum.map(events, & &1.id)

    Enum.each(risk_leases, &Rheo.ack/1)

    assert {:ok, []} = Rheo.fetch(stream, "risk", limit: 10, consumer_id: "risk-2")
    assert {:ok, still} = Rheo.fetch(stream, "surveillance", limit: 10, consumer_id: "surv-2")
    # still leased by surv-1, so empty until ack or expiry — they are leased
    assert still == []

    Enum.each(surv_leases, &Rheo.ack/1)
    assert {:ok, []} = Rheo.fetch(stream, "surveillance", limit: 10, consumer_id: "surv-3")
  end

  test "ack by risk does not acknowledge surveillance", %{stream: stream} do
    {:ok, [event]} = Rheo.append_batch(stream, [%{type: "shared"}])
    {:ok, [risk]} = Rheo.fetch(stream, "risk", limit: 1, consumer_id: "r")
    {:ok, [surv]} = Rheo.fetch(stream, "surveillance", limit: 1, consumer_id: "s")

    assert risk.event_id == event.id
    assert surv.event_id == event.id
    assert :ok = Rheo.ack(risk)

    # surveillance lease still valid
    assert :ok = Rheo.ack(surv)
  end
end
