defmodule Rheo.Backend.ETSTest do
  use ExUnit.Case, async: false

  test "starts without Mongo and survives basic consume loop" do
    name = :"ets_smoke_#{System.unique_integer([:positive])}"
    assert {:ok, _} = start_supervised({Rheo, name: name, backend: Rheo.Backend.ETS}, id: name)

    stream = "ets-#{System.unique_integer([:positive])}"
    assert :ok = Rheo.create_stream(stream, rheo: name)
    assert :ok = Rheo.create_group(stream, "workers", rheo: name)
    assert {:ok, event} = Rheo.append(stream, %{type: "hello", n: 1}, rheo: name)

    assert {:ok, [lease]} = Rheo.fetch(stream, "workers", limit: 1, rheo: name)
    assert lease.event.id == event.id
    assert :ok = Rheo.ack(lease, rheo: name)
    assert {:ok, []} = Rheo.fetch(stream, "workers", limit: 1, rheo: name)
  end

  test "two instances do not share tables" do
    a = :"ets_a_#{System.unique_integer([:positive])}"
    b = :"ets_b_#{System.unique_integer([:positive])}"
    assert {:ok, _} = start_supervised({Rheo, name: a, backend: Rheo.Backend.ETS}, id: a)
    assert {:ok, _} = start_supervised({Rheo, name: b, backend: Rheo.Backend.ETS}, id: b)

    stream = "shared-name-#{System.unique_integer([:positive])}"
    assert :ok = Rheo.create_stream(stream, rheo: a)
    assert {:error, :stream_not_found} = Rheo.append(stream, %{type: "x"}, rheo: b)
    assert :ok = Rheo.create_stream(stream, rheo: b)
  end
end
