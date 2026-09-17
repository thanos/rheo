defmodule Rheo.Backend.NativeStreamDoubleTest do
  use ExUnit.Case, async: true

  alias Rheo.Backend.NativeStreamDouble

  setup do
    name = :"native_stream_#{System.unique_integer([:positive])}"
    rheo = :"rheo_native_#{System.unique_integer([:positive])}"

    start_supervised!({Rheo, name: rheo, backend: {NativeStreamDouble, name: name}})
    %{rheo: rheo}
  end

  test "fetch leases carry native receipts distinct from lease_id", %{rheo: rheo} do
    stream = "ns-#{System.unique_integer([:positive])}"
    assert :ok = Rheo.create_stream(stream, rheo: rheo)
    assert :ok = Rheo.create_group(stream, "g", rheo: rheo)
    assert {:ok, _} = Rheo.append(stream, %{type: "t", key: "a"}, rheo: rheo)

    assert {:ok, [lease]} = Rheo.fetch(stream, "g", limit: 1, rheo: rheo)
    assert is_binary(lease.receipt)
    assert lease.receipt != lease.lease_id
    assert lease.event.sequence == 1
  end

  test "ack requires matching receipt (fencing for native settle)", %{rheo: rheo} do
    stream = "ns-#{System.unique_integer([:positive])}"
    assert :ok = Rheo.create_stream(stream, rheo: rheo)
    assert :ok = Rheo.create_group(stream, "g", rheo: rheo)
    assert {:ok, _} = Rheo.append(stream, %{type: "t"}, rheo: rheo)

    assert {:ok, [lease]} = Rheo.fetch(stream, "g", limit: 1, rheo: rheo)
    bad = %{lease | receipt: "tampered"}
    assert {:error, :receipt_mismatch} = Rheo.ack(bad, rheo: rheo)
    assert :ok = Rheo.ack(lease, rheo: rheo)
  end

  test "capabilities declare native-stream mechanisms" do
    caps = NativeStreamDouble.capabilities()
    assert caps.native_consumer_groups
    assert caps.native_pending_list
    assert Rheo.Backend.Capabilities.guarantee?(caps, :lease_fencing)
  end
end
