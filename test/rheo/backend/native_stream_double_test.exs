defmodule Rheo.Backend.NativeStreamDoubleTest do
  # Receipt semantics specific to native-stream settlement. The shared
  # contract suite runs in native_stream_contract_test.exs.
  use ExUnit.Case, async: true

  alias Rheo.Backend.NativeStreamDouble
  alias Rheo.Clock.Frozen

  setup do
    name = :"native_stream_#{System.unique_integer([:positive])}"
    rheo = :"rheo_native_#{System.unique_integer([:positive])}"

    start_supervised!({Rheo, name: rheo, backend: {NativeStreamDouble, name: name}})

    stream = "ns-#{System.unique_integer([:positive])}"
    :ok = Rheo.create_stream(stream, rheo: rheo)
    :ok = Rheo.create_group(stream, "g", rheo: rheo)
    %{rheo: rheo, stream: stream}
  end

  test "leases carry native receipts distinct from lease_id", %{rheo: rheo, stream: stream} do
    assert {:ok, _} = Rheo.append(stream, %{type: "t", key: "a"}, rheo: rheo)

    assert {:ok, [lease]} = Rheo.fetch(stream, "g", limit: 1, rheo: rheo)
    assert is_binary(lease.receipt)
    assert lease.receipt != lease.lease_id
    assert lease.event.sequence == 1
  end

  test "every settle op requires a matching receipt", %{rheo: rheo, stream: stream} do
    assert {:ok, _} = Rheo.append(stream, %{type: "t"}, rheo: rheo)
    assert {:ok, [lease]} = Rheo.fetch(stream, "g", limit: 1, rheo: rheo)
    bad = %{lease | receipt: "tampered"}

    assert {:error, :receipt_mismatch} = Rheo.ack(bad, rheo: rheo)
    assert {:error, :receipt_mismatch} = Rheo.nack(bad, :x, rheo: rheo)
    assert {:error, :receipt_mismatch} = Rheo.reject(bad, :x, rheo: rheo)
    assert {:error, :receipt_mismatch} = Rheo.renew(bad, rheo: rheo)
    assert :ok = Rheo.ack(lease, rheo: rheo)
  end

  test "a reclaimed delivery keeps its receipt but fences the old lease_id",
       %{rheo: rheo, stream: stream} do
    Frozen.set(DateTime.utc_now())
    on_exit(fn -> Frozen.reset() end)

    assert {:ok, _} = Rheo.append(stream, %{type: "t"}, rheo: rheo)
    assert {:ok, [a]} = Rheo.fetch(stream, "g", limit: 1, lease_ms: 50, rheo: rheo)
    Frozen.advance(100)
    assert {:ok, [b]} = Rheo.fetch(stream, "g", limit: 1, lease_ms: 1_000, rheo: rheo)

    assert b.receipt == a.receipt
    assert b.lease_id != a.lease_id
    assert {:error, :stale_lease} = Rheo.ack(a, rheo: rheo)
    assert :ok = Rheo.ack(b, rheo: rheo)
  end

  test "an unreachable handle maps to backend_unavailable" do
    assert {:error, :backend_unavailable} = NativeStreamDouble.ping(:no_such_double)
  end
end
