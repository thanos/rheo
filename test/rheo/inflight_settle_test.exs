defmodule Rheo.InflightTest do
  use ExUnit.Case, async: true

  alias Rheo.{Event, Inflight, Lease}

  defp lease(id \\ "l1") do
    event = %Event{
      id: "e-#{id}",
      stream: "s",
      partition: 0,
      sequence: 1,
      timestamp: ~U[2026-01-01 00:00:00.000Z],
      payload: %{}
    }

    %Lease{
      lease_id: id,
      stream: "s",
      group: "g",
      event_id: event.id,
      event: event,
      consumer_id: "c",
      attempt: 1,
      leased_at: ~U[2026-01-01 00:00:00.000Z],
      expires_at: ~U[2026-01-01 00:00:30.000Z],
      receipt: id
    }
  end

  test "capacity and put/drop/delete" do
    inflight = Inflight.new()
    assert Inflight.capacity(inflight, 10) == 10
    inflight = Inflight.put(inflight, "l1", lease())
    assert Inflight.size(inflight) == 1
    assert Inflight.capacity(inflight, 10) == 9
    assert {:ok, %Lease{lease_id: "l1"}} = Inflight.fetch_lease(inflight, "l1")
    assert :error = Inflight.fetch_lease(inflight, "missing")
    inflight = Inflight.delete(inflight, "l1")
    assert Inflight.size(inflight) == 0
  end

  test "update_lease and leases" do
    inflight = Inflight.put(Inflight.new(), "k", lease("a"))
    updated = lease("b")
    inflight = Inflight.update_lease(inflight, "k", updated)
    assert {:ok, %Lease{lease_id: "b"}} = Inflight.fetch_lease(inflight, "k")
    assert [%Lease{lease_id: "b"}] = Inflight.leases(inflight)
    assert Inflight.update_lease(inflight, "missing", updated) == inflight
    inflight = Inflight.drop(inflight, ["k"])
    assert Inflight.leases(inflight) == []
  end
end

defmodule Rheo.SettleTest do
  use ExUnit.Case, async: true

  alias Rheo.Settle

  test "classify and lost?" do
    assert Settle.classify(:stale_lease) == :stale_lease
    assert Settle.classify(:receipt_mismatch) == :receipt_mismatch
    assert Settle.classify(:backend_unavailable) == :backend_unavailable
    assert Settle.classify({:error, :receipt_mismatch}) == :receipt_mismatch
    assert Settle.classify(:custom) == :custom
    assert Settle.classify("x") == :ambiguous
    assert Settle.lost?(:stale_lease)
    assert Settle.lost?(:receipt_mismatch)
    refute Settle.lost?(:backend_unavailable)
  end
end

defmodule Rheo.Backend.WakeupTest do
  use ExUnit.Case, async: true

  defmodule NoWait do
  end

  defmodule WithWait do
    def wait(_handle, _opts), do: :ok
  end

  test "wait falls back when callback missing" do
    assert :ok = Rheo.Backend.Wakeup.wait(NoWait, :h, [])
  end

  test "wait delegates when exported" do
    assert :ok = Rheo.Backend.Wakeup.wait(WithWait, :h, timeout: 1)
  end
end
