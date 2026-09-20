defmodule Rheo.InflightTest do
  use ExUnit.Case, async: true

  alias Rheo.{Event, Inflight, Lease}

  doctest Inflight

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

  test "capacity, put, pop, delete" do
    inflight = Inflight.new()
    assert Inflight.capacity(inflight, 10) == 10
    inflight = Inflight.put(inflight, "l1", lease(), %{task: :t})
    assert Inflight.size(inflight) == 1
    assert Inflight.capacity(inflight, 10) == 9
    assert Inflight.capacity(inflight, 0) == 0
    assert {:ok, %Lease{lease_id: "l1"}} = Inflight.fetch_lease(inflight, "l1")
    assert :error = Inflight.fetch_lease(inflight, "missing")
    assert :error = Inflight.pop(inflight, "missing")

    assert {:ok, %{task: :t, lease: %Lease{lease_id: "l1"}}, popped} =
             Inflight.pop(inflight, "l1")

    assert Inflight.size(popped) == 0
    assert Inflight.delete(popped, "l1") == popped

    present = Inflight.put(Inflight.new(), "l1", lease(), %{task: :t})
    assert Inflight.size(Inflight.delete(present, "l1")) == 0
    assert Inflight.delete(Inflight.new(), "missing") == Inflight.new()
  end

  test "update_lease, leases, drop" do
    inflight = Inflight.put(Inflight.new(), "k", lease("a"))
    updated = lease("b")
    inflight = Inflight.update_lease(inflight, "k", updated)
    assert {:ok, %Lease{lease_id: "b"}} = Inflight.fetch_lease(inflight, "k")
    assert [%Lease{lease_id: "b"}] = Inflight.leases(inflight)
    assert Inflight.update_lease(inflight, "missing", updated) == inflight
    assert Inflight.leases(Inflight.drop(inflight, ["k"])) == []
  end

  test "renew_all keeps renewed leases, drops lost ones, retains other failures" do
    inflight =
      Inflight.new()
      |> Inflight.put("ok", lease("ok"))
      |> Inflight.put("stale", lease("stale"))
      |> Inflight.put("down", lease("down"))

    renewed_at = ~U[2026-01-01 00:01:00.000Z]

    {inflight, results} =
      Inflight.renew_all(inflight, fn
        %Lease{lease_id: "ok"} = l -> {:ok, %{l | expires_at: renewed_at}}
        %Lease{lease_id: "stale"} -> {:error, :stale_lease}
        %Lease{lease_id: "down"} -> {:error, %RuntimeError{message: "down"}}
      end)

    assert Enum.sort(Map.keys(inflight)) == ["down", "ok"]
    assert {:ok, %Lease{expires_at: ^renewed_at}} = Inflight.fetch_lease(inflight, "ok")

    assert Enum.sort_by(results, &elem(&1, 0)) == [
             {"down", lease("down"), {:failed, %RuntimeError{message: "down"}}},
             {"ok", %{lease("ok") | expires_at: renewed_at}, :ok},
             {"stale", lease("stale"), :stale_lease}
           ]
  end
end

defmodule Rheo.SettleTest do
  use ExUnit.Case, async: true

  alias Rheo.Settle

  doctest Settle

  test "classify maps errors into the portable vocabulary" do
    assert Settle.classify(:stale_lease) == :stale_lease
    assert Settle.classify({:error, :receipt_mismatch}) == :receipt_mismatch
    assert Settle.classify(:backend_unavailable) == :backend_unavailable
    assert Settle.classify({:ambiguous, :timeout}) == {:ambiguous, :timeout}
    assert Settle.classify({:failed, :constraint}) == {:failed, :constraint}
    assert Settle.classify({:invalid, :no_group}) == {:invalid, :no_group}
    assert Settle.classify(:group_not_found) == :group_not_found
    assert Settle.classify({:error, "boom"}) == {:failed, "boom"}
    assert Settle.classify({:event_missing, "e1"}) == {:failed, {:event_missing, "e1"}}
  end

  test "lost? is true only when another holder owns the lease" do
    assert Settle.lost?(:stale_lease)
    assert Settle.lost?({:error, :receipt_mismatch})
    refute Settle.lost?(:backend_unavailable)
    refute Settle.lost?({:failed, :x})
    refute Settle.lost?({:ambiguous, :x})
  end

  test "nack_after_failed_ack? only for definite, still-owned failures" do
    refute Settle.nack_after_failed_ack?(:stale_lease)
    refute Settle.nack_after_failed_ack?(:receipt_mismatch)
    refute Settle.nack_after_failed_ack?(:backend_unavailable)
    refute Settle.nack_after_failed_ack?({:ambiguous, :timeout})
    assert Settle.nack_after_failed_ack?({:failed, :constraint})
    assert Settle.nack_after_failed_ack?({:invalid, :bad})
    assert Settle.nack_after_failed_ack?(%RuntimeError{})
  end
end

defmodule Rheo.BackoffTest do
  use ExUnit.Case, async: true

  doctest Rheo.Backoff

  test "doubles from min to max" do
    assert Rheo.Backoff.next(0) == Rheo.Backoff.min_ms()

    schedule = Stream.iterate(0, &Rheo.Backoff.next/1) |> Enum.take(9)
    assert schedule == [0, 100, 200, 400, 800, 1_600, 3_200, 5_000, 5_000]
    assert Rheo.Backoff.next(Rheo.Backoff.max_ms()) == Rheo.Backoff.max_ms()
  end
end
