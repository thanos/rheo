defmodule Rheo.Backend.ETSTest do
  use ExUnit.Case, async: false

  alias Rheo.Backend.ETS
  alias Rheo.{Event, Lease}

  setup do
    handle = :"ets_unit_#{System.unique_integer([:positive])}"
    assert {:ok, _} = start_supervised({ETS, name: handle}, id: handle)
    %{handle: handle}
  end

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

  describe "create_stream" do
    test "inserts when lookup empty and rejects duplicate", %{handle: handle} do
      assert :ok = ETS.create_stream(handle, "s1")
      assert {:error, :already_exists} = ETS.create_stream(handle, "s1")
    end
  end

  describe "create_group" do
    test "empty stream lookup returns stream_not_found", %{handle: handle} do
      assert {:error, :stream_not_found} = ETS.create_group(handle, "missing", "g")
    end

    test "duplicate group returns already_exists", %{handle: handle} do
      assert :ok = ETS.create_stream(handle, "s")
      assert :ok = ETS.create_group(handle, "s", "g")
      assert {:error, :already_exists} = ETS.create_group(handle, "s", "g")
    end

    test "start_after and start_at resolve cursor", %{handle: handle} do
      assert :ok = ETS.create_stream(handle, "s")
      assert {:ok, _} = ETS.append_batch(handle, "s", [%{type: "a"}, %{type: "b"}])

      assert :ok = ETS.create_group(handle, "s", "after", start_after: 1)
      assert {:ok, [lease]} = ETS.fetch(handle, "s", "after", limit: 10)
      assert lease.event.sequence == 2

      assert :ok =
               ETS.create_group(handle, "s", "at", start_at: ~U[1970-01-01 00:00:00.000Z])

      assert {:ok, [first | _]} = ETS.fetch(handle, "s", "at", limit: 10)
      assert first.event.sequence == 1

      # No events at/after start_at → cursor defaults to 1 (beginning).
      assert :ok =
               ETS.create_group(handle, "s", "future", start_at: ~U[2099-01-01 00:00:00.000Z])

      assert {:ok, [from_start | _]} = ETS.fetch(handle, "s", "future", limit: 10)
      assert from_start.event.sequence == 1
    end
  end

  describe "empty lookup error paths" do
    test "append_batch on missing stream", %{handle: handle} do
      assert {:error, :stream_not_found} = ETS.append_batch(handle, "nope", [%{type: "x"}])
    end

    test "fetch on missing group", %{handle: handle} do
      assert :ok = ETS.create_stream(handle, "s")
      assert {:error, :group_not_found} = ETS.fetch(handle, "s", "missing", limit: 1)
    end

    test "retry on missing group", %{handle: handle} do
      lease = sample_lease("s", "gone", "e1")
      assert {:error, :group_not_found} = ETS.retry(handle, lease, :tmp)
    end
  end

  describe "replay" do
    setup %{handle: handle} do
      assert :ok = ETS.create_stream(handle, "s")
      assert :ok = ETS.create_group(handle, "s", "g")
      assert {:ok, [e1, e2]} = ETS.append_batch(handle, "s", [%{type: "a"}, %{type: "b"}])
      assert {:ok, [l1, l2]} = ETS.fetch(handle, "s", "g", limit: 2)
      assert :ok = ETS.ack(handle, l1)
      assert :ok = ETS.ack(handle, l2)
      %{e1: e1, e2: e2}
    end

    test "group_not_found when groups lookup empty", %{handle: handle} do
      assert {:error, :group_not_found} = ETS.replay(handle, "s", "missing", from_sequence: 0)
    end

    test "unknown opts return invalid_replay_opts", %{handle: handle} do
      assert {:error, :invalid_replay_opts} = ETS.replay(handle, "s", "g", foo: 1)
      assert {:error, :invalid_replay_opts} = ETS.replay(handle, "s", "g", [])
    end

    test "event_ids reopens deliveries", %{handle: handle, e1: e1} do
      assert :ok = ETS.replay(handle, "s", "g", event_ids: [e1.id])
      assert {:ok, [lease]} = ETS.fetch(handle, "s", "g", limit: 10)
      assert lease.event_id == e1.id
    end

    test "events recreates missing delivery rows", %{handle: handle, e2: e2} do
      assert :ok = ETS.reset_group(handle, "s", "g", start_after: 100)
      assert :ok = ETS.replay(handle, "s", "g", events: [e2])
      assert {:ok, [lease]} = ETS.fetch(handle, "s", "g", limit: 10)
      assert lease.event_id == e2.id
    end

    test "events reopens existing delivery rows", %{handle: handle, e1: e1} do
      assert :ok = ETS.replay(handle, "s", "g", events: [e1])
      assert {:ok, [lease]} = ETS.fetch(handle, "s", "g", limit: 10)
      assert lease.event_id == e1.id
    end

    test "empty event_ids is ok", %{handle: handle} do
      assert :ok = ETS.replay(handle, "s", "g", event_ids: [])
    end

    test "from_sequence reopens range", %{handle: handle, e1: e1} do
      assert :ok = ETS.replay(handle, "s", "g", from_sequence: 0)
      assert {:ok, leases} = ETS.fetch(handle, "s", "g", limit: 10)
      assert Enum.any?(leases, &(&1.event_id == e1.id))
    end
  end

  describe "reset_group" do
    test "group_not_found when lookup empty", %{handle: handle} do
      assert {:error, :group_not_found} = ETS.reset_group(handle, "s", "missing")
    end

    test "clears deliveries and keeps events", %{handle: handle} do
      assert :ok = ETS.create_stream(handle, "s")
      assert :ok = ETS.create_group(handle, "s", "g")
      assert {:ok, [event]} = ETS.append_batch(handle, "s", [%{type: "keep"}])
      assert {:ok, [lease]} = ETS.fetch(handle, "s", "g", limit: 1)
      assert :ok = ETS.ack(handle, lease)

      assert :ok = ETS.reset_group(handle, "s", "g", start_after: 0)
      assert {:ok, [still]} = ETS.read(handle, "s", after: 0)
      assert still.id == event.id
      assert {:ok, [again]} = ETS.fetch(handle, "s", "g", limit: 1)
      assert again.event_id == event.id
    end
  end

  describe "call availability" do
    test "noproc maps to backend_unavailable" do
      assert {:error, :backend_unavailable} = ETS.ping(:definitely_not_running_ets)
      assert {:error, :backend_unavailable} = ETS.create_stream(:no_proc, "s")
      assert {:error, :backend_unavailable} = ETS.create_group(:no_proc, "s", "g")
      assert {:error, :backend_unavailable} = ETS.replay(:no_proc, "s", "g", from_sequence: 0)
      assert {:error, :backend_unavailable} = ETS.reset_group(:no_proc, "s", "g")
    end

    test "timeout maps to backend_unavailable", %{handle: handle} do
      previous = Application.get_env(:rheo, :ets_call_timeout)
      Application.put_env(:rheo, :ets_call_timeout, 50)

      on_exit(fn ->
        if previous do
          Application.put_env(:rheo, :ets_call_timeout, previous)
        else
          Application.delete_env(:rheo, :ets_call_timeout)
        end
      end)

      :ok = :sys.suspend(handle)
      assert {:error, :backend_unavailable} = ETS.ping(handle)
      :ok = :sys.resume(handle)
      assert :ok = ETS.ping(handle)
    end
  end

  defp sample_lease(stream, group, event_id) do
    event = %Event{
      id: event_id,
      stream: stream,
      partition: 0,
      sequence: 1,
      timestamp: ~U[2026-01-01 00:00:00Z],
      payload: %{}
    }

    %Lease{
      lease_id: "L1",
      stream: stream,
      group: group,
      event_id: event_id,
      event: event,
      consumer_id: "c1",
      attempt: 1,
      leased_at: ~U[2026-01-01 00:00:00Z],
      expires_at: ~U[2026-01-01 00:00:30Z]
    }
  end
end
