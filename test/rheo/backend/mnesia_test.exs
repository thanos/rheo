defmodule Rheo.Backend.MnesiaTest do
  use ExUnit.Case, async: false

  alias Rheo.Backend.Mnesia
  alias Rheo.{Event, Lease, Query}

  setup do
    handle = :"mnesia_unit_#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), "rheo_mnesia_unit_#{System.unique_integer([:positive])}")
    assert {:ok, _} = start_supervised({Mnesia, name: handle, dir: dir}, id: handle)
    %{handle: handle, dir: dir}
  end

  defp drop_stream(handle, stream) do
    :sys.replace_state(handle, fn state ->
      :ok = :mnesia.dirty_delete(state.streams, stream)
      state
    end)
  end

  defp mutate_stream(handle, stream, fun) do
    :sys.replace_state(handle, fn state ->
      [{table, ^stream, rec}] = :mnesia.dirty_read(state.streams, stream)
      :ok = :mnesia.dirty_write({table, stream, fun.(rec)})
      state
    end)
  end

  defp mutate_group(handle, stream, group, fun) do
    :sys.replace_state(handle, fn state ->
      key = {stream, group}
      [{table, ^key, rec}] = :mnesia.dirty_read(state.groups, key)
      :ok = :mnesia.dirty_write({table, key, fun.(rec)})
      state
    end)
  end

  test "starts without Mongo and survives basic consume loop" do
    name = :"mnesia_smoke_#{System.unique_integer([:positive])}"
    assert {:ok, _} = start_supervised({Rheo, name: name, backend: Rheo.Backend.Mnesia}, id: name)

    stream = "mnesia-#{System.unique_integer([:positive])}"
    assert :ok = Rheo.create_stream(stream, rheo: name)
    assert :ok = Rheo.create_group(stream, "workers", rheo: name)
    assert {:ok, event} = Rheo.append(stream, %{type: "hello", n: 1}, rheo: name)

    assert {:ok, [lease]} = Rheo.fetch(stream, "workers", limit: 1, rheo: name)
    assert lease.event.id == event.id
    assert :ok = Rheo.ack(lease, rheo: name)
    assert {:ok, []} = Rheo.fetch(stream, "workers", limit: 1, rheo: name)
  end

  test "two instances do not share tables" do
    a = :"mnesia_a_#{System.unique_integer([:positive])}"
    b = :"mnesia_b_#{System.unique_integer([:positive])}"
    assert {:ok, _} = start_supervised({Rheo, name: a, backend: Rheo.Backend.Mnesia}, id: a)
    assert {:ok, _} = start_supervised({Rheo, name: b, backend: Rheo.Backend.Mnesia}, id: b)

    stream = "shared-name-#{System.unique_integer([:positive])}"
    assert :ok = Rheo.create_stream(stream, rheo: a)
    assert {:error, :stream_not_found} = Rheo.append(stream, %{type: "x"}, rheo: b)
    assert :ok = Rheo.create_stream(stream, rheo: b)
  end

  describe "create_stream" do
    test "inserts when lookup empty and rejects duplicate", %{handle: handle} do
      assert :ok = Mnesia.create_stream(handle, "s1")
      assert {:error, :already_exists} = Mnesia.create_stream(handle, "s1")
    end
  end

  describe "create_group" do
    test "empty stream lookup returns stream_not_found", %{handle: handle} do
      assert {:error, :stream_not_found} = Mnesia.create_group(handle, "missing", "g")
    end

    test "duplicate group returns already_exists", %{handle: handle} do
      assert :ok = Mnesia.create_stream(handle, "s")
      assert :ok = Mnesia.create_group(handle, "s", "g")
      assert {:error, :already_exists} = Mnesia.create_group(handle, "s", "g")
    end

    test "start_after and start_at resolve cursor", %{handle: handle} do
      assert :ok = Mnesia.create_stream(handle, "s")
      assert {:ok, _} = Mnesia.append_batch(handle, "s", [%{type: "a"}, %{type: "b"}])

      assert :ok = Mnesia.create_group(handle, "s", "after", start_after: 1)
      assert {:ok, [lease]} = Mnesia.fetch(handle, "s", "after", limit: 10)
      assert lease.event.sequence == 2

      assert :ok =
               Mnesia.create_group(handle, "s", "at", start_at: ~U[1970-01-01 00:00:00.000Z])

      assert {:ok, [first | _]} = Mnesia.fetch(handle, "s", "at", limit: 10)
      assert first.event.sequence == 1

      # No events at/after start_at → cursor defaults to 1 (beginning).
      assert :ok =
               Mnesia.create_group(handle, "s", "future", start_at: ~U[2099-01-01 00:00:00.000Z])

      assert {:ok, [from_start | _]} = Mnesia.fetch(handle, "s", "future", limit: 10)
      assert from_start.event.sequence == 1
    end
  end

  describe "empty lookup error paths" do
    test "append_batch on missing stream", %{handle: handle} do
      assert {:error, :stream_not_found} = Mnesia.append_batch(handle, "nope", [%{type: "x"}])
    end

    test "empty append_batch returns ok list", %{handle: handle} do
      assert :ok = Mnesia.create_stream(handle, "s")
      assert {:ok, []} = Mnesia.append_batch(handle, "s", [])
    end

    test "fetch on missing group", %{handle: handle} do
      assert :ok = Mnesia.create_stream(handle, "s")
      assert {:error, :group_not_found} = Mnesia.fetch(handle, "s", "missing", limit: 1)
    end

    test "fetch / replay / reset / lag when stream row missing", %{handle: handle} do
      assert :ok = Mnesia.create_stream(handle, "s")
      assert :ok = Mnesia.create_group(handle, "s", "g")
      drop_stream(handle, "s")

      assert {:error, :stream_not_found} = Mnesia.fetch(handle, "s", "g", limit: 1)
      assert {:error, :stream_not_found} = Mnesia.replay(handle, "s", "g", from_sequence: 0)
      assert {:error, :stream_not_found} = Mnesia.reset_group(handle, "s", "g")
      assert {:error, :stream_not_found} = Mnesia.lag(handle, "s", "g")
    end

    test "retry on missing group", %{handle: handle} do
      lease = sample_lease("s", "gone", "e1")
      assert {:error, :group_not_found} = Mnesia.retry(handle, lease, :tmp)
    end
  end

  describe "partitions and lag" do
    test "default arity replay and lag", %{handle: handle} do
      assert :ok = Mnesia.create_stream(handle, "s", partition_count: 2)
      assert :ok = Mnesia.create_group(handle, "s", "g")
      assert {:ok, _} = Mnesia.append(handle, "s", %{type: "a"}, partition: 0)
      assert {:ok, [lease]} = Mnesia.fetch(handle, "s", "g", limit: 1, partition: 0)
      assert :ok = Mnesia.ack(handle, lease)

      assert {:error, :invalid_replay_opts} = Mnesia.replay(handle, "s", "g")
      assert :ok = Mnesia.replay(handle, "s", "g", from_sequence: 0)
      assert {:ok, %Rheo.Lag{lag: lag}} = Mnesia.lag(handle, "s", "g")
      assert is_integer(lag)
    end

    test "reject advances frontier past hole", %{handle: handle} do
      assert :ok = Mnesia.create_stream(handle, "s")
      assert :ok = Mnesia.create_group(handle, "s", "g")

      assert {:ok, _} =
               Mnesia.append_batch(handle, "s", [%{type: "a"}, %{type: "b"}, %{type: "c"}])

      assert {:ok, [l1, l2, l3]} = Mnesia.fetch(handle, "s", "g", limit: 3)
      assert :ok = Mnesia.ack(handle, l1)
      assert :ok = Mnesia.reject(handle, l2, :poison)
      assert :ok = Mnesia.ack(handle, l3)

      assert {:ok, lag} = Mnesia.lag(handle, "s", "g")
      assert lag.partitions[0].frontier == 3
    end

    test "ack after group deleted still settles lease", %{handle: handle} do
      assert :ok = Mnesia.create_stream(handle, "s")
      assert :ok = Mnesia.create_group(handle, "s", "g")
      assert {:ok, _} = Mnesia.append(handle, "s", %{type: "a"})
      assert {:ok, [lease]} = Mnesia.fetch(handle, "s", "g", limit: 1)

      :sys.replace_state(handle, fn state ->
        :ok = :mnesia.dirty_delete(state.groups, {"s", "g"})
        state
      end)

      assert :ok = Mnesia.ack(handle, lease)
    end

    test "start_after map with atom and string keys", %{handle: handle} do
      assert :ok = Mnesia.create_stream(handle, "s", partition_count: 2)
      assert {:ok, _} = Mnesia.append(handle, "s", %{type: "a"}, partition: 0)
      assert {:ok, _} = Mnesia.append(handle, "s", %{type: "b"}, partition: 0)
      assert {:ok, _} = Mnesia.append(handle, "s", %{type: "c"}, partition: 1)

      assert :ok =
               Mnesia.create_group(handle, "s", "g0", start_after: %{0 => 1}, partition: 0)

      assert {:ok, [lease0]} = Mnesia.fetch(handle, "s", "g0", limit: 10, partition: 0)
      assert lease0.event.sequence == 2

      assert :ok =
               Mnesia.create_group(handle, "s", "g1", start_after: %{"1" => 0}, partitions: [1])

      assert {:ok, [lease1]} = Mnesia.fetch(handle, "s", "g1", limit: 10, partition: 1)
      assert lease1.event.partition == 1

      # Map missing selected partition leaves default cursor (nil branch).
      assert :ok =
               Mnesia.create_group(handle, "s", "g2", start_after: %{9 => 0}, partition: 0)

      assert {:ok, [from_start | _]} = Mnesia.fetch(handle, "s", "g2", limit: 10, partition: 0)
      assert from_start.event.sequence == 1
    end

    test "start_at with partition subset leaves other partitions alone", %{handle: handle} do
      assert :ok = Mnesia.create_stream(handle, "s", partition_count: 2)
      assert {:ok, e0} = Mnesia.append(handle, "s", %{type: "a"}, partition: 0)
      assert {:ok, _} = Mnesia.append(handle, "s", %{type: "b"}, partition: 1)

      assert :ok =
               Mnesia.create_group(handle, "s", "g",
                 start_at: e0.timestamp,
                 partitions: [0]
               )

      assert {:ok, only0} = Mnesia.fetch(handle, "s", "g", limit: 10, partitions: [0])
      assert Enum.all?(only0, &(&1.event.partition == 0))
    end

    test "partition-scoped reset and replay outside assignment", %{handle: handle} do
      assert :ok = Mnesia.create_stream(handle, "s", partition_count: 2)
      assert :ok = Mnesia.create_group(handle, "s", "g")
      assert {:ok, e0} = Mnesia.append(handle, "s", %{type: "a"}, partition: 0)
      assert {:ok, e1} = Mnesia.append(handle, "s", %{type: "b"}, partition: 1)
      assert {:ok, leases} = Mnesia.fetch(handle, "s", "g", limit: 10)
      Enum.each(leases, &Mnesia.ack(handle, &1))

      assert :ok = Mnesia.reset_group(handle, "s", "g", partition: 0, start_after: 0)
      assert {:ok, again} = Mnesia.fetch(handle, "s", "g", limit: 10, partition: 0)
      assert Enum.all?(again, &(&1.event.partition == 0))

      # Replay events filtered to assigned partitions only.
      assert :ok = Mnesia.replay(handle, "s", "g", events: [e0, e1], partition: 1)
      assert {:ok, replayed} = Mnesia.fetch(handle, "s", "g", limit: 10, partition: 1)
      assert Enum.map(replayed, & &1.event_id) == [e1.id]
    end

    test "legacy next_sequence / next_sequences fallbacks", %{handle: handle} do
      assert :ok = Mnesia.create_stream(handle, "s")
      assert :ok = Mnesia.create_group(handle, "s", "g")
      assert {:ok, _} = Mnesia.append(handle, "s", %{type: "a"})

      mutate_stream(handle, "s", fn rec ->
        rec
        |> Map.delete(:next_sequences)
        |> Map.put(:next_sequence, 4)
      end)

      mutate_group(handle, "s", "g", fn rec ->
        rec
        |> Map.delete(:cursors)
        |> Map.delete(:frontiers)
        |> Map.put(:next_sequence, 2)
      end)

      assert {:ok, lag} = Mnesia.lag(handle, "s", "g")
      assert lag.partitions[0].high_watermark == 4
      assert lag.partitions[0].frontier == 0
    end

    test "nested payload and string metadata key", %{handle: handle} do
      assert :ok = Mnesia.create_stream(handle, "s")

      assert {:ok, event} =
               Mnesia.append(handle, "s", %{
                 "type" => "nested",
                 "metadata" => %{"schema" => "v1"},
                 "bag" => %{"n" => 1, "ts" => ~U[2026-01-01 00:00:00Z], "xs" => [1, %{a: 2}]}
               })

      assert event.metadata["schema"] == "v1"
      assert event.payload["bag"]["n"] == 1
      assert is_list(event.payload["bag"]["xs"])
    end

    test "reset ignores other streams' deliveries", %{handle: handle} do
      assert :ok = Mnesia.create_stream(handle, "s1")
      assert :ok = Mnesia.create_stream(handle, "s2")
      assert :ok = Mnesia.create_group(handle, "s1", "g")
      assert :ok = Mnesia.create_group(handle, "s2", "g")
      assert {:ok, _} = Mnesia.append(handle, "s1", %{type: "a"})
      assert {:ok, e2} = Mnesia.append(handle, "s2", %{type: "b"})
      assert {:ok, [_]} = Mnesia.fetch(handle, "s1", "g", limit: 1)
      assert {:ok, [_]} = Mnesia.fetch(handle, "s2", "g", limit: 1)

      assert :ok = Mnesia.reset_group(handle, "s1", "g")
      assert {:ok, [still]} = Mnesia.read(handle, "s2", after: 0)
      assert still.id == e2.id
    end

    test "lag group_not_found and legacy group cursors on fetch", %{handle: handle} do
      assert :ok = Mnesia.create_stream(handle, "s")
      assert :ok = Mnesia.create_group(handle, "s", "g")
      assert {:ok, _} = Mnesia.append(handle, "s", %{type: "a"})

      assert {:error, :group_not_found} = Mnesia.lag(handle, "s", "missing")

      mutate_group(handle, "s", "g", fn rec ->
        rec
        |> Map.delete(:cursors)
        |> Map.delete(:frontiers)
        |> Map.put(:next_sequence, 1)
      end)

      assert {:ok, [_lease]} = Mnesia.fetch(handle, "s", "g", limit: 1)
    end

    test "replay walks other streams' delivery rows", %{handle: handle} do
      assert :ok = Mnesia.create_stream(handle, "s1")
      assert :ok = Mnesia.create_stream(handle, "s2")
      assert :ok = Mnesia.create_group(handle, "s1", "g")
      assert :ok = Mnesia.create_group(handle, "s2", "g")
      assert {:ok, e1} = Mnesia.append(handle, "s1", %{type: "a"})
      assert {:ok, _} = Mnesia.append(handle, "s2", %{type: "b"})
      assert {:ok, [l1]} = Mnesia.fetch(handle, "s1", "g", limit: 1)
      assert {:ok, [_]} = Mnesia.fetch(handle, "s2", "g", limit: 1)
      assert :ok = Mnesia.ack(handle, l1)

      assert :ok = Mnesia.replay(handle, "s1", "g", from_sequence: 0)
      assert :ok = Mnesia.replay(handle, "s1", "g", event_ids: [e1.id])
    end

    test "starts with non-atom registered name", %{handle: _handle} do
      name = {:global, :"mnesia_tuple_#{System.unique_integer([:positive])}"}

      dir =
        Path.join(System.tmp_dir!(), "rheo_mnesia_tuple_#{System.unique_integer([:positive])}")

      assert {:ok, _} = start_supervised({Mnesia, name: name, dir: dir}, id: name)
      assert :ok = Mnesia.ping(name)
      assert :ok = Mnesia.create_stream(name, "s")
    end

    test "reject stale lease and atom metadata causation filter", %{handle: handle} do
      assert :ok = Mnesia.create_stream(handle, "s")
      assert :ok = Mnesia.create_group(handle, "s", "g")
      assert {:ok, event} = Mnesia.append(handle, "s", %{type: "a"})
      assert {:error, :stale_lease} = Mnesia.reject(handle, sample_lease("s", "g", event.id), :x)

      :sys.replace_state(handle, fn state ->
        key = {event.stream, event.partition, event.sequence}
        [{table, ^key, ^event}] = :mnesia.dirty_read(state.events, key)
        atom_meta = %{causation_id: "atom-cause", correlation_id: "c"}
        :ok = :mnesia.dirty_write({table, key, %{event | metadata: atom_meta}})
        state
      end)

      assert {:ok, [found]} =
               Mnesia.query(handle, %Query{
                 stream: "s",
                 where: [causation_id: "atom-cause"],
                 limit: 10
               })

      assert found.id == event.id
    end
  end

  describe "query filters" do
    setup %{handle: handle} do
      assert :ok = Mnesia.create_stream(handle, "s", partition_count: 2)

      assert {:ok, eur} =
               Mnesia.append(
                 handle,
                 "s",
                 %{
                   type: "curve_update",
                   key: "EUR-1",
                   currency: "EUR",
                   curve: "EURIBOR",
                   price: 2.5,
                   metadata: %{
                     correlation_id: "c1",
                     causation_id: "cause",
                     producer: "svc",
                     schema: "v1"
                   }
                 },
                 partition: 0,
                 timestamp: ~U[2026-01-02 12:00:00Z]
               )

      assert {:ok, usd} =
               Mnesia.append(
                 handle,
                 "s",
                 %{
                   type: "trade",
                   key: "USD-1",
                   currency: "USD",
                   metadata: %{"correlation_id" => "c2", "producer" => "oms"}
                 },
                 partition: 1,
                 timestamp: ~U[2026-01-03 12:00:00Z]
               )

      %{eur: eur, usd: usd}
    end

    test "filters by key, partition, metadata, and time window", %{handle: handle, eur: eur} do
      assert {:ok, [^eur]} =
               Mnesia.query(handle, %Query{
                 stream: "s",
                 where: [key: "EUR-1", partition: 0, currency: "EUR", curve: "EURIBOR"],
                 limit: 10
               })

      assert {:ok, [^eur]} =
               Mnesia.query(handle, %Query{
                 stream: "s",
                 where: [
                   correlation_id: "c1",
                   causation_id: "cause",
                   producer: "svc",
                   schema: "v1"
                 ],
                 limit: 10
               })

      assert {:ok, [^eur]} =
               Mnesia.query(handle, %Query{
                 stream: "s",
                 from: ~U[2026-01-02 00:00:00Z],
                 to: ~U[2026-01-02 23:59:59Z],
                 limit: 10
               })

      assert {:ok, from_only} =
               Mnesia.query(handle, %Query{
                 stream: "s",
                 from: ~U[2026-01-02 00:00:00Z],
                 limit: 10
               })

      assert length(from_only) == 2

      assert {:ok, [_]} =
               Mnesia.query(handle, %Query{
                 stream: "s",
                 to: ~U[2026-01-02 23:59:59Z],
                 limit: 10
               })
    end

    test "generic payload field and catch-all where", %{handle: handle, usd: usd, eur: eur} do
      assert {:ok, [^usd]} =
               Mnesia.query(handle, %Query{
                 stream: "s",
                 where: [type: "trade", currency: "USD"],
                 limit: 10
               })

      assert {:ok, [^eur]} =
               Mnesia.query(handle, %Query{
                 stream: "s",
                 where: [price: 2.5],
                 limit: 10
               })

      # Non-atom where field hits the catch-all matcher.
      assert {:ok, both} =
               Mnesia.query(handle, %Query{
                 stream: "s",
                 where: [{1, :ignored}],
                 limit: 10
               })

      assert length(both) == 2
    end
  end

  describe "replay" do
    setup %{handle: handle} do
      assert :ok = Mnesia.create_stream(handle, "s")
      assert :ok = Mnesia.create_group(handle, "s", "g")
      assert {:ok, [e1, e2]} = Mnesia.append_batch(handle, "s", [%{type: "a"}, %{type: "b"}])
      assert {:ok, [l1, l2]} = Mnesia.fetch(handle, "s", "g", limit: 2)
      assert :ok = Mnesia.ack(handle, l1)
      assert :ok = Mnesia.ack(handle, l2)
      %{e1: e1, e2: e2}
    end

    test "group_not_found when groups lookup empty", %{handle: handle} do
      assert {:error, :group_not_found} = Mnesia.replay(handle, "s", "missing", from_sequence: 0)
    end

    test "unknown opts return invalid_replay_opts", %{handle: handle} do
      assert {:error, :invalid_replay_opts} = Mnesia.replay(handle, "s", "g", foo: 1)
      assert {:error, :invalid_replay_opts} = Mnesia.replay(handle, "s", "g", [])
    end

    test "event_ids reopens deliveries", %{handle: handle, e1: e1} do
      assert :ok = Mnesia.replay(handle, "s", "g", event_ids: [e1.id])
      assert {:ok, [lease]} = Mnesia.fetch(handle, "s", "g", limit: 10)
      assert lease.event_id == e1.id
    end

    test "events recreates missing delivery rows", %{handle: handle, e2: e2} do
      assert :ok = Mnesia.reset_group(handle, "s", "g", start_after: 100)
      assert :ok = Mnesia.replay(handle, "s", "g", events: [e2])
      assert {:ok, [lease]} = Mnesia.fetch(handle, "s", "g", limit: 10)
      assert lease.event_id == e2.id
    end

    test "events reopens existing delivery rows", %{handle: handle, e1: e1} do
      assert :ok = Mnesia.replay(handle, "s", "g", events: [e1])
      assert {:ok, [lease]} = Mnesia.fetch(handle, "s", "g", limit: 10)
      assert lease.event_id == e1.id
    end

    test "empty event_ids is ok", %{handle: handle} do
      assert :ok = Mnesia.replay(handle, "s", "g", event_ids: [])
    end

    test "from_sequence reopens range", %{handle: handle, e1: e1} do
      assert :ok = Mnesia.replay(handle, "s", "g", from_sequence: 0)
      assert {:ok, leases} = Mnesia.fetch(handle, "s", "g", limit: 10)
      assert Enum.any?(leases, &(&1.event_id == e1.id))
    end
  end

  describe "reset_group" do
    test "group_not_found when lookup empty", %{handle: handle} do
      assert {:error, :group_not_found} = Mnesia.reset_group(handle, "s", "missing")
    end

    test "clears deliveries and keeps events", %{handle: handle} do
      assert :ok = Mnesia.create_stream(handle, "s")
      assert :ok = Mnesia.create_group(handle, "s", "g")
      assert {:ok, [event]} = Mnesia.append_batch(handle, "s", [%{type: "keep"}])
      assert {:ok, [lease]} = Mnesia.fetch(handle, "s", "g", limit: 1)
      assert :ok = Mnesia.ack(handle, lease)

      assert :ok = Mnesia.reset_group(handle, "s", "g", start_after: 0)
      assert {:ok, [still]} = Mnesia.read(handle, "s", after: 0)
      assert still.id == event.id
      assert {:ok, [again]} = Mnesia.fetch(handle, "s", "g", limit: 1)
      assert again.event_id == event.id
    end
  end

  describe "call availability" do
    test "noproc maps to backend_unavailable" do
      assert {:error, :backend_unavailable} = Mnesia.ping(:definitely_not_running_ets)
      assert {:error, :backend_unavailable} = Mnesia.create_stream(:no_proc, "s")
      assert {:error, :backend_unavailable} = Mnesia.create_group(:no_proc, "s", "g")
      assert {:error, :backend_unavailable} = Mnesia.replay(:no_proc, "s", "g", from_sequence: 0)
      assert {:error, :backend_unavailable} = Mnesia.reset_group(:no_proc, "s", "g")
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
      assert {:error, :backend_unavailable} = Mnesia.ping(handle)
      :ok = :sys.resume(handle)
      assert :ok = Mnesia.ping(handle)
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
