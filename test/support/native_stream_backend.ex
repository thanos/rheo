defmodule Rheo.Backend.NativeStreamDouble do
  @moduledoc false
  # Test-only native-stream shaped backend (v0.8 Stage 5).
  # Keeps portable event.sequence while settling via Redis-like receipts.
  # Not a production Redis adapter.

  @behaviour Rheo.Backend
  use GenServer

  alias Rheo.{Clock, Event, Id, Lag, Lease, Partition}

  defstruct name: nil, streams: %{}, events: %{}, groups: %{}, deliveries: %{}, seq_wall: 0

  @impl true
  def capabilities do
    Rheo.Backend.Capabilities.new(%{
      durable: false,
      distributed: false,
      atomic_compare_and_set: true,
      notifications: false,
      change_feed: false,
      secondary_indexes: false,
      batch_writes: true,
      ordered_range_scan: true,
      replay: true,
      partitions: true,
      contiguous_frontier: true,
      native_consumer_groups: true,
      native_pending_list: true,
      native_reclaim: true,
      blocking_reads: false,
      native_group_lag: true
    })
    |> Rheo.Backend.Capabilities.to_legacy_map()
  end

  @impl true
  def child_spec(opts) do
    name = Keyword.get(opts, :name, __MODULE__)

    %{
      id: {__MODULE__, name},
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent
    }
  end

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    {:ok, %__MODULE__{name: Keyword.get(opts, :name, __MODULE__)}}
  end

  @impl true
  def ping(handle), do: call(handle, :ping)

  @impl true
  def ensure_indexes(handle), do: call(handle, :ensure_indexes)

  @impl true
  def create_stream(handle, stream, opts \\ []),
    do: call(handle, {:create_stream, stream, opts})

  @impl true
  def create_group(handle, stream, group, opts \\ []),
    do: call(handle, {:create_group, stream, group, opts})

  @impl true
  def append(handle, stream, payload, opts \\ []) do
    with {:ok, [event]} <- append_batch(handle, stream, [payload], opts), do: {:ok, event}
  end

  @impl true
  def append_batch(handle, stream, payloads, opts \\ []),
    do: call(handle, {:append_batch, stream, payloads, opts})

  @impl true
  def read(handle, stream, opts \\ []), do: call(handle, {:read, stream, opts})

  @impl true
  def query(handle, query), do: call(handle, {:query, query})

  @impl true
  def fetch(handle, stream, group, opts \\ []),
    do: call(handle, {:fetch, stream, group, opts})

  @impl true
  def renew(handle, lease, opts \\ []), do: call(handle, {:renew, lease, opts})

  @impl true
  def ack(handle, lease), do: call(handle, {:ack, lease})

  @impl true
  def retry(handle, lease, reason), do: call(handle, {:retry, lease, reason})

  @impl true
  def reject(handle, lease, reason), do: call(handle, {:reject, lease, reason})

  @impl true
  def replay(handle, stream, group, opts \\ []),
    do: call(handle, {:replay, stream, group, opts})

  @impl true
  def reset_group(handle, stream, group, opts \\ []),
    do: call(handle, {:reset_group, stream, group, opts})

  @impl true
  def lag(handle, stream, group, opts \\ []),
    do: call(handle, {:lag, stream, group, opts})

  defp call(handle, msg) do
    GenServer.call(handle, msg, 5_000)
  end

  @impl true
  def handle_call(:ping, _from, state), do: {:reply, :ok, state}

  def handle_call(:ensure_indexes, _from, state), do: {:reply, :ok, state}

  def handle_call({:create_stream, stream, opts}, _from, state) do
    if Map.has_key?(state.streams, stream) do
      {:reply, {:error, :already_exists}, state}
    else
      count = Keyword.get(opts, :partition_count, 1)

      stream_meta = %{
        partition_count: count,
        sequences: Map.new(0..(count - 1), &{&1, 0})
      }

      {:reply, :ok, %{state | streams: Map.put(state.streams, stream, stream_meta)}}
    end
  end

  def handle_call({:create_group, stream, group, opts}, _from, state) do
    key = {stream, group}

    if Map.has_key?(state.groups, key) do
      {:reply, {:error, :already_exists}, state}
    else
      start_after = Keyword.get(opts, :start_after, 0)
      meta = Map.fetch!(state.streams, stream)

      group_meta = %{
        cursors: Map.new(0..(meta.partition_count - 1), &{&1, start_after}),
        frontiers: Map.new(0..(meta.partition_count - 1), &{&1, start_after}),
        max_attempts: Keyword.get(opts, :max_attempts, 5)
      }

      {:reply, :ok, %{state | groups: Map.put(state.groups, key, group_meta)}}
    end
  end

  def handle_call({:append_batch, stream, payloads, opts}, _from, state) do
    meta = Map.fetch!(state.streams, stream)

    {events, meta, state, wall} =
      Enum.reduce(payloads, {[], meta, state, state.seq_wall}, fn payload, {acc, m, st, w} ->
        partition = partition_for(payload, opts, m.partition_count)
        seq = Map.fetch!(m.sequences, partition) + 1
        w = w + 1
        receipt = "#{System.system_time(:millisecond)}-#{w}"
        id = Id.generate()

        event = %Event{
          id: id,
          stream: stream,
          partition: partition,
          sequence: seq,
          timestamp: Clock.utc_now(),
          key: payload[:key] || payload["key"],
          type: payload[:type] || payload["type"],
          metadata: payload[:metadata] || payload["metadata"] || %{},
          payload: stringify_keys(Map.drop(payload, [:metadata, "metadata"]))
        }

        m = %{m | sequences: Map.put(m.sequences, partition, seq)}
        st = put_in(st.events[id], Map.put(Map.from_struct(event), :native_id, receipt))
        {[event | acc], m, st, w}
      end)

    state = %{
      state
      | streams: Map.put(state.streams, stream, meta),
        seq_wall: wall
    }

    {:reply, {:ok, Enum.reverse(events)}, state}
  end

  def handle_call({:read, stream, opts}, _from, state) do
    after_seq = Keyword.get(opts, :after, 0)
    limit = Keyword.get(opts, :limit, 100)

    events =
      state.events
      |> Map.values()
      |> Enum.filter(&(&1.stream == stream and &1.sequence > after_seq))
      |> Enum.sort_by(&{&1.partition, &1.sequence})
      |> Enum.take(limit)
      |> Enum.map(&struct(Event, Map.drop(&1, [:native_id])))

    {:reply, {:ok, events}, state}
  end

  def handle_call({:query, query}, _from, state) do
    events =
      state.events
      |> Map.values()
      |> Enum.filter(&(&1.stream == query.stream))
      |> Enum.sort_by(&{&1.partition, &1.sequence})
      |> Enum.take(query.limit || 100)
      |> Enum.map(&struct(Event, Map.drop(&1, [:native_id])))

    {:reply, {:ok, events}, state}
  end

  def handle_call({:fetch, stream, group, opts}, _from, state) do
    limit = Keyword.get(opts, :limit, 10)
    consumer_id = Keyword.get(opts, :consumer_id, "native-1")
    lease_ms = Keyword.get(opts, :lease_ms, 30_000)
    now = Clock.utc_now()
    group_key = {stream, group}
    group_meta = Map.fetch!(state.groups, group_key)

    state = materialize(state, stream, group)

    claimable =
      state.deliveries
      |> Map.values()
      |> Enum.filter(fn d ->
        d.stream == stream and d.group == group and claimable?(d, now)
      end)
      |> Enum.sort_by(&{&1.partition, &1.sequence})
      |> Enum.take(limit)

    {leases, state} =
      Enum.map_reduce(claimable, state, fn d, st ->
        lease_id = Id.generate()
        expires = DateTime.add(now, lease_ms, :millisecond)
        attempt = max(d.attempt, 0) + 1
        event_row = Map.fetch!(st.events, d.event_id)
        receipt = event_row.native_id

        d = %{
          d
          | status: :leased,
            lease_id: lease_id,
            attempt: attempt,
            expires_at: expires,
            consumer_id: consumer_id,
            receipt: receipt
        }

        st = put_in(st.deliveries[d.key], d)

        lease = %Lease{
          lease_id: lease_id,
          stream: stream,
          group: group,
          event_id: d.event_id,
          event: struct(Event, Map.drop(event_row, [:native_id])),
          consumer_id: consumer_id,
          attempt: attempt,
          leased_at: now,
          expires_at: expires,
          receipt: receipt
        }

        {lease, st}
      end)

    _ = group_meta
    {:reply, {:ok, leases}, state}
  end

  def handle_call({:renew, lease, opts}, _from, state) do
    key = delivery_key(lease)
    now = Clock.utc_now()
    lease_ms = Keyword.get(opts, :lease_ms, 30_000)

    case Map.get(state.deliveries, key) do
      %{status: :leased, lease_id: lid} = d when lid == lease.lease_id ->
        expires = DateTime.add(now, lease_ms, :millisecond)
        d = %{d | expires_at: expires}
        {:reply, {:ok, %{lease | expires_at: expires}}, put_in(state.deliveries[key], d)}

      _ ->
        {:reply, {:error, :stale_lease}, state}
    end
  end

  def handle_call({:ack, lease}, _from, state) do
    key = delivery_key(lease)

    case Map.get(state.deliveries, key) do
      %{status: :leased, lease_id: lid, receipt: receipt} = d
      when lid == lease.lease_id and receipt == lease.receipt ->
        d = %{d | status: :acked, lease_id: nil, expires_at: nil}
        state = put_in(state.deliveries[key], d)
        state = advance_frontier(state, lease)
        {:reply, :ok, state}

      %{status: :leased, lease_id: lid} when lid == lease.lease_id ->
        {:reply, {:error, :receipt_mismatch}, state}

      _ ->
        {:reply, {:error, :stale_lease}, state}
    end
  end

  def handle_call({:retry, lease, _reason}, _from, state) do
    key = delivery_key(lease)

    case Map.get(state.deliveries, key) do
      %{status: :leased, lease_id: lid} = d when lid == lease.lease_id ->
        d = %{d | status: :available, lease_id: nil, expires_at: nil}
        {:reply, :ok, put_in(state.deliveries[key], d)}

      _ ->
        {:reply, {:error, :stale_lease}, state}
    end
  end

  def handle_call({:reject, lease, _reason}, _from, state) do
    key = delivery_key(lease)

    case Map.get(state.deliveries, key) do
      %{status: :leased, lease_id: lid} = d when lid == lease.lease_id ->
        d = %{d | status: :rejected, lease_id: nil, expires_at: nil}
        state = put_in(state.deliveries[key], d) |> advance_frontier(lease)
        {:reply, :ok, state}

      _ ->
        {:reply, {:error, :stale_lease}, state}
    end
  end

  def handle_call({:replay, stream, group, opts}, _from, state) do
    from = Keyword.get(opts, :from_sequence, 0)
    group_key = {stream, group}
    g = Map.fetch!(state.groups, group_key)
    meta = Map.fetch!(state.streams, stream)

    g = %{
      g
      | cursors: Map.new(0..(meta.partition_count - 1), &{&1, from}),
        frontiers: Map.new(0..(meta.partition_count - 1), &{&1, from})
    }

    deliveries =
      state.deliveries
      |> Enum.reject(fn {_k, d} -> d.stream == stream and d.group == group end)
      |> Map.new()

    {:reply, :ok, %{state | groups: Map.put(state.groups, group_key, g), deliveries: deliveries}}
  end

  def handle_call({:reset_group, stream, group, opts}, _from, state) do
    handle_call({:replay, stream, group, opts}, nil, state)
  end

  def handle_call({:lag, stream, group, _opts}, _from, state) do
    g = Map.fetch!(state.groups, {stream, group})
    meta = Map.fetch!(state.streams, stream)

    high =
      Map.new(0..(meta.partition_count - 1), fn p ->
        {p, Map.get(meta.sequences, p, 0)}
      end)

    {:reply, {:ok, Lag.from_maps(stream, group, g.frontiers, high, Map.keys(high))}, state}
  end

  defp materialize(state, stream, group) do
    group_key = {stream, group}
    g = Map.fetch!(state.groups, group_key)
    meta = Map.fetch!(state.streams, stream)

    Enum.reduce(0..(meta.partition_count - 1), state, fn partition, st ->
      cursor = Map.fetch!(g.cursors, partition)
      high = Map.fetch!(meta.sequences, partition)

      new_events =
        st.events
        |> Map.values()
        |> Enum.filter(fn e ->
          e.stream == stream and e.partition == partition and e.sequence > cursor and
            e.sequence <= high
        end)
        |> Enum.sort_by(& &1.sequence)

      Enum.reduce(new_events, st, fn e, acc ->
        key = {stream, group, e.id}

        if Map.has_key?(acc.deliveries, key) do
          acc
        else
          d = %{
            key: key,
            stream: stream,
            group: group,
            event_id: e.id,
            partition: partition,
            sequence: e.sequence,
            status: :available,
            attempt: 0,
            lease_id: nil,
            expires_at: nil,
            consumer_id: nil,
            receipt: e.native_id
          }

          put_in(acc.deliveries[key], d)
        end
      end)
      |> then(fn acc ->
        g = Map.fetch!(acc.groups, group_key)
        g = %{g | cursors: Map.put(g.cursors, partition, high)}
        %{acc | groups: Map.put(acc.groups, group_key, g)}
      end)
    end)
  end

  defp advance_frontier(state, lease) do
    group_key = {lease.stream, lease.group}
    g = Map.fetch!(state.groups, group_key)
    partition = lease.event.partition
    frontier = Map.fetch!(g.frontiers, partition)

    next =
      state.deliveries
      |> Map.values()
      |> Enum.filter(fn d ->
        d.stream == lease.stream and d.group == lease.group and d.partition == partition and
          d.sequence > frontier and d.status in [:acked, :rejected]
      end)
      |> Enum.sort_by(& &1.sequence)
      |> Enum.reduce_while(frontier, fn d, f ->
        if d.sequence == f + 1, do: {:cont, d.sequence}, else: {:halt, f}
      end)

    g = %{g | frontiers: Map.put(g.frontiers, partition, next)}
    %{state | groups: Map.put(state.groups, group_key, g)}
  end

  defp claimable?(%{status: :available}, _now), do: true

  defp claimable?(%{status: :leased, expires_at: exp}, now) when not is_nil(exp),
    do: DateTime.compare(exp, now) != :gt

  defp claimable?(_, _), do: false

  defp delivery_key(%Lease{} = lease), do: {lease.stream, lease.group, lease.event_id}

  defp partition_for(payload, opts, count) do
    case Partition.resolve(payload, opts, count) do
      {:ok, p} -> p
      {:error, _} -> 0
    end
  end

  defp stringify_keys(map) when is_map(map) do
    Map.new(map, fn
      {k, v} when is_atom(k) -> {Atom.to_string(k), v}
      {k, v} -> {k, v}
    end)
  end
end
