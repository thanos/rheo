defmodule Rheo.Backend.NativeStreamDouble do
  @moduledoc false

  # Test-only backend shaped like a native stream (Redis Streams): events carry
  # a backend-native id that becomes the lease receipt, and settlement is
  # fenced on both `lease_id` and `receipt`. It keeps the portable
  # `event.sequence` per partition. It is a conformance fixture, not a Redis
  # adapter, and it runs the same `Rheo.BackendContract` suite as the
  # database backends.

  @behaviour Rheo.Backend
  use GenServer

  alias Rheo.{Clock, Event, Id, Lag, Lease, Partition, Query}

  defstruct name: nil, streams: %{}, events: %{}, groups: %{}, deliveries: %{}, native_seq: 0

  @impl true
  def capabilities do
    Rheo.Backend.Capabilities.new(%{
      durable: false,
      distributed: false,
      partitions: true,
      contiguous_frontier: true,
      replay: true,
      atomic_compare_and_set: true,
      batch_writes: true,
      ordered_range_scan: true,
      native_consumer_groups: true,
      native_pending_list: true,
      native_reclaim: true,
      native_group_lag: true
    })
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
  catch
    :exit, {:noproc, _} -> {:error, :backend_unavailable}
  end

  @impl true
  def handle_call(:ping, _from, state), do: {:reply, :ok, state}

  def handle_call(:ensure_indexes, _from, state), do: {:reply, :ok, state}

  def handle_call({:create_stream, stream, opts}, _from, state) do
    if Map.has_key?(state.streams, stream) do
      {:reply, {:error, :already_exists}, state}
    else
      count = Keyword.get(opts, :partition_count, 1)
      meta = %{partition_count: count, sequences: Map.new(0..(count - 1), &{&1, 0})}
      {:reply, :ok, %{state | streams: Map.put(state.streams, stream, meta)}}
    end
  end

  def handle_call({:create_group, stream, group, opts}, _from, state) do
    key = {stream, group}

    cond do
      not Map.has_key?(state.streams, stream) ->
        {:reply, {:error, :stream_not_found}, state}

      Map.has_key?(state.groups, key) ->
        {:reply, {:error, :already_exists}, state}

      true ->
        start_after = Keyword.get(opts, :start_after, 0)
        partitions = partitions_of(state, stream)

        group_meta = %{
          cursors: Map.new(partitions, &{&1, start_after}),
          frontiers: Map.new(partitions, &{&1, start_after}),
          max_attempts:
            Keyword.get(opts, :max_attempts, Application.get_env(:rheo, :default_max_attempts, 5))
        }

        {:reply, :ok, %{state | groups: Map.put(state.groups, key, group_meta)}}
    end
  end

  def handle_call({:append_batch, stream, payloads, opts}, _from, state) do
    case Map.fetch(state.streams, stream) do
      :error ->
        {:reply, {:error, :stream_not_found}, state}

      {:ok, meta} ->
        {events, meta, state} =
          Enum.reduce(payloads, {[], meta, state}, fn payload, {acc, m, st} ->
            partition = partition_for(payload, opts, m.partition_count)
            seq = Map.fetch!(m.sequences, partition) + 1
            native_id = "#{System.system_time(:millisecond)}-#{st.native_seq}"
            id = Id.generate()

            event = %Event{
              id: id,
              stream: stream,
              partition: partition,
              sequence: seq,
              timestamp: Keyword.get(opts, :timestamp, Clock.utc_now()),
              key: Keyword.get(opts, :key) || payload[:key] || payload["key"],
              type: payload[:type] || payload["type"],
              metadata: stringify_keys(payload[:metadata] || payload["metadata"] || %{}),
              payload: stringify_keys(Map.drop(payload, [:metadata, "metadata"]))
            }

            m = %{m | sequences: Map.put(m.sequences, partition, seq)}
            row = event |> Map.from_struct() |> Map.put(:native_id, native_id)
            st = %{st | events: Map.put(st.events, id, row), native_seq: st.native_seq + 1}
            {[event | acc], m, st}
          end)

        state = %{state | streams: Map.put(state.streams, stream, meta)}
        {:reply, {:ok, Enum.reverse(events)}, state}
    end
  end

  def handle_call({:read, stream, opts}, _from, state) do
    partition = Keyword.get(opts, :partition, 0)
    after_seq = Keyword.get(opts, :after, 0)
    limit = Keyword.get(opts, :limit, 100)

    events =
      state.events
      |> Map.values()
      |> Enum.filter(&(&1.stream == stream and &1.partition == partition))
      |> Enum.filter(&(&1.sequence > after_seq))
      |> Enum.sort_by(& &1.sequence)
      |> Enum.take(limit)
      |> Enum.map(&to_event/1)

    {:reply, {:ok, events}, state}
  end

  def handle_call({:query, %Query{} = query}, _from, state) do
    query = Query.apply_cursor(query)

    events =
      state.events
      |> Map.values()
      |> Enum.filter(&(&1.stream == query.stream and matches?(&1, query)))
      |> Enum.sort(order_fun(query.order_by))
      |> Enum.take(query.limit)
      |> Enum.map(&to_event/1)

    {:reply, {:ok, events}, state}
  end

  def handle_call({:fetch, stream, group, opts}, _from, state) do
    with {:ok, group_meta} <- fetch_group(state, group_key(stream, group)),
         {:ok, partitions} <- assignment(state, stream, opts) do
      limit = Keyword.get(opts, :limit, 10)
      consumer_id = Keyword.get(opts, :consumer_id, "native-1")
      lease_ms = Keyword.get(opts, :lease_ms, 30_000)
      now = Clock.utc_now()

      state = materialize(state, stream, group, group_meta, partitions)

      claimable =
        state.deliveries
        |> Map.values()
        |> Enum.filter(fn d ->
          d.stream == stream and d.group == group and d.partition in partitions and
            claimable?(d, now)
        end)
        |> Enum.sort_by(&{&1.partition, &1.sequence})
        |> Enum.take(limit)

      {leases, state} =
        Enum.map_reduce(claimable, state, fn d, st ->
          lease_id = Id.generate()
          expires = DateTime.add(now, lease_ms, :millisecond)
          attempt = d.attempt + 1
          row = Map.fetch!(st.events, d.event_id)

          d = %{
            d
            | status: :leased,
              lease_id: lease_id,
              attempt: attempt,
              expires_at: expires,
              consumer_id: consumer_id
          }

          lease = %Lease{
            lease_id: lease_id,
            stream: stream,
            group: group,
            event_id: d.event_id,
            event: to_event(row),
            consumer_id: consumer_id,
            attempt: attempt,
            leased_at: now,
            expires_at: expires,
            receipt: row.native_id
          }

          {lease, put_in(st.deliveries[d.key], d)}
        end)

      {:reply, {:ok, leases}, state}
    else
      {:error, _} = error -> {:reply, error, state}
    end
  end

  def handle_call({:renew, lease, opts}, _from, state) do
    lease_ms = Keyword.get(opts, :lease_ms, 30_000)

    settle(state, lease, fn d ->
      expires = DateTime.add(Clock.utc_now(), lease_ms, :millisecond)
      {{:ok, %{lease | expires_at: expires}}, %{d | expires_at: expires}}
    end)
  end

  def handle_call({:ack, lease}, _from, state) do
    settle(state, lease, fn d -> {:ok, %{d | status: :acked, lease_id: nil, expires_at: nil}} end)
  end

  def handle_call({:retry, lease, reason}, _from, state) do
    max_attempts = get_in(state.groups, [group_key(lease.stream, lease.group), :max_attempts])

    settle(state, lease, fn d ->
      if d.attempt >= max_attempts do
        {:ok,
         %{d | status: :rejected, reason: {:max_attempts, reason}, lease_id: nil, expires_at: nil}}
      else
        {:ok, %{d | status: :available, lease_id: nil, expires_at: nil}}
      end
    end)
  end

  def handle_call({:reject, lease, reason}, _from, state) do
    settle(state, lease, fn d ->
      {:ok, %{d | status: :rejected, reason: reason, lease_id: nil, expires_at: nil}}
    end)
  end

  def handle_call({:replay, stream, group, opts}, _from, state) do
    reopen(state, stream, group, Keyword.get(opts, :from_sequence, 0))
  end

  def handle_call({:reset_group, stream, group, opts}, _from, state) do
    reopen(state, stream, group, Keyword.get(opts, :start_after, 0))
  end

  def handle_call({:lag, stream, group, opts}, _from, state) do
    with {:ok, g} <- fetch_group(state, group_key(stream, group)),
         {:ok, partitions} <- assignment(state, stream, opts) do
      high = Map.new(partitions, &{&1, get_in(state.streams, [stream, :sequences, &1])})
      {:reply, {:ok, Lag.from_maps(stream, group, g.frontiers, high, partitions)}, state}
    else
      {:error, _} = error -> {:reply, error, state}
    end
  end

  # Fenced settle: `lease_id` and `receipt` must both match the current claim.
  defp settle(state, %Lease{} = lease, fun) do
    key = delivery_key(lease)

    case Map.get(state.deliveries, key) do
      %{status: :leased, lease_id: lid} = d when lid == lease.lease_id ->
        if d.receipt == lease.receipt do
          {reply, d} =
            case fun.(d) do
              {:ok, d} -> {:ok, d}
              {{:ok, _} = ok, d} -> {ok, d}
            end

          state = put_in(state.deliveries[key], d)

          state =
            if d.status in [:acked, :rejected], do: advance_frontier(state, lease), else: state

          {:reply, reply, state}
        else
          {:reply, {:error, :receipt_mismatch}, state}
        end

      _ ->
        {:reply, {:error, :stale_lease}, state}
    end
  end

  defp reopen(state, stream, group, from) do
    key = group_key(stream, group)

    case fetch_group(state, key) do
      {:ok, g} ->
        partitions = partitions_of(state, stream)

        g = %{
          g
          | cursors: Map.new(partitions, &{&1, from}),
            frontiers: Map.new(partitions, &{&1, from})
        }

        deliveries =
          state.deliveries
          |> Enum.reject(fn {_k, d} -> d.stream == stream and d.group == group end)
          |> Map.new()

        {:reply, :ok, %{state | groups: Map.put(state.groups, key, g), deliveries: deliveries}}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  defp materialize(state, stream, group, g, partitions) do
    Enum.reduce(partitions, state, fn partition, st ->
      cursor = Map.fetch!(g.cursors, partition)
      high = get_in(st.streams, [stream, :sequences, partition])

      new_rows =
        st.events
        |> Map.values()
        |> Enum.filter(fn e ->
          e.stream == stream and e.partition == partition and e.sequence > cursor and
            e.sequence <= high
        end)
        |> Enum.sort_by(& &1.sequence)

      st =
        Enum.reduce(new_rows, st, fn e, acc ->
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
              reason: nil,
              receipt: e.native_id
            }

            put_in(acc.deliveries[key], d)
          end
        end)

      update_in(st.groups[group_key(stream, group)].cursors, &Map.put(&1, partition, high))
    end)
  end

  defp advance_frontier(state, lease) do
    key = group_key(lease.stream, lease.group)
    g = Map.fetch!(state.groups, key)
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

    put_in(state.groups[key].frontiers, Map.put(g.frontiers, partition, next))
  end

  defp claimable?(%{status: :available}, _now), do: true

  defp claimable?(%{status: :leased, expires_at: exp}, now) when not is_nil(exp),
    do: DateTime.compare(exp, now) != :gt

  defp claimable?(_, _), do: false

  defp matches?(row, %Query{} = query) do
    Enum.all?(query.where, &where_match?(row, &1)) and
      (is_nil(query.from) or DateTime.compare(row.timestamp, query.from) != :lt) and
      (is_nil(query.to) or DateTime.compare(row.timestamp, query.to) != :gt) and
      (is_nil(query.after_sequence) or row.sequence > query.after_sequence) and
      (is_nil(query.until_sequence) or row.sequence <= query.until_sequence)
  end

  @metadata_fields [:correlation_id, :causation_id, :producer, :schema, :schema_version]

  defp where_match?(row, {:type, type}), do: row.type == type
  defp where_match?(row, {:key, key}), do: row.key == key
  defp where_match?(row, {:partition, partition}), do: row.partition == partition

  defp where_match?(row, {field, value}) when field in @metadata_fields,
    do: Map.get(row.metadata, Atom.to_string(field)) == value

  defp where_match?(row, {field, value}),
    do: Map.get(row.payload, Atom.to_string(field)) == value

  defp order_fun(order_by) do
    fn a, b ->
      Enum.reduce_while(order_by, true, fn {field, dir}, _ ->
        va = Map.get(a, field)
        vb = Map.get(b, field)

        cond do
          va == vb -> {:cont, true}
          dir == :desc -> {:halt, va > vb}
          true -> {:halt, va < vb}
        end
      end)
    end
  end

  defp to_event(row), do: struct(Event, Map.drop(row, [:native_id]))

  defp fetch_group(state, key) do
    case Map.fetch(state.groups, key) do
      {:ok, g} -> {:ok, g}
      :error -> {:error, :group_not_found}
    end
  end

  defp assignment(state, stream, opts) do
    count = get_in(state.streams, [stream, :partition_count])
    scope = Keyword.get(opts, :partitions, Keyword.get(opts, :partition, :all))
    Partition.normalize_assignment(scope, count)
  end

  defp partitions_of(state, stream),
    do: Enum.to_list(0..(get_in(state.streams, [stream, :partition_count]) - 1))

  defp group_key(stream, group), do: {stream, group}
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
