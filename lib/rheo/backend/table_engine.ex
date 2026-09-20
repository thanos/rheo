defmodule Rheo.Backend.TableEngine do
  @moduledoc false

  # Shared query / claim / frontier helpers for ETS-shaped backends.
  # `state` is a map or struct with `:store` and table fields.

  require Logger

  alias Rheo.{Clock, Event, Query, Telemetry}

  @metadata_fields [:correlation_id, :causation_id, :producer, :schema, :schema_version]

  def lookup_stream_group(state, stream, group) do
    streams = store(state).lookup(state.streams, stream)
    groups = store(state).lookup(state.groups, {stream, group})

    case {streams, groups} do
      {[], _} -> {:error, :stream_not_found}
      {_, []} -> {:error, :group_not_found}
      {[{^stream, stream_rec}], [{{^stream, ^group}, group_rec}]} -> {:ok, stream_rec, group_rec}
    end
  end

  def read_partition(state, stream, partition, after_seq, limit) do
    scan_events(state, stream, partition, after_seq, limit)
  end

  def query_events(state, %Query{} = query) do
    query = Query.apply_cursor(query)
    head = {{query.stream, :"$1", :"$2"}, :"$3"}
    spec = [{head, [], [:"$3"]}]

    state.events
    |> then(&store(state).select(&1, spec))
    |> Enum.filter(&match_query?(&1, query))
    |> sort_events(query.order_by)
    |> Enum.take(query.limit)
  end

  def events_from_sequence(state, stream, partition, next_sequence, limit) do
    scan_events(state, stream, partition, next_sequence - 1, limit)
  end

  # `events` is an `ordered_set` keyed `{stream, partition, sequence}`, so
  # walking from a probe key yields the range in sequence order and costs
  # O(limit) rather than O(events in the stream). A guarded `select/2` would
  # traverse and materialize every later event before `Enum.take/2` dropped it.
  defp scan_events(_state, _stream, _partition, _after_seq, limit) when limit <= 0, do: []

  defp scan_events(state, stream, partition, after_seq, limit) do
    state
    |> walk_events({stream, partition, after_seq}, stream, partition, limit, [])
    |> Enum.reverse()
  end

  defp walk_events(_state, _key, _stream, _partition, 0, acc), do: acc

  defp walk_events(state, key, stream, partition, remaining, acc) do
    case store(state).next_key(state.events, key) do
      {^stream, ^partition, _seq} = next ->
        case store(state).lookup(state.events, next) do
          [{^next, %Event{} = event}] ->
            walk_events(state, next, stream, partition, remaining - 1, [event | acc])

          _ ->
            walk_events(state, next, stream, partition, remaining, acc)
        end

      _past_partition_or_end ->
        acc
    end
  end

  def insert_event(state, %Event{} = event) do
    key = {event.stream, event.partition, event.sequence}
    :ok = store(state).insert(state.events, key, event)
    :ok = store(state).insert(state.event_ids, event.id, key)
    :ok
  end

  def event_by_id(state, event_id) do
    case store(state).lookup(state.event_ids, event_id) do
      [{^event_id, key}] ->
        case store(state).lookup(state.events, key) do
          [{^key, %Event{} = event}] -> {:ok, event}
          _ -> :error
        end

      _ ->
        :error
    end
  end

  def put_delivery(state, delivery) do
    key = {delivery.stream, delivery.group, delivery.event_id}
    seq_key = {delivery.stream, delivery.group, delivery.partition, delivery.sequence}
    :ok = store(state).insert(state.deliveries, key, delivery)
    :ok = store(state).insert(state.delivery_by_seq, seq_key, delivery.event_id)

    if delivery.status in [:available, :leased] do
      :ok = store(state).insert(state.open_deliveries, seq_key, delivery.event_id)
    else
      :ok = store(state).delete(state.open_deliveries, seq_key)
    end

    :ok
  end

  def delete_delivery(state, delivery) do
    key = {delivery.stream, delivery.group, delivery.event_id}
    seq_key = {delivery.stream, delivery.group, delivery.partition, delivery.sequence}
    :ok = store(state).delete(state.deliveries, key)
    :ok = store(state).delete(state.delivery_by_seq, seq_key)
    :ok = store(state).delete(state.open_deliveries, seq_key)
    :ok
  end

  @doc """
  Claims up to `limit` deliveries for a group in one ordered pass.

  `open_deliveries` is an `ordered_set` keyed `{stream, group, partition,
  sequence}`, so a single walk yields candidates already in partition/sequence
  order. The walk carries its position across claims: restarting it per lease
  re-scanned the growing already-leased prefix, making one `fetch` quadratic in
  `:limit`.
  """
  def claim_batch(state, stream, group, partitions, consumer_id, lease_ms, limit, now) do
    ctx = %{
      state: state,
      stream: stream,
      group: group,
      partitions: MapSet.new(partitions),
      consumer_id: consumer_id,
      lease_ms: lease_ms,
      now: now
    }

    ctx
    |> collect_claims({stream, group, -1, -1}, limit, [])
    |> Enum.reverse()
  end

  defp collect_claims(_ctx, _key, remaining, acc) when remaining <= 0, do: acc

  defp collect_claims(ctx, key, remaining, acc) do
    case next_claim(ctx, key) do
      :done -> acc
      {:skip, next} -> collect_claims(ctx, next, remaining, acc)
      {:lease, lease, next} -> collect_claims(ctx, next, remaining - 1, [lease | acc])
    end
  end

  defp next_claim(%{state: state, stream: stream, group: group} = ctx, key) do
    case store(state).next_key(state.open_deliveries, key) do
      {^stream, ^group, partition, _seq} = next ->
        case try_claim(ctx, next, partition) do
          {:lease, lease} -> {:lease, lease, next}
          _not_claimable -> {:skip, next}
        end

      _past_group_or_end ->
        :done
    end
  end

  defp try_claim(%{partitions: partitions} = ctx, key, partition) do
    if MapSet.member?(partitions, partition) do
      %{state: state, stream: stream, group: group} = ctx

      with [{^key, event_id}] <- store(state).lookup(state.open_deliveries, key),
           [{_key, delivery}] <- store(state).lookup(state.deliveries, {stream, group, event_id}) do
        claim_if_ready(state, delivery, ctx.consumer_id, ctx.lease_ms, ctx.now)
      else
        _ -> nil
      end
    end
  end

  def walk_frontier(state, stream, group, partition, frontier) do
    next = frontier + 1
    seq_key = {stream, group, partition, next}

    terminal? =
      case store(state).lookup(state.delivery_by_seq, seq_key) do
        [{^seq_key, event_id}] ->
          case store(state).lookup(state.deliveries, {stream, group, event_id}) do
            [{_, %{status: status}}] when status in [:acked, :rejected] -> true
            _ -> false
          end

        [] ->
          false
      end

    if terminal?, do: walk_frontier(state, stream, group, partition, next), else: frontier
  end

  def group_deliveries(state, stream, group) do
    head = {{stream, group, :"$1"}, :"$2"}
    spec = [{head, [], [:"$2"]}]
    store(state).select(state.deliveries, spec)
  end

  def drop_after(rows, nil), do: {:ok, rows}

  def drop_after(rows, after_id) when is_binary(after_id) do
    case Enum.find_index(rows, &(&1.event_id == after_id)) do
      nil -> {:error, :cursor_not_found}
      idx -> {:ok, Enum.drop(rows, idx + 1)}
    end
  end

  def match_query?(%Event{} = event, %Query{} = query) do
    event.stream == query.stream and
      Enum.all?(query.where, &match_where?(event, &1)) and
      in_time_range?(event, query.from, query.to) and
      in_sequence_range?(event, query)
  end

  def stream_next_sequences(rec) when is_map(rec), do: Map.fetch!(rec, :next_sequences)
  def group_cursors(rec) when is_map(rec), do: Map.fetch!(rec, :cursors)
  def group_frontiers(rec) when is_map(rec), do: Map.get(rec, :frontiers) || %{}

  def sort_events(events, order_by) when is_list(order_by) do
    Enum.reduce(Enum.reverse(order_by), events, fn {field, dir}, acc ->
      sorter = fn a, b ->
        va = Map.get(a, field)
        vb = Map.get(b, field)

        case dir do
          :desc -> vb <= va
          _ -> va <= vb
        end
      end

      Enum.sort(acc, sorter)
    end)
  end

  def sort_events(events, _), do: events

  defp claim_if_ready(state, delivery, consumer_id, lease_ms, now) do
    if claimable?(delivery, now) do
      lease_id = Rheo.Id.generate()
      expires_at = DateTime.add(now, lease_ms, :millisecond)
      attempt = delivery.attempt + 1

      updated =
        Map.merge(delivery, %{
          status: :leased,
          lease_id: lease_id,
          consumer_id: consumer_id,
          attempt: attempt,
          leased_at: now,
          expires_at: expires_at
        })

      put_delivery(state, updated)
      maybe_redelivery(delivery.stream, delivery.group, delivery.event_id, attempt)

      case event_by_id(state, delivery.event_id) do
        {:ok, event} ->
          {:lease,
           %Rheo.Lease{
             lease_id: lease_id,
             stream: delivery.stream,
             group: delivery.group,
             event_id: delivery.event_id,
             event: event,
             consumer_id: consumer_id,
             attempt: attempt,
             leased_at: now,
             expires_at: expires_at,
             receipt: lease_id
           }}

        :error ->
          dead_letter_missing(state, updated)
          nil
      end
    end
  end

  defp dead_letter_missing(state, delivery) do
    Logger.warning(
      "Rheo fetch skipped missing event stream=#{delivery.stream} group=#{delivery.group} " <>
        "event_id=#{delivery.event_id}"
    )

    put_delivery(
      state,
      Map.merge(delivery, %{
        status: :rejected,
        dead_lettered_at: Clock.utc_now(),
        reason: {:event_missing, delivery.event_id},
        lease_id: nil,
        expires_at: nil
      })
    )

    Telemetry.execute([:rheo, :dead_letter], %{count: 1}, %{
      stream: delivery.stream,
      group: delivery.group,
      event_id: delivery.event_id
    })

    :ok
  end

  defp claimable?(%{status: :available}, _now), do: true

  defp claimable?(%{status: :leased, expires_at: expires_at}, now)
       when not is_nil(expires_at),
       do: DateTime.compare(expires_at, now) != :gt

  defp claimable?(_, _), do: false

  defp maybe_redelivery(_stream, _group, _event_id, attempt) when attempt <= 1, do: :ok

  defp maybe_redelivery(stream, group, event_id, _attempt) do
    Telemetry.execute([:rheo, :redelivery], %{count: 1}, %{
      stream: stream,
      group: group,
      event_id: event_id
    })
  end

  defp match_where?(event, {:type, type}), do: event.type == type
  defp match_where?(event, {:key, key}), do: event.key == key
  defp match_where?(event, {:partition, p}), do: event.partition == p

  defp match_where?(event, {field, value}) when field in @metadata_fields do
    Map.get(event.metadata, Atom.to_string(field)) == value or
      Map.get(event.metadata, field) == value
  end

  defp match_where?(event, {field, value}) when is_atom(field) do
    key = Atom.to_string(field)
    Map.get(event.payload, key) == value or Map.get(event.payload, field) == value
  end

  defp match_where?(_, _), do: false

  defp in_sequence_range?(event, %Query{} = query) do
    Query.past_after?(event, query) and
      (is_nil(query.until_sequence) or event.sequence <= query.until_sequence)
  end

  defp in_time_range?(_event, nil, nil), do: true

  defp in_time_range?(event, from, nil) when not is_nil(from),
    do: DateTime.compare(event.timestamp, from) != :lt

  defp in_time_range?(event, nil, to) when not is_nil(to),
    do: DateTime.compare(event.timestamp, to) != :gt

  defp in_time_range?(event, from, to),
    do: in_time_range?(event, from, nil) and in_time_range?(event, nil, to)

  defp store(%{store: store}), do: store
end
