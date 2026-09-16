defmodule Rheo.Backend.ETS do
  @moduledoc """
  In-memory ETS implementation of `Rheo.Backend`.

  Intended for tests, Livebook, and ephemeral apps. Data lives in per-instance
  ETS tables owned by this GenServer and **does not survive** owner or node
  restart (`durable: false`).

  ## Supervision example

      children = [
        {Rheo, name: MyRheo, backend: Rheo.Backend.ETS}
      ]

  The opaque handle is this process's registered name.
  """

  @behaviour Rheo.Backend
  use GenServer

  alias Rheo.{Clock, Event, Id, Lease, Query, Telemetry}

  defstruct [:name, :streams, :events, :groups, :deliveries]

  @impl true
  def capabilities do
    %{
      durable: false,
      distributed: false,
      atomic_compare_and_set: true,
      notifications: false,
      change_feed: false,
      secondary_indexes: false,
      batch_writes: true,
      ordered_range_scan: true,
      replay: true
    }
  end

  @impl true
  def child_spec(opts) do
    name = Keyword.get(opts, :name, default_handle())

    %{
      id: {__MODULE__, name},
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent
    }
  end

  @doc false
  def start_link(opts) do
    name = Keyword.get(opts, :name, default_handle())
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Default ETS handle name for the `Rheo` instance."
  @spec default_handle() :: atom()
  def default_handle, do: Rheo.ETS

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
    Telemetry.span([:rheo, :append], %{stream: stream}, fn ->
      with {:ok, [event]} <- append_batch(handle, stream, [payload], opts), do: {:ok, event}
    end)
  end

  @impl true
  def append_batch(handle, stream, payloads, opts \\ []) do
    Telemetry.span([:rheo, :append_batch], %{stream: stream, count: length(payloads)}, fn ->
      call(handle, {:append_batch, stream, payloads, opts})
    end)
  end

  @impl true
  def read(handle, stream, opts \\ []), do: call(handle, {:read, stream, opts})

  @impl true
  def query(handle, %Query{} = query) do
    Telemetry.span([:rheo, :query], %{stream: query.stream}, fn ->
      call(handle, {:query, query})
    end)
  end

  @impl true
  def fetch(handle, stream, group, opts \\ []) do
    Telemetry.span([:rheo, :fetch], %{stream: stream, group: group}, fn ->
      call(handle, {:fetch, stream, group, opts})
    end)
  end

  @impl true
  def renew(handle, %Lease{} = lease, opts \\ []) do
    Telemetry.span([:rheo, :lease, :renew], %{stream: lease.stream, group: lease.group}, fn ->
      call(handle, {:renew, lease, opts})
    end)
  end

  @impl true
  def ack(handle, %Lease{} = lease) do
    Telemetry.span([:rheo, :ack], %{stream: lease.stream, group: lease.group}, fn ->
      call(handle, {:ack, lease})
    end)
  end

  @impl true
  def retry(handle, %Lease{} = lease, reason) do
    Telemetry.span([:rheo, :retry], %{stream: lease.stream, group: lease.group}, fn ->
      call(handle, {:retry, lease, reason})
    end)
  end

  @impl true
  def reject(handle, %Lease{} = lease, reason) do
    Telemetry.span([:rheo, :reject], %{stream: lease.stream, group: lease.group}, fn ->
      call(handle, {:reject, lease, reason})
    end)
  end

  @impl true
  def replay(handle, stream, group, opts \\ []),
    do: call(handle, {:replay, stream, group, opts})

  @impl true
  def reset_group(handle, stream, group, opts \\ []),
    do: call(handle, {:reset_group, stream, group, opts})

  defp call(handle, request) do
    GenServer.call(handle, request)
  catch
    :exit, {:noproc, _} -> {:error, :backend_unavailable}
    :exit, {:timeout, _} -> {:error, :backend_unavailable}
  end

  @impl true
  def init(opts) do
    name = Keyword.get(opts, :name, default_handle())
    prefix = table_prefix(name)

    state = %__MODULE__{
      name: name,
      streams: :ets.new(:"#{prefix}.streams", [:set, :protected]),
      events: :ets.new(:"#{prefix}.events", [:ordered_set, :protected]),
      groups: :ets.new(:"#{prefix}.groups", [:set, :protected]),
      deliveries: :ets.new(:"#{prefix}.deliveries", [:set, :protected])
    }

    {:ok, state}
  end

  @impl true
  def handle_call(:ping, _from, state), do: {:reply, :ok, state}

  def handle_call(:ensure_indexes, _from, state), do: {:reply, :ok, state}

  def handle_call({:create_stream, stream, opts}, _from, state) do
    reply =
      case :ets.lookup(state.streams, stream) do
        [_] ->
          {:error, :already_exists}

        [] ->
          :ets.insert(state.streams, {stream, stream_rec(stream, opts)})
          Telemetry.execute([:rheo, :stream, :create], %{count: 1}, %{stream: stream})
          :ok
      end

    {:reply, reply, state}
  end

  def handle_call({:create_group, stream, group, opts}, _from, state) do
    reply =
      cond do
        :ets.lookup(state.streams, stream) == [] ->
          {:error, :stream_not_found}

        :ets.lookup(state.groups, {stream, group}) != [] ->
          {:error, :already_exists}

        true ->
          {:ok, next_sequence} = resolve_group_start(state, stream, opts)

          :ets.insert(
            state.groups,
            {{stream, group}, group_rec(stream, group, opts, next_sequence)}
          )

          Telemetry.execute([:rheo, :group, :create], %{count: 1}, %{
            stream: stream,
            group: group
          })

          :ok
      end

    {:reply, reply, state}
  end

  def handle_call({:append_batch, _stream, [], _opts}, _from, state),
    do: {:reply, {:ok, []}, state}

  def handle_call({:append_batch, stream, payloads, opts}, _from, state) do
    reply =
      case :ets.lookup(state.streams, stream) do
        [] ->
          {:error, :stream_not_found}

        [{^stream, rec}] ->
          start_seq = rec.next_sequence + 1
          partition = Keyword.get(opts, :partition, 0)
          now = Clock.utc_now()

          events =
            payloads
            |> Enum.with_index()
            |> Enum.map(fn {payload, idx} ->
              build_event(stream, partition, start_seq + idx, payload, now, opts)
            end)

          Enum.each(events, fn event ->
            :ets.insert(state.events, {{stream, partition, event.sequence}, event})
          end)

          :ets.insert(
            state.streams,
            {stream, %{rec | next_sequence: start_seq + length(payloads) - 1}}
          )

          {:ok, events}
      end

    {:reply, reply, state}
  end

  def handle_call({:read, stream, opts}, _from, state) do
    after_seq = Keyword.get(opts, :after, 0)
    limit = Keyword.get(opts, :limit, 100)
    partition = Keyword.get(opts, :partition, 0)

    events =
      state.events
      |> :ets.tab2list()
      |> Enum.filter(fn {{s, p, seq}, _} ->
        s == stream and p == partition and seq > after_seq
      end)
      |> Enum.sort_by(fn {{_, _, seq}, _} -> seq end)
      |> Enum.take(limit)
      |> Enum.map(fn {_, event} -> event end)

    {:reply, {:ok, events}, state}
  end

  def handle_call({:query, %Query{} = query}, _from, state) do
    query = Query.apply_cursor(query)

    events =
      state.events
      |> :ets.tab2list()
      |> Enum.map(fn {_, event} -> event end)
      |> Enum.filter(&match_query?(&1, query))
      |> sort_events(query.order_by)
      |> Enum.take(query.limit)

    {:reply, {:ok, events}, state}
  end

  def handle_call({:fetch, stream, group, opts}, _from, state) do
    reply =
      case :ets.lookup(state.groups, {stream, group}) do
        [] ->
          {:error, :group_not_found}

        [{{^stream, ^group}, group_rec}] ->
          limit = Keyword.get(opts, :limit, Application.get_env(:rheo, :default_max_demand, 10))
          consumer_id = Keyword.get_lazy(opts, :consumer_id, &Id.generate/0)

          lease_ms =
            Keyword.get(opts, :lease_ms, Application.get_env(:rheo, :default_lease_ms, 30_000))

          now = Clock.utc_now()
          materialize(state, stream, group, group_rec, max(limit, 50))
          leases = claim_many(state, stream, group, consumer_id, lease_ms, limit, now)

          Telemetry.execute([:rheo, :lease], %{count: length(leases)}, %{
            stream: stream,
            group: group,
            consumer_id: consumer_id
          })

          {:ok, leases}
      end

    {:reply, reply, state}
  end

  def handle_call({:renew, lease, opts}, _from, state) do
    lease_ms =
      Keyword.get(opts, :lease_ms, Application.get_env(:rheo, :default_lease_ms, 30_000))

    now = Clock.utc_now()
    expires_at = DateTime.add(now, lease_ms, :millisecond)

    reply =
      with {:ok, delivery} <- fetch_active_delivery(state, lease) do
        updated =
          Map.merge(delivery, %{
            expires_at: expires_at,
            renewed_at: now
          })

        put_delivery(state, updated)
        {:ok, %{lease | expires_at: expires_at}}
      end

    {:reply, reply, state}
  end

  def handle_call({:ack, lease}, _from, state) do
    reply =
      with {:ok, delivery} <- fetch_active_delivery(state, lease) do
        put_delivery(
          state,
          Map.merge(delivery, %{
            status: :acked,
            acked_at: Clock.utc_now(),
            lease_id: nil,
            expires_at: nil
          })
        )

        :ok
      end

    {:reply, reply, state}
  end

  def handle_call({:retry, lease, reason}, _from, state) do
    reply =
      case :ets.lookup(state.groups, {lease.stream, lease.group}) do
        [] ->
          {:error, :group_not_found}

        [{_, group_rec}] ->
          with {:ok, delivery} <- fetch_active_delivery(state, lease) do
            if lease.attempt >= group_rec.max_attempts do
              do_reject(state, lease, delivery, {:max_attempts, reason})
            else
              put_delivery(
                state,
                Map.merge(delivery, %{
                  status: :available,
                  lease_id: nil,
                  consumer_id: nil,
                  expires_at: nil,
                  reason: reason,
                  retried_at: Clock.utc_now()
                })
              )

              :ok
            end
          end
      end

    {:reply, reply, state}
  end

  def handle_call({:reject, lease, reason}, _from, state) do
    reply =
      with {:ok, delivery} <- fetch_active_delivery(state, lease) do
        do_reject(state, lease, delivery, reason)
      end

    {:reply, reply, state}
  end

  def handle_call({:replay, stream, group, opts}, _from, state) do
    reply =
      case :ets.lookup(state.groups, {stream, group}) do
        [] ->
          {:error, :group_not_found}

        [{key, group_rec}] ->
          cond do
            events = Keyword.get(opts, :events) ->
              reopen_ets_events(state, stream, group, events)
              :ok

            ids = Keyword.get(opts, :event_ids) ->
              reopen_ets_deliveries(state, stream, group, ids)
              :ok

            Keyword.has_key?(opts, :from_sequence) ->
              next = Keyword.fetch!(opts, :from_sequence) + 1
              reopen_ets_from_sequence(state, stream, group, next)
              :ets.insert(state.groups, {key, %{group_rec | next_sequence: next}})
              :ok

            true ->
              {:error, :invalid_replay_opts}
          end
      end

    {:reply, reply, state}
  end

  def handle_call({:reset_group, stream, group, opts}, _from, state) do
    reply =
      case :ets.lookup(state.groups, {stream, group}) do
        [] ->
          {:error, :group_not_found}

        [{key, group_rec}] ->
          start_seq = Keyword.get(opts, :start_after, 0) + 1

          state.deliveries
          |> :ets.tab2list()
          |> Enum.each(fn
            {{^stream, ^group, _} = key, _} -> :ets.delete(state.deliveries, key)
            _ -> :ok
          end)

          :ets.insert(state.groups, {key, %{group_rec | next_sequence: start_seq}})
          :ok
      end

    {:reply, reply, state}
  end

  defp do_reject(state, lease, delivery, reason) do
    put_delivery(
      state,
      Map.merge(delivery, %{
        status: :rejected,
        dead_lettered_at: Clock.utc_now(),
        reason: reason,
        lease_id: nil,
        expires_at: nil
      })
    )

    Telemetry.execute([:rheo, :dead_letter], %{count: 1}, %{
      stream: lease.stream,
      group: lease.group,
      event_id: lease.event_id
    })

    :ok
  end

  defp materialize(state, stream, group, group_rec, limit) do
    next_seq = group_rec.next_sequence

    events =
      state.events
      |> :ets.tab2list()
      |> Enum.filter(fn {{s, _p, seq}, _} -> s == stream and seq >= next_seq end)
      |> Enum.sort_by(fn {{_, _, seq}, _} -> seq end)
      |> Enum.take(limit)
      |> Enum.map(fn {_, event} -> event end)

    Enum.each(events, fn event ->
      key = {stream, group, event.id}

      case :ets.lookup(state.deliveries, key) do
        [] ->
          :ets.insert(
            state.deliveries,
            {key,
             %{
               stream: stream,
               group: group,
               event_id: event.id,
               partition: event.partition,
               sequence: event.sequence,
               status: :available,
               attempt: 0,
               lease_id: nil,
               consumer_id: nil,
               expires_at: nil,
               created_at: Clock.utc_now()
             }}
          )

        _ ->
          :ok
      end
    end)

    case List.last(events) do
      nil ->
        :ok

      last ->
        :ets.insert(
          state.groups,
          {{stream, group}, %{group_rec | next_sequence: last.sequence + 1}}
        )
    end
  end

  defp claim_many(state, stream, group, consumer_id, lease_ms, limit, now) do
    claim_loop(state, stream, group, consumer_id, lease_ms, limit, now, [])
  end

  defp claim_loop(_state, _stream, _group, _consumer_id, _lease_ms, 0, _now, acc),
    do: Enum.reverse(acc)

  defp claim_loop(state, stream, group, consumer_id, lease_ms, remaining, now, acc) do
    case claim_one(state, stream, group, consumer_id, lease_ms, now) do
      nil ->
        Enum.reverse(acc)

      lease ->
        claim_loop(state, stream, group, consumer_id, lease_ms, remaining - 1, now, [lease | acc])
    end
  end

  defp claim_one(state, stream, group, consumer_id, lease_ms, now) do
    candidates =
      state.deliveries
      |> :ets.tab2list()
      |> Enum.filter(fn {{s, g, _}, d} ->
        s == stream and g == group and claimable?(d, now)
      end)
      |> Enum.sort_by(fn {_, d} -> d.sequence end)

    case candidates do
      [] ->
        nil

      [{_key, delivery} | _] ->
        lease_id = Id.generate()
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
        maybe_redelivery(stream, group, delivery.event_id, attempt)
        event = event_by_id!(state, delivery.event_id)

        %Lease{
          lease_id: lease_id,
          stream: stream,
          group: group,
          event_id: delivery.event_id,
          event: event,
          consumer_id: consumer_id,
          attempt: attempt,
          leased_at: now,
          expires_at: expires_at
        }
    end
  end

  defp claimable?(%{status: :available}, _now), do: true

  defp claimable?(%{status: :leased, expires_at: expires_at}, now)
       when not is_nil(expires_at),
       do: DateTime.compare(expires_at, now) != :gt

  defp claimable?(_, _), do: false

  defp fetch_active_delivery(state, lease) do
    key = {lease.stream, lease.group, lease.event_id}

    case :ets.lookup(state.deliveries, key) do
      [{^key, %{status: :leased, lease_id: lid} = delivery}] when lid == lease.lease_id ->
        {:ok, delivery}

      _ ->
        {:error, :stale_lease}
    end
  end

  defp put_delivery(state, delivery) do
    key = {delivery.stream, delivery.group, delivery.event_id}
    :ets.insert(state.deliveries, {key, delivery})
  end

  defp event_by_id!(state, event_id) do
    state.events
    |> :ets.tab2list()
    |> Enum.find_value(fn
      {_, %Event{id: ^event_id} = event} -> event
      _ -> nil
    end) || raise "missing event #{event_id}"
  end

  defp maybe_redelivery(_stream, _group, _event_id, attempt) when attempt <= 1, do: :ok

  defp maybe_redelivery(stream, group, event_id, _attempt) do
    Telemetry.execute([:rheo, :redelivery], %{count: 1}, %{
      stream: stream,
      group: group,
      event_id: event_id
    })
  end

  defp match_query?(%Event{} = event, %Query{} = query) do
    event.stream == query.stream and
      Enum.all?(query.where, &match_where?(event, &1)) and
      in_time_range?(event, query.from, query.to) and
      in_sequence_range?(event, query.after_sequence, query.until_sequence)
  end

  defp match_where?(event, {:type, type}), do: event.type == type
  defp match_where?(event, {:key, key}), do: event.key == key
  defp match_where?(event, {:partition, p}), do: event.partition == p

  defp match_where?(event, {:currency, c}),
    do: Map.get(event.payload, "currency") == c or Map.get(event.payload, :currency) == c

  defp match_where?(event, {:curve, c}),
    do: Map.get(event.payload, "curve") == c or Map.get(event.payload, :curve) == c

  defp match_where?(event, {:correlation_id, id}),
    do:
      Map.get(event.metadata, "correlation_id") == id or
        Map.get(event.metadata, :correlation_id) == id

  defp match_where?(event, {:causation_id, id}),
    do:
      Map.get(event.metadata, "causation_id") == id or
        Map.get(event.metadata, :causation_id) == id

  defp match_where?(event, {:producer, p}),
    do: Map.get(event.metadata, "producer") == p or Map.get(event.metadata, :producer) == p

  defp match_where?(event, {:schema, s}),
    do: Map.get(event.metadata, "schema") == s or Map.get(event.metadata, :schema) == s

  defp match_where?(event, {field, value}) when is_atom(field) do
    key = Atom.to_string(field)
    Map.get(event.payload, key) == value or Map.get(event.payload, field) == value
  end

  defp match_where?(_, _), do: true

  defp in_sequence_range?(_event, nil, nil), do: true

  defp in_sequence_range?(event, after_seq, nil) when is_integer(after_seq),
    do: event.sequence > after_seq

  defp in_sequence_range?(event, nil, until_seq) when is_integer(until_seq),
    do: event.sequence <= until_seq

  defp in_sequence_range?(event, after_seq, until_seq),
    do: in_sequence_range?(event, after_seq, nil) and in_sequence_range?(event, nil, until_seq)

  defp in_time_range?(_event, nil, nil), do: true

  defp in_time_range?(event, from, nil) when not is_nil(from),
    do: DateTime.compare(event.timestamp, from) != :lt

  defp in_time_range?(event, nil, to) when not is_nil(to),
    do: DateTime.compare(event.timestamp, to) != :gt

  defp in_time_range?(event, from, to),
    do: in_time_range?(event, from, nil) and in_time_range?(event, nil, to)

  defp sort_events(events, order_by) when is_list(order_by) do
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

  defp stream_rec(stream, opts) do
    %{
      name: stream,
      partition_count: Keyword.get(opts, :partition_count, 1),
      next_sequence: 0,
      created_at: Clock.utc_now()
    }
  end

  defp group_rec(stream, group, opts, next_sequence) do
    %{
      stream: stream,
      name: group,
      next_sequence: next_sequence,
      max_attempts:
        Keyword.get(opts, :max_attempts, Application.get_env(:rheo, :default_max_attempts, 5)),
      created_at: Clock.utc_now()
    }
  end

  defp resolve_group_start(state, stream, opts) do
    cond do
      Keyword.has_key?(opts, :start_after) ->
        {:ok, Keyword.fetch!(opts, :start_after) + 1}

      Keyword.has_key?(opts, :start_at) ->
        dt = Keyword.fetch!(opts, :start_at)

        events =
          state.events
          |> :ets.tab2list()
          |> Enum.map(fn {_, e} -> e end)
          |> Enum.filter(fn e ->
            e.stream == stream and DateTime.compare(e.timestamp, dt) != :lt
          end)
          |> Enum.sort_by(& &1.sequence)

        case events do
          [%{sequence: seq} | _] -> {:ok, seq}
          [] -> {:ok, 1}
        end

      true ->
        {:ok, 1}
    end
  end

  defp reopen_ets_from_sequence(state, stream, group, next_sequence) do
    state.deliveries
    |> :ets.tab2list()
    |> Enum.each(fn
      {{^stream, ^group, _}, delivery} ->
        if delivery.sequence >= next_sequence do
          put_delivery(
            state,
            Map.merge(delivery, %{
              status: :available,
              lease_id: nil,
              consumer_id: nil,
              expires_at: nil,
              reason: :replay,
              retried_at: Clock.utc_now()
            })
          )
        end

      _ ->
        :ok
    end)
  end

  defp reopen_ets_deliveries(state, stream, group, event_ids) do
    id_set = MapSet.new(event_ids)

    state.deliveries
    |> :ets.tab2list()
    |> Enum.each(fn
      {{^stream, ^group, event_id}, delivery} ->
        if MapSet.member?(id_set, event_id) do
          put_delivery(
            state,
            Map.merge(delivery, %{
              status: :available,
              lease_id: nil,
              consumer_id: nil,
              expires_at: nil,
              reason: :replay,
              retried_at: Clock.utc_now()
            })
          )
        end

      _ ->
        :ok
    end)
  end

  defp reopen_ets_events(state, stream, group, events) do
    Enum.each(events, fn event ->
      key = {stream, group, event.id}

      delivery =
        case :ets.lookup(state.deliveries, key) do
          [{^key, existing}] ->
            existing

          [] ->
            %{
              stream: stream,
              group: group,
              event_id: event.id,
              partition: event.partition,
              sequence: event.sequence,
              attempt: 0,
              created_at: Clock.utc_now()
            }
        end

      put_delivery(
        state,
        Map.merge(delivery, %{
          status: :available,
          lease_id: nil,
          consumer_id: nil,
          expires_at: nil,
          reason: :replay,
          retried_at: Clock.utc_now(),
          partition: event.partition,
          sequence: event.sequence
        })
      )
    end)
  end

  defp build_event(stream, partition, sequence, payload, now, opts) do
    id = Keyword.get_lazy(opts, :id, &Id.generate/0)
    type = Map.get(payload, :type) || Map.get(payload, "type")
    key = Keyword.get(opts, :key) || Map.get(payload, :key) || Map.get(payload, "key")
    metadata = Keyword.get(opts, :metadata, %{})
    timestamp = Keyword.get(opts, :timestamp, now)
    {event_meta, event_payload} = split_metadata(payload, metadata)

    %Event{
      id: id,
      stream: stream,
      partition: partition,
      sequence: sequence,
      timestamp: timestamp,
      key: key && to_string(key),
      type: type && to_string(type),
      metadata: stringify_keys(event_meta),
      payload: stringify_keys(event_payload)
    }
  end

  defp split_metadata(payload, metadata) do
    case Map.pop(payload, :metadata) do
      {nil, rest} ->
        case Map.pop(rest, "metadata") do
          {nil, r} -> {metadata, r}
          {m, r} -> {Map.merge(metadata, m), r}
        end

      {m, rest} ->
        {Map.merge(metadata, m), rest}
    end
  end

  defp stringify_keys(map) when is_map(map) do
    Map.new(map, fn
      {k, v} when is_atom(k) -> {Atom.to_string(k), stringify_value(v)}
      {k, v} -> {k, stringify_value(v)}
    end)
  end

  defp stringify_value(%DateTime{} = dt), do: dt
  defp stringify_value(map) when is_map(map), do: stringify_keys(map)
  defp stringify_value(list) when is_list(list), do: Enum.map(list, &stringify_value/1)
  defp stringify_value(other), do: other

  defp table_prefix(name) when is_atom(name), do: Atom.to_string(name)
  defp table_prefix(name), do: inspect(name)
end
