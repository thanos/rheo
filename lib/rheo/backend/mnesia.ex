defmodule Rheo.Backend.Mnesia do
  @moduledoc """
  Durable OTP `:mnesia` implementation of `Rheo.Backend`.

  ETS-shaped tables on single-node `disc_copies` (v0.11+ / ADR 028). Survives
  process restart when the Mnesia directory is preserved (`durable: true`).
  Not multi-node in v0.11 (`distributed: false`).

  `dirty_write` to `disc_copies` is not crash-durable the way
  `:mnesia.sync_transaction` is (`atomic_compare_and_set: false`).

  ## Supervision example

      children = [
        {Rheo, name: MyRheo, backend: {Rheo.Backend.Mnesia, dir: "/var/lib/rheo/mnesia"}}
      ]

  Options: `:name` (default `Rheo.Mnesia`), `:dir` (Mnesia directory). Multiple
  instances on one node share the schema but use unique table name prefixes.

  The opaque handle is this process's registered name.
  """

  @behaviour Rheo.Backend
  use GenServer

  alias Rheo.Backend.TableEngine
  alias Rheo.{Clock, DeadLetter, Event, GroupInfo, Id, Lag, Lease, Partition, Query, Telemetry}

  defstruct [
    :name,
    :store,
    :streams,
    :events,
    :groups,
    :deliveries,
    :event_ids,
    :delivery_by_seq,
    :open_deliveries
  ]

  @impl true
  def capabilities do
    Rheo.Backend.Capabilities.new(%{
      durable: true,
      distributed: false,
      atomic_compare_and_set: false,
      notifications: false,
      change_feed: false,
      secondary_indexes: false,
      batch_writes: true,
      ordered_range_scan: true,
      replay: true,
      partitions: true,
      contiguous_frontier: true
    })
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

  @doc "Default Mnesia handle name for the `Rheo` instance."
  @spec default_handle() :: atom()
  def default_handle, do: Rheo.Mnesia

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

  @impl true
  def lag(handle, stream, group, opts \\ []),
    do: call(handle, {:lag, stream, group, opts})

  @impl true
  def list_streams(handle, opts \\ []),
    do: call(handle, {:list_streams, opts})

  @impl true
  def list_groups(handle, stream, opts \\ []),
    do: call(handle, {:list_groups, stream, opts})

  @impl true
  def dead_letters(handle, stream, group, opts \\ []),
    do: call(handle, {:dead_letters, stream, group, opts})

  @impl true
  def group_info(handle, stream, group, opts \\ []),
    do: call(handle, {:group_info, stream, group, opts})

  defp call(handle, request) do
    timeout = Application.get_env(:rheo, :mnesia_call_timeout, 5_000)
    GenServer.call(handle, request, timeout)
  catch
    :exit, {:noproc, _} -> {:error, :backend_unavailable}
    :exit, {:timeout, _} -> {:error, :backend_unavailable}
  end

  @impl true
  def init(opts) do
    name = Keyword.get(opts, :name, default_handle())
    prefix = table_prefix(name)
    dir = Keyword.get_lazy(opts, :dir, fn -> default_dir(name) end)

    :ok = ensure_mnesia!(dir)

    streams = table_atom(prefix, "streams")
    events = table_atom(prefix, "events")
    groups = table_atom(prefix, "groups")
    deliveries = table_atom(prefix, "deliveries")
    event_ids = table_atom(prefix, "event_ids")
    delivery_by_seq = table_atom(prefix, "delivery_by_seq")
    open_deliveries = table_atom(prefix, "open_deliveries")

    :ok = ensure_table(streams, :set)
    :ok = ensure_table(events, :ordered_set)
    :ok = ensure_table(groups, :set)
    :ok = ensure_table(deliveries, :set)
    :ok = ensure_table(event_ids, :set)
    :ok = ensure_table(delivery_by_seq, :set)
    :ok = ensure_table(open_deliveries, :ordered_set)

    tables = [
      streams,
      events,
      groups,
      deliveries,
      event_ids,
      delivery_by_seq,
      open_deliveries
    ]

    case store().wait_for_tables(tables, 10_000) do
      :ok -> :ok
      {:timeout, missing} -> raise "Mnesia tables not ready: #{inspect(missing)}"
    end

    state = %__MODULE__{
      name: name,
      store: Rheo.Backend.Table.Mnesia,
      streams: streams,
      events: events,
      groups: groups,
      deliveries: deliveries,
      event_ids: event_ids,
      delivery_by_seq: delivery_by_seq,
      open_deliveries: open_deliveries
    }

    rebuild_indexes(state)
    {:ok, state}
  end

  @impl true
  def handle_call(:ping, _from, state), do: {:reply, :ok, state}

  def handle_call(:ensure_indexes, _from, state) do
    rebuild_indexes(state)
    {:reply, :ok, state}
  end

  def handle_call({:create_stream, stream, opts}, _from, state) do
    reply =
      case lookup(state.streams, stream) do
        [_] ->
          {:error, :already_exists}

        [] ->
          partition_count = Keyword.get(opts, :partition_count, 1)

          if is_integer(partition_count) and partition_count >= 1 do
            insert(state.streams, {stream, stream_rec(stream, opts)})
            Telemetry.execute([:rheo, :stream, :create], %{count: 1}, %{stream: stream})
            :ok
          else
            {:error, :invalid_partition_count}
          end
      end

    {:reply, reply, state}
  end

  def handle_call({:create_group, stream, group, opts}, _from, state) do
    reply =
      cond do
        lookup(state.streams, stream) == [] ->
          {:error, :stream_not_found}

        lookup(state.groups, {stream, group}) != [] ->
          {:error, :already_exists}

        true ->
          [{^stream, stream_rec}] = lookup(state.streams, stream)

          with {:ok, cursors} <- resolve_group_start(state, stream, stream_rec, opts) do
            insert(
              state.groups,
              {{stream, group}, group_rec(stream, group, opts, cursors)}
            )

            Telemetry.execute([:rheo, :group, :create], %{count: 1}, %{
              stream: stream,
              group: group
            })

            :ok
          end
      end

    {:reply, reply, state}
  end

  def handle_call({:append_batch, _stream, [], _opts}, _from, state),
    do: {:reply, {:ok, []}, state}

  def handle_call({:append_batch, stream, payloads, opts}, _from, state) do
    reply =
      case lookup(state.streams, stream) do
        [] ->
          {:error, :stream_not_found}

        [{^stream, rec}] ->
          partition_count = Map.get(rec, :partition_count, 1)
          next_sequences = stream_next_sequences(rec)
          now = Clock.utc_now()

          with {:ok, events, updated_sequences} <-
                 build_partitioned_events(
                   stream,
                   payloads,
                   opts,
                   partition_count,
                   next_sequences,
                   now
                 ) do
            Enum.each(events, fn event ->
              TableEngine.insert_event(state, event)
            end)

            insert(state.streams, {stream, Map.put(rec, :next_sequences, updated_sequences)})
            {:ok, events}
          end
      end

    {:reply, reply, state}
  end

  def handle_call({:read, stream, opts}, _from, state) do
    after_seq = Keyword.get(opts, :after, 0)
    limit = Keyword.get(opts, :limit, 100)
    partition = Keyword.get(opts, :partition, 0)

    events = TableEngine.read_partition(state, stream, partition, after_seq, limit)

    {:reply, {:ok, events}, state}
  end

  def handle_call({:query, %Query{} = query}, _from, state) do
    {:reply, {:ok, TableEngine.query_events(state, query)}, state}
  end

  def handle_call({:fetch, stream, group, opts}, _from, state) do
    reply =
      case {lookup(state.streams, stream), lookup(state.groups, {stream, group})} do
        {[], _} ->
          {:error, :stream_not_found}

        {_, []} ->
          {:error, :group_not_found}

        {[{^stream, stream_rec}], [{{^stream, ^group}, group_rec}]} ->
          limit = Keyword.get(opts, :limit, Application.get_env(:rheo, :default_max_demand, 10))
          consumer_id = Keyword.get_lazy(opts, :consumer_id, &Id.generate/0)

          lease_ms =
            Keyword.get(opts, :lease_ms, Application.get_env(:rheo, :default_lease_ms, 30_000))

          with {:ok, partitions} <- resolve_assignment(opts, stream_rec.partition_count) do
            now = Clock.utc_now()
            materialize(state, stream, group, group_rec, partitions, max(limit, 50))

            leases =
              claim_many(
                state,
                stream,
                group,
                partitions,
                consumer_id,
                lease_ms,
                limit,
                now
              )

            Telemetry.execute([:rheo, :lease], %{count: length(leases)}, %{
              stream: stream,
              group: group,
              consumer_id: consumer_id
            })

            {:ok, leases}
          end
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

        advance_frontier(state, delivery)
        :ok
      end

    {:reply, reply, state}
  end

  def handle_call({:retry, lease, reason}, _from, state) do
    reply =
      case lookup(state.groups, {lease.stream, lease.group}) do
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
      case {lookup(state.streams, stream), lookup(state.groups, {stream, group})} do
        {[], _} ->
          {:error, :stream_not_found}

        {_, []} ->
          {:error, :group_not_found}

        {[{^stream, stream_rec}], [{key, group_rec}]} ->
          with {:ok, partitions} <- resolve_assignment(opts, stream_rec.partition_count) do
            cond do
              events = Keyword.get(opts, :events) ->
                reopen_ets_events(state, stream, group, events, partitions)
                rewind_frontiers_for_events(state, key, group_rec, events, partitions)
                :ok

              ids = Keyword.get(opts, :event_ids) ->
                reopened =
                  reopen_ets_deliveries(state, stream, group, ids, partitions)

                rewind_frontiers_for_deliveries(state, key, group_rec, reopened)
                :ok

              Keyword.has_key?(opts, :from_sequence) ->
                next = Keyword.fetch!(opts, :from_sequence) + 1
                reopen_ets_from_sequence(state, stream, group, next, partitions)
                cursors = put_partitions(group_cursors(group_rec), partitions, next)

                frontiers =
                  rewind_frontiers_to_sequence(
                    group_frontiers(group_rec),
                    partitions,
                    next
                  )

                insert(
                  state.groups,
                  {key, group_rec |> Map.put(:cursors, cursors) |> Map.put(:frontiers, frontiers)}
                )

                :ok

              true ->
                {:error, :invalid_replay_opts}
            end
          end
      end

    {:reply, reply, state}
  end

  def handle_call({:reset_group, stream, group, opts}, _from, state) do
    reply =
      case {lookup(state.streams, stream), lookup(state.groups, {stream, group})} do
        {[], _} ->
          {:error, :stream_not_found}

        {_, []} ->
          {:error, :group_not_found}

        {[{^stream, stream_rec}], [{key, group_rec}]} ->
          with {:ok, partitions} <- resolve_assignment(opts, stream_rec.partition_count),
               {:ok, starts} <- resolve_group_start(state, stream, stream_rec, opts) do
            delete_partition_deliveries(state, stream, group, partitions)

            cursors =
              Enum.reduce(partitions, group_cursors(group_rec), fn partition, acc ->
                Map.put(acc, partition, Partition.map_get(starts, partition, 1))
              end)

            frontiers = put_partitions(group_frontiers(group_rec), partitions, 0)

            insert(
              state.groups,
              {key, group_rec |> Map.put(:cursors, cursors) |> Map.put(:frontiers, frontiers)}
            )

            :ok
          end
      end

    {:reply, reply, state}
  end

  def handle_call({:lag, stream, group, opts}, _from, state) do
    reply =
      case {lookup(state.streams, stream), lookup(state.groups, {stream, group})} do
        {[], _} ->
          {:error, :stream_not_found}

        {_, []} ->
          {:error, :group_not_found}

        {[{^stream, stream_rec}], [{_, group_rec}]} ->
          with {:ok, partitions} <- resolve_assignment(opts, stream_rec.partition_count) do
            {:ok,
             Lag.from_maps(
               stream,
               group,
               group_frontiers(group_rec),
               stream_next_sequences(stream_rec),
               partitions
             )}
          end
      end

    {:reply, reply, state}
  end

  def handle_call({:list_streams, _opts}, _from, state) do
    streams =
      state.streams
      |> tab2list()
      |> Enum.map(fn {name, _} -> name end)
      |> Enum.sort()

    {:reply, {:ok, streams}, state}
  end

  def handle_call({:list_groups, stream, _opts}, _from, state) do
    reply =
      case lookup(state.streams, stream) do
        [] ->
          {:error, :stream_not_found}

        [_] ->
          groups =
            state.groups
            |> tab2list()
            |> Enum.filter(fn {{s, _g}, _} -> s == stream end)
            |> Enum.map(fn {{_s, g}, _} -> g end)
            |> Enum.sort()

          {:ok, groups}
      end

    {:reply, reply, state}
  end

  def handle_call({:dead_letters, stream, group, opts}, _from, state) do
    reply =
      case lookup(state.groups, {stream, group}) do
        [] ->
          {:error, :group_not_found}

        [_] ->
          limit = Keyword.get(opts, :limit, 100)
          after_id = Keyword.get(opts, :after)

          rows =
            state
            |> TableEngine.group_deliveries(stream, group)
            |> Enum.filter(&(&1.status == :rejected))
            |> Enum.map(&delivery_to_dead_letter(state, &1))
            |> Enum.sort_by(fn %DeadLetter{sequence: seq, dead_lettered_at: at} ->
              {seq || 0, at || ~U[1970-01-01 00:00:00Z]}
            end)

          case TableEngine.drop_after(rows, after_id) do
            {:ok, rows} -> {:ok, Enum.take(rows, limit)}
            error -> error
          end
      end

    {:reply, reply, state}
  end

  def handle_call({:group_info, stream, group, opts}, _from, state) do
    reply =
      case {lookup(state.streams, stream), lookup(state.groups, {stream, group})} do
        {[], _} ->
          {:error, :stream_not_found}

        {_, []} ->
          {:error, :group_not_found}

        {[{^stream, stream_rec}], [{_, group_rec}]} ->
          with {:ok, partitions} <- resolve_assignment(opts, stream_rec.partition_count) do
            lag =
              Lag.from_maps(
                stream,
                group,
                group_frontiers(group_rec),
                stream_next_sequences(stream_rec),
                partitions
              )

            {inflight, dead} = count_group_statuses(state, stream, group)

            {:ok,
             %GroupInfo{
               stream: stream,
               group: group,
               lag: lag,
               inflight_count: inflight,
               dead_letter_count: dead
             }}
          end
      end

    {:reply, reply, state}
  end

  defp count_group_statuses(state, stream, group) do
    state
    |> TableEngine.group_deliveries(stream, group)
    |> Enum.reduce({0, 0}, fn
      %{status: :leased}, {inf, dl} -> {inf + 1, dl}
      %{status: :rejected}, {inf, dl} -> {inf, dl + 1}
      _, acc -> acc
    end)
  end

  defp delivery_to_dead_letter(state, delivery) do
    event =
      case lookup(
             state.events,
             {delivery.stream, delivery.partition, delivery.sequence}
           ) do
        [{_, %Event{} = e}] -> e
        _ -> nil
      end

    %DeadLetter{
      stream: delivery.stream,
      group: delivery.group,
      event_id: delivery.event_id,
      partition: delivery.partition,
      sequence: delivery.sequence,
      reason: Map.get(delivery, :reason),
      dead_lettered_at: Map.get(delivery, :dead_lettered_at),
      event: event
    }
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

    advance_frontier(state, delivery)
    :ok
  end

  defp materialize(state, stream, group, group_rec, partitions, limit) do
    cursors =
      Enum.reduce(partitions, group_cursors(group_rec), fn partition, cursors ->
        next_sequence = Partition.map_get(cursors, partition, 1)

        events = TableEngine.events_from_sequence(state, stream, partition, next_sequence, limit)

        Enum.each(events, &materialize_event(state, stream, group, &1))

        case List.last(events) do
          nil -> cursors
          last -> Map.put(cursors, partition, last.sequence + 1)
        end
      end)

    updated =
      group_rec
      |> Map.put(:cursors, cursors)
      |> Map.put(:frontiers, group_frontiers(group_rec))

    insert(state.groups, {{stream, group}, updated})
  end

  defp materialize_event(state, stream, group, event) do
    key = {stream, group, event.id}

    case lookup(state.deliveries, key) do
      [] ->
        TableEngine.put_delivery(state, %{
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
        })

      _ ->
        :ok
    end
  end

  defp claim_many(state, stream, group, partitions, consumer_id, lease_ms, limit, now) do
    TableEngine.claim_batch(state, stream, group, partitions, consumer_id, lease_ms, limit, now)
  end

  defp fetch_active_delivery(state, lease) do
    key = {lease.stream, lease.group, lease.event_id}

    case lookup(state.deliveries, key) do
      [{^key, %{status: :leased, lease_id: lid} = delivery}] when lid == lease.lease_id ->
        {:ok, delivery}

      _ ->
        {:error, :stale_lease}
    end
  end

  defp put_delivery(state, delivery), do: TableEngine.put_delivery(state, delivery)

  defp advance_frontier(state, delivery) do
    key = {delivery.stream, delivery.group}

    case lookup(state.groups, key) do
      [{^key, group_rec}] ->
        frontiers = group_frontiers(group_rec)
        old_frontier = Partition.map_get(frontiers, delivery.partition, 0)

        frontier =
          walk_frontier(state, delivery.stream, delivery.group, delivery.partition, old_frontier)

        if frontier > old_frontier do
          updated_frontiers = Map.put(frontiers, delivery.partition, frontier)
          insert(state.groups, {key, Map.put(group_rec, :frontiers, updated_frontiers)})

          Telemetry.execute(
            [:rheo, :group, :frontier],
            %{frontier: frontier},
            %{
              stream: delivery.stream,
              group: delivery.group,
              partition: delivery.partition
            }
          )
        end

      [] ->
        :ok
    end
  end

  defp walk_frontier(state, stream, group, partition, frontier) do
    TableEngine.walk_frontier(state, stream, group, partition, frontier)
  end

  defp stream_rec(stream, opts) do
    partition_count = Keyword.get(opts, :partition_count, 1)

    %{
      name: stream,
      partition_count: partition_count,
      next_sequences: Map.new(0..(partition_count - 1), &{&1, 0}),
      created_at: Clock.utc_now()
    }
  end

  defp group_rec(stream, group, opts, cursors) do
    %{
      stream: stream,
      name: group,
      cursors: cursors,
      frontiers: Map.new(Map.keys(cursors), &{&1, 0}),
      max_attempts:
        Keyword.get(opts, :max_attempts, Application.get_env(:rheo, :default_max_attempts, 5)),
      created_at: Clock.utc_now()
    }
  end

  defp resolve_group_start(state, stream, stream_rec, opts) do
    partition_count = stream_rec.partition_count

    with {:ok, selected} <- resolve_assignment(opts, partition_count) do
      selected_set = MapSet.new(selected)
      default_cursors = Map.new(0..(partition_count - 1), &{&1, 1})

      cursors =
        cond do
          Keyword.has_key?(opts, :start_after) ->
            apply_start_after_cursors(
              selected,
              Keyword.fetch!(opts, :start_after),
              default_cursors
            )

          Keyword.has_key?(opts, :start_at) ->
            datetime = Keyword.fetch!(opts, :start_at)

            apply_start_at_cursors(
              state,
              stream,
              partition_count,
              selected_set,
              datetime,
              default_cursors
            )

          true ->
            default_cursors
        end

      {:ok, cursors}
    end
  end

  defp reopen_ets_from_sequence(state, stream, group, next_sequence, partitions) do
    partition_set = MapSet.new(partitions)

    state
    |> TableEngine.group_deliveries(stream, group)
    |> Enum.each(fn delivery ->
      if MapSet.member?(partition_set, delivery.partition) and
           delivery.sequence >= next_sequence do
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
    end)
  end

  defp reopen_ets_deliveries(state, stream, group, event_ids, partitions) do
    id_set = MapSet.new(event_ids)
    partition_set = MapSet.new(partitions)

    state
    |> TableEngine.group_deliveries(stream, group)
    |> Enum.reduce([], fn delivery, acc ->
      if MapSet.member?(id_set, delivery.event_id) and
           MapSet.member?(partition_set, delivery.partition) do
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

        [delivery | acc]
      else
        acc
      end
    end)
  end

  defp reopen_ets_events(state, stream, group, events, partitions) do
    partition_set = MapSet.new(partitions)

    Enum.each(events, fn event ->
      if MapSet.member?(partition_set, event.partition) do
        key = {stream, group, event.id}

        delivery =
          case lookup(state.deliveries, key) do
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
      end
    end)
  end

  defp build_partitioned_events(
         stream,
         payloads,
         opts,
         partition_count,
         next_sequences,
         now
       ) do
    payloads
    |> Enum.reduce_while({:ok, [], next_sequences}, fn payload, {:ok, events, sequences} ->
      case Partition.resolve(payload, opts, partition_count) do
        {:ok, partition} ->
          sequence = Partition.map_get(sequences, partition, 0) + 1
          event = build_event(stream, partition, sequence, payload, now, opts)

          {:cont, {:ok, [event | events], Map.put(sequences, partition, sequence)}}

        {:error, :invalid_partition} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, events, sequences} -> {:ok, Enum.reverse(events), sequences}
      error -> error
    end
  end

  defp resolve_assignment(opts, partition_count) do
    assignment =
      cond do
        Keyword.has_key?(opts, :partition) -> Keyword.fetch!(opts, :partition)
        Keyword.has_key?(opts, :partitions) -> Keyword.fetch!(opts, :partitions)
        true -> :all
      end

    Partition.normalize_assignment(assignment, partition_count)
  end

  defp stream_next_sequences(rec), do: TableEngine.stream_next_sequences(rec)
  defp group_cursors(rec), do: TableEngine.group_cursors(rec)
  defp group_frontiers(rec), do: TableEngine.group_frontiers(rec)

  defp put_partitions(map, partitions, value) do
    Enum.reduce(partitions, map, &Map.put(&2, &1, value))
  end

  defp rewind_frontiers_to_sequence(frontiers, partitions, next) do
    replay_frontier = max(next - 1, 0)

    Enum.reduce(partitions, frontiers, fn partition, acc ->
      Map.put(acc, partition, min(Partition.map_get(acc, partition, 0), replay_frontier))
    end)
  end

  defp delete_partition_deliveries(state, stream, group, partitions) do
    partition_set = MapSet.new(partitions)

    state
    |> TableEngine.group_deliveries(stream, group)
    |> Enum.each(fn delivery ->
      if MapSet.member?(partition_set, delivery.partition) do
        TableEngine.delete_delivery(state, delivery)
      end
    end)
  end

  defp apply_start_at_cursors(
         state,
         stream,
         partition_count,
         selected_set,
         datetime,
         default_cursors
       ) do
    Enum.reduce(0..(partition_count - 1), default_cursors, fn partition, acc ->
      apply_start_at_partition(state, stream, partition, selected_set, datetime, acc)
    end)
  end

  defp apply_start_after_cursors(selected, start_after, default_cursors) do
    Enum.reduce(selected, default_cursors, fn partition, acc ->
      put_start_after_cursor(acc, partition, start_after_for_partition(start_after, partition))
    end)
  end

  defp put_start_after_cursor(acc, _partition, nil), do: acc

  defp put_start_after_cursor(acc, partition, sequence),
    do: Map.put(acc, partition, sequence + 1)

  defp apply_start_at_partition(state, stream, partition, selected_set, datetime, acc) do
    if MapSet.member?(selected_set, partition) do
      Map.put(acc, partition, first_sequence_at(state, stream, partition, datetime, 1))
    else
      acc
    end
  end

  defp start_after_for_partition(sequence, _partition) when is_integer(sequence), do: sequence

  defp start_after_for_partition(sequences, partition) when is_map(sequences) do
    string_key = Partition.key(partition)

    cond do
      Map.has_key?(sequences, partition) -> Map.get(sequences, partition)
      Map.has_key?(sequences, string_key) -> Map.get(sequences, string_key)
      true -> nil
    end
  end

  defp first_sequence_at(state, stream, partition, datetime, default) do
    state
    |> TableEngine.events_from_sequence(stream, partition, 1, 1_000_000)
    |> Enum.find(fn event -> DateTime.compare(event.timestamp, datetime) != :lt end)
    |> case do
      nil -> default
      event -> event.sequence
    end
  end

  defp rewind_frontiers_for_events(state, key, group_rec, events, partitions) do
    partition_set = MapSet.new(partitions)

    frontiers =
      Enum.reduce(events, group_frontiers(group_rec), fn event, acc ->
        if MapSet.member?(partition_set, event.partition) do
          old = Partition.map_get(acc, event.partition, 0)
          Map.put(acc, event.partition, min(old, max(event.sequence - 1, 0)))
        else
          acc
        end
      end)

    insert(state.groups, {key, Map.put(group_rec, :frontiers, frontiers)})
  end

  defp rewind_frontiers_for_deliveries(state, key, group_rec, deliveries) do
    frontiers =
      Enum.reduce(deliveries, group_frontiers(group_rec), fn delivery, acc ->
        old = Partition.map_get(acc, delivery.partition, 0)
        Map.put(acc, delivery.partition, min(old, max(delivery.sequence - 1, 0)))
      end)

    insert(state.groups, {key, Map.put(group_rec, :frontiers, frontiers)})
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

  defp default_dir(name) do
    Path.join(System.tmp_dir!(), "rheo_mnesia_#{sanitize_dir_token(name)}")
  end

  defp sanitize_dir_token(name) do
    name
    |> to_string()
    |> String.replace(~r/[^A-Za-z0-9_\-]/, "_")
  end

  defp store, do: Rheo.Backend.Mnesia.Store.current()

  defp ensure_mnesia!(dir) when is_binary(dir) do
    File.mkdir_p!(dir)
    dir_c = String.to_charlist(dir)
    _ = Application.ensure_loaded(:mnesia)
    Application.put_env(:mnesia, :dir, dir_c)

    if store().running?() do
      if store().disc_schema?() do
        :ok
      else
        # Rare: host already started `:mnesia` without disc_copies. Recycle once.
        _ = store().stop()
        wait_until_mnesia_stopped!()
        bootstrap_mnesia!(dir_c)
      end
    else
      bootstrap_mnesia!(dir_c)
    end
  end

  defp bootstrap_mnesia!(dir_c) do
    _ = Application.ensure_loaded(:mnesia)
    Application.put_env(:mnesia, :dir, dir_c)

    case store().create_schema([node()]) do
      :ok -> :ok
      {:error, {_, {:already_exists, _}}} -> :ok
      {:error, reason} -> raise "mnesia.create_schema failed: #{inspect(reason)}"
    end

    case store().start() do
      :ok -> :ok
      {:error, {:already_started, :mnesia}} -> :ok
      {:error, reason} -> raise "mnesia.start failed: #{inspect(reason)}"
    end

    unless store().disc_schema?() do
      raise "Mnesia schema has no disc_copies on #{inspect(node())} (dir=#{inspect(dir_c)})"
    end

    :ok
  end

  defp wait_until_mnesia_stopped!(attempts \\ 50)
  defp wait_until_mnesia_stopped!(0), do: :ok

  defp wait_until_mnesia_stopped!(attempts) do
    if store().running?() do
      Process.sleep(20)
      wait_until_mnesia_stopped!(attempts - 1)
    else
      :ok
    end
  end

  defp table_atom(prefix, suffix), do: String.to_atom("#{prefix}.#{suffix}")

  defp rebuild_indexes(state) do
    Enum.each(tab2list(state.events), fn
      {key, %Event{id: id}} -> insert(state.event_ids, {id, key})
      _ -> :ok
    end)

    Enum.each(tab2list(state.deliveries), fn
      {_key, delivery} when is_map(delivery) -> TableEngine.put_delivery(state, delivery)
      _ -> :ok
    end)
  end

  defp ensure_table(name, type) when type in [:set, :ordered_set] do
    case store().create_table(name,
           attributes: [:key, :value],
           type: type,
           disc_copies: [node()],
           record_name: name
         ) do
      {:atomic, :ok} -> :ok
      {:aborted, {:already_exists, ^name}} -> :ok
      {:aborted, reason} -> raise "mnesia.create_table #{name} failed: #{inspect(reason)}"
    end
  end

  defp lookup(table, key) do
    case store().dirty_read(table, key) do
      [{^table, ^key, value}] -> [{key, value}]
      [] -> []
    end
  end

  defp insert(table, {key, value}) do
    :ok = store().dirty_write({table, key, value})
  end

  defp tab2list(table) do
    store().dirty_select(table, [
      {{table, :"$1", :"$2"}, [], [{{:"$1", :"$2"}}]}
    ])
  end
end
