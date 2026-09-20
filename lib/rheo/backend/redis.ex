# Optional integration: compiled only when `:redix` is present.
if Code.ensure_loaded?(Redix) do
  defmodule Rheo.Backend.Redis do
    @moduledoc """
    Redis Streams implementation of `Rheo.Backend` (ADR 026).

    Requires `{:redix, "~> 1.5"}` in your dependencies and Redis **6.2+**.

    Prefer the `Rheo` facade for application code. The opaque handle is the
    Redix process name (or pid) started via `child_spec/1`.

    ## Model C

    One Redis STREAM per Rheo partition. Rheo keeps the portable
    `event.sequence` in a counter and a `sequence → entry id` sorted set; the
    lease `receipt` is the Redis entry id (ADR 021). Redis owns delivery
    mechanics (consumer groups, pending list, `XCLAIM` reclaim), Rheo owns
    semantics (fencing, attempts, dead-letter, contiguous frontier).

    Fencing is stronger than `XACK`: every claim writes a fence hash keyed by
    entry id holding the current `lease_id`. A settle call validates the fence
    before `XACK`, so a stale holder can never acknowledge someone else's claim.

    ## Keys

    See `Rheo.Backend.Redis.Keys`. Everything lives under `rheo:{handle}:`, so
    several instances may share one Redis database.

    ## Supervision example

        children = [
          {Rheo,
           name: MyRheo,
           backend: {Rheo.Backend.Redis, name: MyRheo.Redis, url: "redis://localhost:6379"}}
        ]

    `child_spec/1` starts two Redix connections: the handle itself and a
    `…​.Waiter` connection used only by `wait/2`, so a blocking read never
    stalls commands on the main connection. `:pool_size` is not supported in
    v0.9.

    ## Query cost

    `secondary_indexes: false`: `query/2` walks the partition streams and
    filters in the adapter. Bound queries with `:after_sequence` /
    `:until_sequence` on large streams.

    Callback semantics are documented on `Rheo.Backend`.
    """

    @behaviour Rheo.Backend
    @behaviour Rheo.Backend.Wakeup

    alias Rheo.Backend.Redis.Codec
    alias Rheo.Backend.Redis.Keys
    alias Rheo.{Clock, DeadLetter, Event, GroupInfo, Id, Lag, Lease, Partition, Query, Telemetry}

    @command_timeout 5_000
    @pending_scan 100
    @pending_page 1_000
    @default_consumer "rheo"
    @default_wait_ms 1_000

    @doc """
    Child spec for the Redis connections (backend handle).

    ## Arguments

      * `opts` — keyword options:
        * `:name` — handle / process name (default `default_handle/0`)
        * `:url` — `redis://host:port/db` (or `rediss://…` for TLS)
        * `:host` / `:port` / `:database` / `:password` — used when `:url` is absent
        * other options are forwarded to `Redix.start_link/1`

    ## Examples

        iex> spec = Rheo.Backend.Redis.child_spec(name: :demo_redis, url: "redis://localhost:6379")
        iex> {spec.id, spec.type}
        {{Rheo.Backend.Redis, :demo_redis}, :supervisor}

    ## Returns

    A supervisor child spec map.
    """
    @impl Rheo.Backend
    @spec child_spec(keyword()) :: Supervisor.child_spec()
    def child_spec(opts) do
      name = Keyword.get(opts, :name, default_handle())

      %{
        id: {__MODULE__, name},
        start: {__MODULE__, :start_link, [Keyword.put(opts, :name, name)]},
        type: :supervisor,
        restart: :permanent
      }
    end

    @doc false
    @spec start_link(keyword()) :: Supervisor.on_start()
    def start_link(opts) do
      name = Keyword.get(opts, :name, default_handle())
      conn_opts = connection_opts(opts)

      children =
        [
          Supervisor.child_spec({Redix, Keyword.put(conn_opts, :name, name)}, id: {Redix, name})
        ] ++ waiter_children(name, conn_opts)

      Supervisor.start_link(children, [strategy: :one_for_one] ++ supervisor_name(name))
    end

    @doc """
    Default Redis handle name for the `Rheo` instance.

    ## Examples

        iex> Rheo.Backend.Redis.default_handle()
        Rheo.Redis

    ## Returns

    A process name atom (default `Rheo.Redis`).
    """
    @spec default_handle() :: atom()
    def default_handle, do: Application.get_env(:rheo, :redis_name, Rheo.Redis)

    @impl Rheo.Backend
    def capabilities do
      Rheo.Backend.Capabilities.new(%{
        durable: true,
        distributed: true,
        partitions: true,
        contiguous_frontier: true,
        replay: true,
        ordered_range_scan: true,
        secondary_indexes: false,
        batch_writes: true,
        atomic_compare_and_set: true,
        native_consumer_groups: true,
        native_pending_list: true,
        native_reclaim: true,
        blocking_reads: true,
        native_group_lag: true
      })
    end

    @impl Rheo.Backend
    def ping(handle) do
      case command(handle, ["PING"]) do
        {:ok, _} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end

    @impl Rheo.Backend
    def ensure_indexes(_handle), do: :ok

    @impl Rheo.Backend
    def create_stream(handle, stream, opts \\ []) when is_binary(stream) do
      partition_count = Keyword.get(opts, :partition_count, 1)

      if is_integer(partition_count) and partition_count >= 1 do
        do_create_stream(handle, stream, partition_count)
      else
        {:error, :invalid_partition_count}
      end
    end

    defp do_create_stream(handle, stream, partition_count) do
      key = Keys.meta(handle, stream)

      case command(handle, ["HSETNX", key, "partition_count", to_str(partition_count)]) do
        {:ok, 1} ->
          with {:ok, _} <-
                 command(handle, ["HSET", key, "created_at", iso(Clock.utc_now())]) do
            Telemetry.execute([:rheo, :stream, :create], %{count: 1}, %{stream: stream})
            :ok
          end

        {:ok, 0} ->
          {:error, :already_exists}

        {:error, reason} ->
          {:error, reason}
      end
    end

    @impl Rheo.Backend
    def create_group(handle, stream, group, opts \\ [])
        when is_binary(stream) and is_binary(group) do
      with {:ok, partition_count} <- partition_count(handle, stream),
           {:ok, starts} <- group_start_sequences(handle, stream, partition_count, opts),
           :ok <- claim_group_name(handle, stream, group, opts) do
        with :ok <- open_redis_groups(handle, stream, group, starts),
             :ok <- write_group_cursors(handle, stream, group, starts) do
          Telemetry.execute([:rheo, :group, :create], %{count: 1}, %{
            stream: stream,
            group: group
          })

          :ok
        end
      end
    end

    defp claim_group_name(handle, stream, group, opts) do
      max_attempts =
        Keyword.get(opts, :max_attempts, Application.get_env(:rheo, :default_max_attempts, 5))

      key = Keys.group_meta(handle, stream, group)

      case command(handle, ["HSETNX", key, "max_attempts", to_str(max_attempts)]) do
        {:ok, 1} -> :ok
        {:ok, 0} -> {:error, :already_exists}
        {:error, reason} -> {:error, reason}
      end
    end

    defp open_redis_groups(handle, stream, group, starts) do
      Enum.reduce_while(starts, :ok, fn {partition, sequence}, :ok ->
        case create_redis_group(handle, stream, group, partition, sequence) do
          :ok -> {:cont, :ok}
          {:error, _} = error -> {:halt, error}
        end
      end)
    end

    defp create_redis_group(handle, stream, group, partition, sequence) do
      with {:ok, entry_id} <- entry_id_at_or_before(handle, stream, partition, sequence) do
        cmd = [
          "XGROUP",
          "CREATE",
          Keys.events(handle, stream, partition),
          group,
          entry_id,
          "MKSTREAM"
        ]

        case tolerant_command(handle, cmd, "BUSYGROUP") do
          {:ok, _} -> :ok
          {:error, reason} -> {:error, reason}
        end
      end
    end

    defp write_group_cursors(handle, stream, group, starts) do
      fields =
        Enum.flat_map(starts, fn {partition, sequence} ->
          [
            Keys.frontier_field(partition),
            to_str(sequence),
            Keys.cursor_field(partition),
            to_str(sequence)
          ]
        end)

      with {:ok, _} <-
             command(handle, ["HSET", Keys.group_meta(handle, stream, group)] ++ fields) do
        :ok
      end
    end

    @impl Rheo.Backend
    def append(handle, stream, payload, opts \\ []) when is_map(payload) do
      Telemetry.span([:rheo, :append], %{stream: stream}, fn ->
        with {:ok, [event]} <- append_batch(handle, stream, [payload], opts), do: {:ok, event}
      end)
    end

    @impl Rheo.Backend
    def append_batch(handle, stream, payloads, opts \\ []) when is_list(payloads) do
      Telemetry.span([:rheo, :append_batch], %{stream: stream, count: length(payloads)}, fn ->
        do_append_batch(handle, stream, payloads, opts)
      end)
    end

    defp do_append_batch(_handle, _stream, [], _opts), do: {:ok, []}

    defp do_append_batch(handle, stream, payloads, opts) do
      with {:ok, partition_count} <- partition_count(handle, stream),
           {:ok, routed} <- resolve_payload_partitions(payloads, opts, partition_count),
           {:ok, starts} <- allocate_sequences(handle, stream, routed) do
        write_entries(handle, stream, routed, starts, opts)
      end
    end

    defp allocate_sequences(handle, stream, routed) do
      counts = routed |> Enum.frequencies_by(&elem(&1, 1)) |> Enum.sort_by(&elem(&1, 0))

      cmds =
        Enum.map(counts, fn {partition, count} ->
          ["INCRBY", Keys.sequence(handle, stream, partition), to_str(count)]
        end)

      with {:ok, results} <- pipeline(handle, cmds) do
        starts =
          counts
          |> Enum.zip(results)
          |> Map.new(fn {{partition, count}, last} ->
            {partition, Codec.to_integer(last, count) - count + 1}
          end)

        {:ok, starts}
      end
    end

    defp write_entries(handle, stream, routed, starts, opts) do
      now = Clock.utc_now()

      {events, _offsets} =
        Enum.map_reduce(routed, %{}, fn {payload, partition}, offsets ->
          offset = Map.get(offsets, partition, 0)
          sequence = Map.fetch!(starts, partition) + offset
          event = build_event(stream, partition, sequence, payload, now, opts)
          {event, Map.put(offsets, partition, offset + 1)}
        end)

      adds =
        Enum.map(events, fn event ->
          ["XADD", Keys.events(handle, stream, event.partition), "*"] ++ Codec.to_fields(event)
        end)

      with {:ok, entry_ids} <- pipeline(handle, adds),
           {:ok, _} <- pipeline(handle, index_commands(handle, stream, events, entry_ids)) do
        {:ok, events}
      end
    end

    defp index_commands(handle, stream, events, entry_ids) do
      events
      |> Enum.zip(entry_ids)
      |> Enum.map(fn {event, entry_id} ->
        [
          "ZADD",
          Keys.index(handle, stream, event.partition),
          to_str(event.sequence),
          entry_id
        ]
      end)
    end

    defp build_event(stream, partition, sequence, payload, now, opts) do
      {metadata, body} = split_metadata(payload, Keyword.get(opts, :metadata, %{}))

      %Event{
        id: Keyword.get_lazy(opts, :id, &Id.generate/0),
        stream: stream,
        partition: partition,
        sequence: sequence,
        timestamp: Keyword.get(opts, :timestamp, now),
        key: Keyword.get(opts, :key) || Map.get(payload, :key) || Map.get(payload, "key"),
        type: Map.get(payload, :type) || Map.get(payload, "type"),
        metadata: metadata,
        payload: body
      }
    end

    defp split_metadata(payload, metadata) do
      {inline, rest} = Map.pop_lazy(payload, :metadata, fn -> Map.get(payload, "metadata") end)
      rest = Map.drop(rest, ["metadata"])
      {Map.merge(metadata, stringify_keys(inline || %{})), stringify_keys(rest)}
    end

    @impl Rheo.Backend
    def read(handle, stream, opts \\ []) do
      partition = Keyword.get(opts, :partition, 0)
      after_sequence = Keyword.get(opts, :after, 0)
      limit = Keyword.get(opts, :limit, 100)

      load_range(handle, stream, partition, after_sequence + 1, nil, limit)
    end

    @impl Rheo.Backend
    def query(handle, %Query{} = query) do
      Telemetry.span([:rheo, :query], %{stream: query.stream}, fn ->
        do_query(handle, Query.apply_cursor(query))
      end)
    end

    defp do_query(handle, %Query{} = query) do
      with {:ok, count} <- partition_count(handle, query.stream),
           {:ok, partitions} <- query_partitions(query, count),
           {:ok, events} <- scan_partitions(handle, query, partitions) do
        {:ok,
         events
         |> Enum.filter(&matches?(&1, query))
         |> Enum.sort(order_fun(query.order_by))
         |> Enum.take(query.limit)}
      end
    end

    defp scan_partitions(handle, %Query{} = query, partitions) do
      Enum.reduce_while(partitions, {:ok, []}, fn partition, {:ok, acc} ->
        min_sequence = (Query.after_sequence_for(query, partition) || 0) + 1

        case load_range(
               handle,
               query.stream,
               partition,
               min_sequence,
               query.until_sequence,
               nil
             ) do
          {:ok, events} -> {:cont, {:ok, acc ++ events}}
          {:error, _} = error -> {:halt, error}
        end
      end)
    end

    defp query_partitions(%Query{where: where}, count) do
      case Keyword.fetch(where, :partition) do
        {:ok, partition} -> Partition.normalize_assignment(partition, count)
        :error -> Partition.normalize_assignment(:all, count)
      end
    end

    # Range scan over one partition: bound the XRANGE with entry ids resolved
    # from the sequence index, then decode.
    defp load_range(handle, stream, partition, min_sequence, max_sequence, limit) do
      with {:ok, start_id} <- entry_id_at_or_after(handle, stream, partition, min_sequence),
           {:ok, end_id} <- range_end_id(handle, stream, partition, max_sequence) do
        cond do
          is_nil(start_id) -> {:ok, []}
          is_nil(end_id) -> {:ok, []}
          true -> xrange(handle, stream, partition, start_id, end_id, limit)
        end
      end
    end

    defp xrange(handle, stream, partition, start_id, end_id, limit) do
      cmd =
        ["XRANGE", Keys.events(handle, stream, partition), start_id, end_id] ++
          if(is_integer(limit), do: ["COUNT", to_str(limit)], else: [])

      with {:ok, entries} <- command(handle, cmd) do
        {:ok, Enum.map(entries, fn [_id, fields] -> Codec.event_from_fields(stream, fields) end)}
      end
    end

    defp range_end_id(_handle, _stream, _partition, nil), do: {:ok, "+"}

    defp range_end_id(handle, stream, partition, max_sequence) do
      case entry_id_at_or_before(handle, stream, partition, max_sequence) do
        {:ok, "0"} -> {:ok, nil}
        other -> other
      end
    end

    @impl Rheo.Backend
    def fetch(handle, stream, group, opts \\ []) do
      Telemetry.span([:rheo, :fetch], %{stream: stream, group: group}, fn ->
        do_fetch(handle, stream, group, opts)
      end)
    end

    defp do_fetch(handle, stream, group, opts) do
      with {:ok, count} <- partition_count(handle, stream),
           {:ok, _gmeta} <- group_meta(handle, stream, group),
           {:ok, partitions} <- resolve_assignment(opts, count) do
        ctx = fetch_context(handle, stream, group, opts)

        case claim_partitions(ctx, partitions) do
          {:ok, leases} = ok ->
            Telemetry.execute([:rheo, :lease], %{count: length(leases)}, %{
              stream: stream,
              group: group,
              consumer_id: ctx.consumer
            })

            ok

          other ->
            other
        end
      end
    end

    defp fetch_context(handle, stream, group, opts) do
      %{
        handle: handle,
        stream: stream,
        group: group,
        consumer: Keyword.get(opts, :consumer_id) || @default_consumer,
        limit: Keyword.get(opts, :limit, Application.get_env(:rheo, :default_max_demand, 10)),
        lease_ms:
          Keyword.get(opts, :lease_ms, Application.get_env(:rheo, :default_lease_ms, 30_000)),
        now: Clock.utc_now()
      }
    end

    defp claim_partitions(ctx, partitions) do
      Enum.reduce_while(partitions, {:ok, []}, fn partition, {:ok, acc} ->
        remaining = ctx.limit - length(acc)

        if remaining <= 0 do
          {:halt, {:ok, acc}}
        else
          case claim_partition(ctx, partition, remaining) do
            {:ok, leases} -> {:cont, {:ok, acc ++ leases}}
            {:error, _} = error -> {:halt, error}
          end
        end
      end)
    end

    defp claim_partition(ctx, partition, remaining) do
      with {:ok, reclaimed} <- reclaim_entries(ctx, partition, remaining),
           {:ok, fresh} <- read_new_entries(ctx, partition, remaining - length(reclaimed)) do
        grant_leases(ctx, partition, reclaimed ++ fresh)
      end
    end

    # Reclaim is fenced on Rheo's clock, not Redis idle time: an entry is
    # claimable when its fence is gone, released by retry, or expired.
    defp reclaim_entries(ctx, partition, remaining) do
      key = Keys.events(ctx.handle, ctx.stream, partition)
      scan = max(remaining, @pending_scan)

      with {:ok, pending} <-
             group_command(ctx.handle, ["XPENDING", key, ctx.group, "-", "+", to_str(scan)]),
           {:ok, candidates} <- expired_candidates(ctx, pending, remaining) do
        claim(ctx, partition, candidates)
      end
    end

    defp expired_candidates(_ctx, [], _remaining), do: {:ok, []}

    defp expired_candidates(ctx, pending, remaining) do
      ids = Enum.map(pending, fn [id | _rest] -> id end)

      cmds =
        Enum.map(ids, fn id ->
          ["HGETALL", Keys.fence(ctx.handle, ctx.stream, ctx.group, id)]
        end)

      with {:ok, results} <- pipeline(ctx.handle, cmds) do
        now_ms = DateTime.to_unix(ctx.now, :millisecond)

        candidates =
          pending
          |> Enum.zip(results)
          |> Enum.filter(fn {_entry, fields} -> expired?(Codec.to_map(fields), now_ms) end)
          |> Enum.map(fn {[id | rest], fields} ->
            {id, prior_attempt(Codec.to_map(fields), rest)}
          end)
          |> Enum.take(remaining)

        {:ok, candidates}
      end
    end

    defp expired?(fence, _now_ms) when map_size(fence) == 0, do: true

    defp expired?(fence, now_ms) do
      fence["lease_id"] in [nil, ""] or
        Codec.to_integer(fence["expires_at_ms"], 0) <= now_ms
    end

    defp prior_attempt(fence, pending_rest) do
      case fence["attempt"] do
        nil -> delivery_count(pending_rest)
        value -> Codec.to_integer(value, 0)
      end
    end

    defp delivery_count([_consumer, _idle, count]), do: Codec.to_integer(count, 0)
    defp delivery_count(_other), do: 0

    defp claim(_ctx, _partition, []), do: {:ok, []}

    defp claim(ctx, partition, candidates) do
      ids = Enum.map(candidates, &elem(&1, 0))
      attempts = Map.new(candidates)

      cmd =
        [
          "XCLAIM",
          Keys.events(ctx.handle, ctx.stream, partition),
          ctx.group,
          ctx.consumer,
          "0"
        ] ++ ids

      with {:ok, entries} <- command(ctx.handle, cmd) do
        {:ok,
         entries
         |> Enum.filter(&match?([_id, fields] when is_list(fields), &1))
         |> Enum.map(fn [id, fields] -> {id, fields, Map.get(attempts, id, 0)} end)}
      end
    end

    defp read_new_entries(_ctx, _partition, count) when count <= 0, do: {:ok, []}

    defp read_new_entries(ctx, partition, count) do
      cmd = [
        "XREADGROUP",
        "GROUP",
        ctx.group,
        ctx.consumer,
        "COUNT",
        to_str(count),
        "STREAMS",
        Keys.events(ctx.handle, ctx.stream, partition),
        ">"
      ]

      case group_command(ctx.handle, cmd) do
        {:ok, nil} -> {:ok, []}
        {:ok, [[_key, entries]]} -> {:ok, Enum.map(entries, fn [id, f] -> {id, f, 0} end)}
        {:ok, _other} -> {:ok, []}
        {:error, reason} -> {:error, reason}
      end
    end

    defp grant_leases(_ctx, _partition, []), do: {:ok, []}

    defp grant_leases(ctx, partition, entries) do
      expires_at = DateTime.add(ctx.now, ctx.lease_ms, :millisecond)
      leases = Enum.map(entries, &to_lease(ctx, &1, expires_at))
      cmds = Enum.map(leases, &fence_command(ctx, &1, expires_at))

      with {:ok, _} <- pipeline(ctx.handle, cmds ++ [cursor_command(ctx, partition, leases)]) do
        Enum.each(leases, &maybe_redelivery(ctx, &1))
        {:ok, Enum.sort_by(leases, & &1.event.sequence)}
      end
    end

    defp to_lease(ctx, {entry_id, fields, prior}, expires_at) do
      event = Codec.event_from_fields(ctx.stream, fields)

      %Lease{
        lease_id: Id.generate(),
        stream: ctx.stream,
        group: ctx.group,
        event_id: event.id,
        event: event,
        consumer_id: ctx.consumer,
        attempt: prior + 1,
        leased_at: ctx.now,
        expires_at: expires_at,
        receipt: entry_id
      }
    end

    defp fence_command(ctx, %Lease{} = lease, expires_at) do
      [
        "HSET",
        Keys.fence(ctx.handle, ctx.stream, ctx.group, lease.receipt),
        "lease_id",
        lease.lease_id,
        "attempt",
        to_str(lease.attempt),
        "expires_at_ms",
        to_str(DateTime.to_unix(expires_at, :millisecond)),
        "event_id",
        lease.event_id,
        "partition",
        to_str(lease.event.partition),
        "sequence",
        to_str(lease.event.sequence),
        "consumer_id",
        ctx.consumer
      ]
    end

    defp cursor_command(ctx, partition, leases) do
      highest = leases |> Enum.map(& &1.event.sequence) |> Enum.max()

      [
        "HSET",
        Keys.group_meta(ctx.handle, ctx.stream, ctx.group),
        Keys.cursor_field(partition),
        to_str(highest)
      ]
    end

    defp maybe_redelivery(_ctx, %Lease{attempt: attempt}) when attempt <= 1, do: :ok

    defp maybe_redelivery(ctx, %Lease{} = lease) do
      Telemetry.execute([:rheo, :redelivery], %{count: 1}, %{
        stream: ctx.stream,
        group: ctx.group,
        event_id: lease.event_id
      })
    end

    @impl Rheo.Backend
    def renew(handle, %Lease{} = lease, opts \\ []) do
      Telemetry.span([:rheo, :lease, :renew], %{stream: lease.stream, group: lease.group}, fn ->
        lease_ms =
          Keyword.get(opts, :lease_ms, Application.get_env(:rheo, :default_lease_ms, 30_000))

        expires_at = DateTime.add(Clock.utc_now(), lease_ms, :millisecond)

        with {:ok, entry_id, _fence} <- authorize(handle, lease),
             {:ok, _} <-
               command(handle, [
                 "HSET",
                 Keys.fence(handle, lease.stream, lease.group, entry_id),
                 "expires_at_ms",
                 to_str(DateTime.to_unix(expires_at, :millisecond))
               ]) do
          {:ok, %{lease | expires_at: expires_at}}
        end
      end)
    end

    @impl Rheo.Backend
    def ack(handle, %Lease{} = lease) do
      Telemetry.span([:rheo, :ack], %{stream: lease.stream, group: lease.group}, fn ->
        with {:ok, entry_id, _fence} <- authorize(handle, lease) do
          settle(handle, lease, entry_id, [])
        end
      end)
    end

    @impl Rheo.Backend
    def retry(handle, %Lease{} = lease, reason) do
      Telemetry.span([:rheo, :retry], %{stream: lease.stream, group: lease.group}, fn ->
        with {:ok, gmeta} <- group_meta(handle, lease.stream, lease.group),
             {:ok, entry_id, fence} <- authorize(handle, lease) do
          max_attempts =
            Codec.to_integer(
              gmeta["max_attempts"],
              Application.get_env(:rheo, :default_max_attempts, 5)
            )

          attempt = Codec.to_integer(fence["attempt"], lease.attempt)

          if attempt >= max_attempts do
            dead_letter(handle, lease, entry_id, {:max_attempts, reason})
          else
            release(handle, lease, entry_id, reason)
          end
        end
      end)
    end

    @impl Rheo.Backend
    def reject(handle, %Lease{} = lease, reason) do
      Telemetry.span([:rheo, :reject], %{stream: lease.stream, group: lease.group}, fn ->
        with {:ok, entry_id, _fence} <- authorize(handle, lease) do
          dead_letter(handle, lease, entry_id, reason)
        end
      end)
    end

    # Retry keeps the entry in the pending list and drops the fencing token, so
    # the next fetch reclaims it with an incremented attempt.
    defp release(handle, %Lease{} = lease, entry_id, reason) do
      cmd = [
        "HSET",
        Keys.fence(handle, lease.stream, lease.group, entry_id),
        "lease_id",
        "",
        "expires_at_ms",
        "0",
        "reason",
        Codec.encode_reason(reason)
      ]

      with {:ok, _} <- command(handle, cmd), do: :ok
    end

    defp dead_letter(handle, %Lease{} = lease, entry_id, reason) do
      dlq = [
        ["XADD", Keys.dlq(handle, lease.stream, lease.group), "*"] ++
          Codec.to_fields(lease.event) ++
          ["reason", Codec.encode_reason(reason), "dead_lettered_at", iso(Clock.utc_now())]
      ]

      with :ok <- settle(handle, lease, entry_id, dlq) do
        Telemetry.execute([:rheo, :dead_letter], %{count: 1}, %{
          stream: lease.stream,
          group: lease.group,
          event_id: lease.event_id
        })

        :ok
      end
    end

    # Settling removes the entry from the pending list, drops the fence, and
    # records the sequence so the contiguous frontier can advance.
    defp settle(handle, %Lease{} = lease, entry_id, extra_commands) do
      partition = lease.event.partition
      sequence = lease.event.sequence

      cmds =
        [
          ["XACK", Keys.events(handle, lease.stream, partition), lease.group, entry_id],
          ["DEL", Keys.fence(handle, lease.stream, lease.group, entry_id)],
          [
            "ZADD",
            Keys.settled(handle, lease.stream, lease.group, partition),
            to_str(sequence),
            to_str(sequence)
          ]
        ] ++ extra_commands

      with {:ok, _} <- pipeline(handle, cmds) do
        _ = advance_frontier(handle, lease.stream, lease.group, partition)
        :ok
      end
    end

    defp advance_frontier(handle, stream, group, partition) do
      gmeta_key = Keys.group_meta(handle, stream, group)
      settled_key = Keys.settled(handle, stream, group, partition)

      with {:ok, current} <- command(handle, ["HGET", gmeta_key, Keys.frontier_field(partition)]),
           frontier = Codec.to_integer(current, 0),
           {:ok, members} <-
             command(handle, ["ZRANGEBYSCORE", settled_key, "(#{frontier}", "+inf"]) do
        collapse_frontier(handle, stream, group, partition, frontier, members)
      end
    end

    defp collapse_frontier(handle, stream, group, partition, frontier, members) do
      next =
        Enum.reduce_while(members, frontier, fn member, acc ->
          sequence = Codec.to_integer(member, 0)
          if sequence == acc + 1, do: {:cont, sequence}, else: {:halt, acc}
        end)

      if next > frontier do
        cmds = [
          [
            "HSET",
            Keys.group_meta(handle, stream, group),
            Keys.frontier_field(partition),
            to_str(next)
          ],
          [
            "ZREMRANGEBYSCORE",
            Keys.settled(handle, stream, group, partition),
            "-inf",
            to_str(next)
          ]
        ]

        with {:ok, _} <- pipeline(handle, cmds) do
          Telemetry.execute([:rheo, :group, :frontier], %{frontier: next}, %{
            stream: stream,
            group: group,
            partition: partition,
            frontier: next
          })

          :ok
        end
      else
        :ok
      end
    end

    # Fencing: the entry id is derived from the portable sequence, so a settle
    # call proves both that it holds the current `lease_id` and that its
    # receipt still names the same entry (ADR 021).
    defp authorize(handle, %Lease{} = lease) do
      partition = lease.event.partition

      with {:ok, entry_id} <-
             entry_id_of(handle, lease.stream, partition, lease.event.sequence),
           {:ok, fields} <-
             command(handle, ["HGETALL", Keys.fence(handle, lease.stream, lease.group, entry_id)]) do
        check_fence(Codec.to_map(fields), lease, entry_id)
      end
    end

    defp check_fence(fence, %Lease{} = lease, entry_id) do
      cond do
        map_size(fence) == 0 -> {:error, :stale_lease}
        fence["lease_id"] != lease.lease_id -> {:error, :stale_lease}
        not receipt_match?(lease.receipt, entry_id) -> {:error, :receipt_mismatch}
        true -> {:ok, entry_id, fence}
      end
    end

    defp receipt_match?(nil, _entry_id), do: true
    defp receipt_match?(receipt, entry_id), do: to_string(receipt) == entry_id

    defp entry_id_of(handle, stream, partition, sequence) do
      cmd = [
        "ZRANGEBYSCORE",
        Keys.index(handle, stream, partition),
        to_str(sequence),
        to_str(sequence),
        "LIMIT",
        "0",
        "1"
      ]

      case command(handle, cmd) do
        {:ok, [entry_id]} -> {:ok, entry_id}
        {:ok, _} -> {:error, :stale_lease}
        {:error, reason} -> {:error, reason}
      end
    end

    @impl Rheo.Backend
    def replay(handle, stream, group, opts \\ []) do
      with {:ok, _gmeta} <- group_meta(handle, stream, group),
           {:ok, count} <- partition_count(handle, stream),
           {:ok, partitions} <- resolve_assignment(opts, count),
           {:ok, from} <- replay_from(handle, stream, partitions, opts) do
        with :ok <- rewind_partitions(handle, stream, group, from, clear_all: false) do
          Telemetry.execute([:rheo, :group, :replay], %{count: map_size(from)}, %{
            stream: stream,
            group: group
          })

          :ok
        end
      end
    end

    defp replay_from(handle, stream, partitions, opts) do
      cond do
        events = Keyword.get(opts, :events) ->
          {:ok, earliest_from_events(events, partitions)}

        ids = Keyword.get(opts, :event_ids) ->
          earliest_from_event_ids(handle, stream, partitions, ids)

        Keyword.has_key?(opts, :from_sequence) ->
          from = Keyword.fetch!(opts, :from_sequence)
          {:ok, Map.new(partitions, &{&1, max(from, 0)})}

        true ->
          {:error, :invalid_replay_opts}
      end
    end

    defp earliest_from_events(events, partitions) do
      events
      |> Enum.filter(&(&1.partition in partitions))
      |> Enum.group_by(& &1.partition, & &1.sequence)
      |> Map.new(fn {partition, sequences} ->
        {partition, max(Enum.min(sequences) - 1, 0)}
      end)
    end

    defp earliest_from_event_ids(handle, stream, partitions, ids) do
      wanted = MapSet.new(ids)

      Enum.reduce_while(partitions, {:ok, %{}}, fn partition, {:ok, acc} ->
        case load_range(handle, stream, partition, 1, nil, nil) do
          {:ok, events} ->
            matched = Enum.filter(events, &MapSet.member?(wanted, &1.id))

            acc =
              case matched do
                [] ->
                  acc

                found ->
                  Map.put(acc, partition, max(Enum.min_by(found, & &1.sequence).sequence - 1, 0))
              end

            {:cont, {:ok, acc}}

          {:error, _} = error ->
            {:halt, error}
        end
      end)
    end

    @impl Rheo.Backend
    def reset_group(handle, stream, group, opts \\ []) do
      with {:ok, _gmeta} <- group_meta(handle, stream, group),
           {:ok, count} <- partition_count(handle, stream),
           {:ok, starts} <- group_start_sequences(handle, stream, count, opts),
           :ok <- rewind_partitions(handle, stream, group, starts, clear_all: true),
           :ok <- delete_fences(handle, stream, group) do
        Telemetry.execute([:rheo, :group, :reset], %{count: map_size(starts)}, %{
          stream: stream,
          group: group
        })

        :ok
      end
    end

    defp rewind_partitions(handle, stream, group, from, opts) do
      Enum.reduce_while(from, :ok, fn {partition, sequence}, :ok ->
        case rewind_partition(handle, stream, group, partition, sequence, opts) do
          :ok -> {:cont, :ok}
          {:error, _} = error -> {:halt, error}
        end
      end)
    end

    defp rewind_partition(handle, stream, group, partition, sequence, opts) do
      clear_all? = Keyword.get(opts, :clear_all, false)

      with {:ok, pending} <- pending_sequences(handle, stream, group, partition),
           :ok <- drop_pending(handle, stream, group, partition, pending, sequence, clear_all?),
           {:ok, entry_id} <- entry_id_at_or_before(handle, stream, partition, sequence),
           {:ok, _} <-
             group_command(handle, [
               "XGROUP",
               "SETID",
               Keys.events(handle, stream, partition),
               group,
               entry_id
             ]),
           {:ok, _} <-
             pipeline(
               handle,
               rewind_commands(handle, stream, group, partition, sequence, clear_all?)
             ) do
        :ok
      end
    end

    defp rewind_commands(handle, stream, group, partition, sequence, clear_all?) do
      settled_key = Keys.settled(handle, stream, group, partition)

      clear =
        if clear_all? do
          [["DEL", settled_key]]
        else
          [["ZREMRANGEBYSCORE", settled_key, "(#{sequence}", "+inf"]]
        end

      clear ++
        [
          [
            "HSET",
            Keys.group_meta(handle, stream, group),
            Keys.frontier_field(partition),
            to_str(sequence),
            Keys.cursor_field(partition),
            to_str(sequence)
          ]
        ]
    end

    # Entries above the replay point are acknowledged out of the pending list so
    # SETID redelivers them exactly once; entries below it keep their claim.
    defp drop_pending(handle, stream, group, partition, pending, sequence, clear_all?) do
      selected =
        Enum.filter(pending, fn {_id, entry_sequence} ->
          clear_all? or is_nil(entry_sequence) or entry_sequence > sequence
        end)

      if selected == [] do
        :ok
      else
        ids = Enum.map(selected, &elem(&1, 0))

        cmds =
          [["XACK", Keys.events(handle, stream, partition), group] ++ ids] ++
            Enum.map(ids, &["DEL", Keys.fence(handle, stream, group, &1)])

        with {:ok, _} <- pipeline(handle, cmds), do: :ok
      end
    end

    # A reset must leave no claim behind, including fences whose entries were
    # trimmed out of the stream.
    defp delete_fences(handle, stream, group) do
      scan_delete(handle, Keys.fence_pattern(handle, stream, group), "0")
    end

    defp scan_delete(handle, pattern, cursor) do
      with {:ok, [next, keys]} <-
             command(handle, ["SCAN", cursor, "MATCH", pattern, "COUNT", "500"]),
           {:ok, _} <- pipeline(handle, if(keys == [], do: [], else: [["DEL" | keys]])) do
        if next == "0", do: :ok, else: scan_delete(handle, pattern, next)
      end
    end

    defp pending_sequences(handle, stream, group, partition) do
      key = Keys.events(handle, stream, partition)

      with {:ok, pending} <-
             group_command(handle, ["XPENDING", key, group, "-", "+", to_str(@pending_page)]) do
        ids = Enum.map(pending, fn [id | _rest] -> id end)
        entry_sequences(handle, key, ids)
      end
    end

    defp entry_sequences(_handle, _key, []), do: {:ok, []}

    defp entry_sequences(handle, key, ids) do
      cmds = Enum.map(ids, &["XRANGE", key, &1, &1])

      with {:ok, results} <- pipeline(handle, cmds) do
        {:ok,
         ids
         |> Enum.zip(results)
         |> Enum.map(fn {id, entries} -> {id, sequence_of(entries)} end)}
      end
    end

    defp sequence_of([[_id, fields] | _rest]) do
      case fields |> Codec.to_map() |> Map.get("sequence") do
        nil -> nil
        value -> Codec.to_integer(value, 0)
      end
    end

    defp sequence_of(_other), do: nil

    @impl Rheo.Backend
    def lag(handle, stream, group, opts \\ []) do
      with {:ok, gmeta} <- group_meta(handle, stream, group),
           {:ok, count} <- partition_count(handle, stream),
           {:ok, partitions} <- resolve_assignment(opts, count),
           {:ok, high_watermarks} <- high_watermarks(handle, stream, partitions) do
        frontiers =
          Map.new(partitions, fn partition ->
            {Partition.key(partition), Codec.to_integer(gmeta[Keys.frontier_field(partition)], 0)}
          end)

        {:ok, Lag.from_maps(stream, group, frontiers, high_watermarks, partitions)}
      end
    end

    @impl Rheo.Backend
    def list_streams(handle, _opts \\ []) do
      with {:ok, keys} <- scan_keys(handle, Keys.meta_pattern(handle), "0", []) do
        prefix = Keys.prefix(handle) <> "meta:"

        names =
          keys
          |> Enum.map(fn key -> String.replace_prefix(key, prefix, "") end)
          |> Enum.reject(&(&1 == ""))
          |> Enum.sort()

        {:ok, names}
      end
    end

    @impl Rheo.Backend
    def list_groups(handle, stream, _opts \\ []) do
      with {:ok, _} <- partition_count(handle, stream),
           {:ok, keys} <- scan_keys(handle, Keys.group_meta_pattern(handle, stream), "0", []) do
        prefix = Keys.prefix(handle) <> "gmeta:" <> stream <> ":"

        names =
          keys
          |> Enum.map(fn key -> String.replace_prefix(key, prefix, "") end)
          |> Enum.reject(&(&1 == ""))
          |> Enum.sort()

        {:ok, names}
      end
    end

    @impl Rheo.Backend
    def dead_letters(handle, stream, group, opts \\ []) do
      with {:ok, _} <- group_meta(handle, stream, group) do
        limit = Keyword.get(opts, :limit, 100)
        after_id = Keyword.get(opts, :after)
        key = Keys.dlq(handle, stream, group)

        case command(handle, ["XRANGE", key, "-", "+", "COUNT", to_str(max(limit * 4, 100))]) do
          {:ok, entries} when is_list(entries) ->
            rows =
              Enum.map(entries, fn [_id, fields] -> redis_dead_letter(stream, group, fields) end)

            case drop_after_dead(rows, after_id) do
              {:ok, rows} -> {:ok, Enum.take(rows, limit)}
              error -> error
            end

          {:ok, nil} ->
            {:ok, []}

          {:error, reason} ->
            {:error, reason}
        end
      end
    end

    @impl Rheo.Backend
    def group_info(handle, stream, group, opts \\ []) do
      with {:ok, lag} <- lag(handle, stream, group, opts),
           {:ok, count} <- partition_count(handle, stream),
           {:ok, partitions} <- resolve_assignment(opts, count),
           {:ok, inflight} <- pending_count(handle, stream, group, partitions),
           {:ok, dead} <- dlq_count(handle, stream, group) do
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

    defp pending_count(handle, stream, group, partitions) do
      Enum.reduce_while(partitions, {:ok, 0}, fn partition, {:ok, acc} ->
        key = Keys.events(handle, stream, partition)

        case group_command(handle, ["XPENDING", key, group]) do
          {:ok, [count | _]} -> {:cont, {:ok, acc + Codec.to_integer(count, 0)}}
          {:ok, _} -> {:cont, {:ok, acc}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
    end

    defp dlq_count(handle, stream, group) do
      case command(handle, ["XLEN", Keys.dlq(handle, stream, group)]) do
        {:ok, n} -> {:ok, Codec.to_integer(n, 0)}
        {:error, reason} -> {:error, reason}
      end
    end

    defp redis_dead_letter(stream, group, fields) do
      event = Codec.event_from_fields(stream, fields)
      map = Codec.to_map(fields)

      %DeadLetter{
        stream: stream,
        group: group,
        event_id: event.id,
        partition: event.partition,
        sequence: event.sequence,
        reason: map["reason"],
        dead_lettered_at: parse_iso(map["dead_lettered_at"]),
        event: event
      }
    end

    defp drop_after_dead(rows, nil), do: {:ok, rows}

    defp drop_after_dead(rows, after_id) when is_binary(after_id) do
      case Enum.find_index(rows, &(&1.event_id == after_id)) do
        nil -> {:error, :cursor_not_found}
        idx -> {:ok, Enum.drop(rows, idx + 1)}
      end
    end

    defp scan_keys(handle, pattern, cursor, acc) do
      with {:ok, [next, keys]} <-
             command(handle, ["SCAN", cursor, "MATCH", pattern, "COUNT", "500"]) do
        acc = acc ++ List.wrap(keys)
        if next == "0", do: {:ok, acc}, else: scan_keys(handle, pattern, next, acc)
      end
    end

    defp parse_iso(nil), do: nil

    defp parse_iso(value) when is_binary(value) do
      case DateTime.from_iso8601(value) do
        {:ok, dt, _} -> dt
        _ -> nil
      end
    end

    defp high_watermarks(handle, stream, partitions) do
      cmds = Enum.map(partitions, &["GET", Keys.sequence(handle, stream, &1)])

      with {:ok, results} <- pipeline(handle, cmds) do
        {:ok,
         partitions
         |> Enum.zip(results)
         |> Map.new(fn {partition, value} ->
           {Partition.key(partition), Codec.to_integer(value, 0)}
         end)}
      end
    end

    @doc """
    Blocks until new entries may be available for the group (ADR 025).

    Runs `XREAD BLOCK` from each assigned partition's group cursor on a
    dedicated Redix connection, so a blocking wait never stalls commands on the
    handle. Returns `:ok` on both a wakeup and a timeout: the caller must still
    fetch. Without `:stream` / `:group` hints there is nothing to watch, so the
    call sleeps for `:timeout` and returns `:ok`, leaving polling in charge.

    ## Arguments

      * `handle` — backend handle
      * `opts` — `:timeout` (ms, default `1000`), `:stream`, `:group`,
        `:partitions`

    ## Returns

    `:ok`, or `{:error, :backend_unavailable}` / `{:error, {:failed, cause}}`.
    """
    @impl Rheo.Backend.Wakeup
    @spec wait(Rheo.Backend.handle(), keyword()) :: :ok | {:error, term()}
    def wait(handle, opts \\ []) do
      timeout = Keyword.get(opts, :timeout, @default_wait_ms)
      stream = Keyword.get(opts, :stream)
      group = Keyword.get(opts, :group)

      if is_binary(stream) and is_binary(group) do
        block_on_streams(handle, stream, group, timeout, opts)
      else
        idle(timeout)
      end
    end

    defp block_on_streams(handle, stream, group, timeout, opts) do
      with {:ok, count} <- partition_count(handle, stream),
           {:ok, partitions} <- resolve_assignment(opts, count),
           {:ok, cursors} <- group_cursors(handle, stream, group, partitions) do
        case waiter(handle) do
          nil -> idle(timeout)
          waiter -> xread_block(waiter, handle, stream, timeout, cursors)
        end
      end
    end

    defp group_cursors(handle, stream, group, partitions) do
      cmds = Enum.map(partitions, &["XINFO", "GROUPS", Keys.events(handle, stream, &1)])

      with {:ok, results} <- pipeline(handle, cmds) do
        {:ok,
         partitions
         |> Enum.zip(results)
         |> Enum.map(fn {partition, groups} -> {partition, last_delivered(groups, group)} end)
         |> Enum.reject(&is_nil(elem(&1, 1)))}
      end
    end

    defp last_delivered(groups, group) when is_list(groups) do
      Enum.find_value(groups, fn info ->
        fields = Codec.to_map(info)
        if fields["name"] == group, do: fields["last-delivered-id"]
      end)
    end

    defp last_delivered(_groups, _group), do: nil

    defp xread_block(_waiter, _handle, _stream, timeout, []), do: idle(timeout)

    defp xread_block(waiter, handle, stream, timeout, cursors) do
      {partitions, ids} = Enum.unzip(cursors)
      keys = Enum.map(partitions, &Keys.events(handle, stream, &1))

      cmd = ["XREAD", "COUNT", "1", "BLOCK", to_str(timeout), "STREAMS"] ++ keys ++ ids

      case client().command(waiter, cmd, timeout: timeout + @command_timeout) do
        {:ok, _result} -> :ok
        {:error, reason} -> {:error, map_error(reason)}
      end
    catch
      :exit, _reason -> {:error, :backend_unavailable}
    end

    defp idle(timeout) when is_integer(timeout) and timeout > 0 do
      Process.sleep(timeout)
      :ok
    end

    defp idle(_timeout), do: :ok

    defp waiter(handle) when is_atom(handle) do
      name = waiter_name(handle)
      if is_pid(Process.whereis(name)), do: name
    end

    defp waiter(_handle), do: nil

    defp waiter_name(name) when is_atom(name), do: Module.concat(name, Waiter)

    defp waiter_children(name, conn_opts) when is_atom(name) do
      waiter = waiter_name(name)

      [
        Supervisor.child_spec({Redix, Keyword.put(conn_opts, :name, waiter)},
          id: {Redix, waiter}
        )
      ]
    end

    defp waiter_children(_name, _conn_opts), do: []

    defp supervisor_name(name) when is_atom(name), do: [name: Module.concat(name, Supervisor)]
    defp supervisor_name(_name), do: []

    defp connection_opts(opts) do
      {url, rest} = opts |> Keyword.drop([:name, :pool_size]) |> Keyword.pop(:url)

      case url do
        nil ->
          rest |> Keyword.put_new(:host, "localhost") |> Keyword.put_new(:port, 6379)

        url when is_binary(url) ->
          Keyword.merge(parse_url(url), rest)
      end
    end

    defp parse_url(url) do
      uri = URI.parse(url)

      [host: uri.host || "localhost", port: uri.port || 6379]
      |> put_unless_nil(:database, database_of(uri.path))
      |> put_unless_nil(:password, password_of(uri.userinfo))
      |> put_unless_nil(:ssl, if(uri.scheme == "rediss", do: true))
    end

    defp database_of(nil), do: nil
    defp database_of("/"), do: nil

    defp database_of("/" <> rest) do
      case Integer.parse(rest) do
        {database, _} -> database
        :error -> nil
      end
    end

    defp database_of(_path), do: nil

    defp password_of(nil), do: nil

    defp password_of(userinfo) do
      case String.split(userinfo, ":", parts: 2) do
        [_user, password] when password != "" -> password
        [password] when password != "" -> password
        _ -> nil
      end
    end

    defp put_unless_nil(opts, _key, nil), do: opts
    defp put_unless_nil(opts, key, value), do: Keyword.put(opts, key, value)

    defp partition_count(handle, stream) do
      case command(handle, ["HGET", Keys.meta(handle, stream), "partition_count"]) do
        {:ok, nil} -> {:error, :stream_not_found}
        {:ok, value} -> {:ok, Codec.to_integer(value, 1)}
        {:error, reason} -> {:error, reason}
      end
    end

    defp group_meta(handle, stream, group) do
      case command(handle, ["HGETALL", Keys.group_meta(handle, stream, group)]) do
        {:ok, []} -> {:error, :group_not_found}
        {:ok, fields} -> {:ok, Codec.to_map(fields)}
        {:error, reason} -> {:error, reason}
      end
    end

    defp group_start_sequences(handle, stream, count, opts) do
      with {:ok, selected} <- resolve_assignment(opts, count) do
        base = Map.new(0..(count - 1), &{&1, 0})

        cond do
          Keyword.has_key?(opts, :start_after) ->
            {:ok, apply_start_after(base, selected, Keyword.fetch!(opts, :start_after))}

          Keyword.has_key?(opts, :start_at) ->
            apply_start_at(handle, stream, base, selected, Keyword.fetch!(opts, :start_at))

          true ->
            {:ok, base}
        end
      end
    end

    defp apply_start_after(base, selected, start_after) do
      Enum.reduce(selected, base, fn partition, acc ->
        case start_after_for(start_after, partition) do
          sequence when is_integer(sequence) -> Map.put(acc, partition, max(sequence, 0))
          _ -> acc
        end
      end)
    end

    defp start_after_for(sequence, _partition) when is_integer(sequence), do: sequence

    defp start_after_for(sequences, partition) when is_map(sequences) do
      Map.get(sequences, partition) || Map.get(sequences, Partition.key(partition))
    end

    defp start_after_for(_other, _partition), do: nil

    defp apply_start_at(handle, stream, base, selected, %DateTime{} = datetime) do
      Enum.reduce_while(selected, {:ok, base}, fn partition, {:ok, acc} ->
        case load_range(handle, stream, partition, 1, nil, nil) do
          {:ok, events} ->
            {:cont, {:ok, Map.put(acc, partition, start_at_sequence(events, datetime))}}

          {:error, _} = error ->
            {:halt, error}
        end
      end)
    end

    defp apply_start_at(_handle, _stream, _base, _selected, _other),
      do: {:error, :invalid_start_at}

    defp start_at_sequence(events, datetime) do
      events
      |> Enum.find(fn event ->
        match?(%DateTime{}, event.timestamp) and
          DateTime.compare(event.timestamp, datetime) != :lt
      end)
      |> case do
        nil -> 0
        event -> max(event.sequence - 1, 0)
      end
    end

    defp entry_id_at_or_before(_handle, _stream, _partition, sequence) when sequence <= 0 do
      {:ok, "0"}
    end

    defp entry_id_at_or_before(handle, stream, partition, sequence) do
      cmd = [
        "ZREVRANGEBYSCORE",
        Keys.index(handle, stream, partition),
        to_str(sequence),
        "-inf",
        "LIMIT",
        "0",
        "1"
      ]

      case command(handle, cmd) do
        {:ok, [entry_id]} -> {:ok, entry_id}
        {:ok, _} -> {:ok, "0"}
        {:error, reason} -> {:error, reason}
      end
    end

    defp entry_id_at_or_after(handle, stream, partition, sequence) do
      cmd = [
        "ZRANGEBYSCORE",
        Keys.index(handle, stream, partition),
        to_str(max(sequence, 0)),
        "+inf",
        "LIMIT",
        "0",
        "1"
      ]

      case command(handle, cmd) do
        {:ok, [entry_id]} -> {:ok, entry_id}
        {:ok, _} -> {:ok, nil}
        {:error, reason} -> {:error, reason}
      end
    end

    defp resolve_assignment(opts, count) do
      assignment =
        cond do
          Keyword.has_key?(opts, :partition) -> Keyword.fetch!(opts, :partition)
          Keyword.has_key?(opts, :partitions) -> Keyword.fetch!(opts, :partitions)
          true -> :all
        end

      Partition.normalize_assignment(assignment, count)
    end

    defp resolve_payload_partitions(payloads, opts, count) do
      payloads
      |> Enum.reduce_while({:ok, []}, fn payload, {:ok, acc} ->
        case Partition.resolve(payload, opts, count) do
          {:ok, partition} -> {:cont, {:ok, [{payload, partition} | acc]}}
          {:error, :invalid_partition} = error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, routed} -> {:ok, Enum.reverse(routed)}
        error -> error
      end
    end

    @metadata_fields [:correlation_id, :causation_id, :producer, :schema, :schema_version]

    defp matches?(%Event{} = event, %Query{} = query) do
      Enum.all?(query.where, &where_match?(event, &1)) and
        in_time_range?(event, query) and
        Query.past_after?(event, query) and
        (is_nil(query.until_sequence) or event.sequence <= query.until_sequence)
    end

    defp in_time_range?(%Event{timestamp: %DateTime{} = timestamp}, %Query{} = query) do
      (is_nil(query.from) or DateTime.compare(timestamp, query.from) != :lt) and
        (is_nil(query.to) or DateTime.compare(timestamp, query.to) != :gt)
    end

    defp in_time_range?(_event, %Query{from: nil, to: nil}), do: true
    defp in_time_range?(_event, _query), do: false

    defp where_match?(event, {:type, type}), do: event.type == type
    defp where_match?(event, {:key, key}), do: event.key == key
    defp where_match?(event, {:partition, partition}), do: event.partition == partition

    defp where_match?(event, {field, value}) when field in @metadata_fields,
      do: Map.get(event.metadata, Atom.to_string(field)) == value

    defp where_match?(event, {field, value}) when is_atom(field),
      do: Map.get(event.payload, Atom.to_string(field)) == value

    defp where_match?(_event, _filter), do: false

    defp order_fun(order_by) do
      fn a, b ->
        Enum.reduce_while(order_by, true, fn {field, dir}, _acc ->
          va = Map.get(a, field)
          vb = Map.get(b, field)

          cond do
            va == vb -> {:cont, true}
            dir == :desc -> {:halt, compare(va, vb) == :gt}
            true -> {:halt, compare(va, vb) == :lt}
          end
        end)
      end
    end

    defp compare(%DateTime{} = a, %DateTime{} = b), do: DateTime.compare(a, b)
    defp compare(a, b) when a < b, do: :lt
    defp compare(a, b) when a > b, do: :gt
    defp compare(_a, _b), do: :eq

    defp client, do: Rheo.Backend.Redis.Client.current()

    defp command(handle, cmd) do
      case client().command(handle, cmd, timeout: @command_timeout) do
        {:ok, result} -> {:ok, result}
        {:error, reason} -> {:error, map_error(reason)}
      end
    catch
      :exit, {:timeout, _} -> {:error, {:ambiguous, :timeout}}
      :exit, _reason -> {:error, :backend_unavailable}
    end

    # A missing Redis consumer group is a Rheo semantic error, not a driver one.
    defp group_command(handle, cmd) do
      case command(handle, cmd) do
        {:error, {:failed, "NOGROUP" <> _rest}} -> {:error, :group_not_found}
        other -> other
      end
    end

    # Some commands fail benignly (`BUSYGROUP` when a Redis group already
    # exists); those errors are part of the idempotent path.
    defp tolerant_command(handle, cmd, prefix) do
      case client().command(handle, cmd, timeout: @command_timeout) do
        {:ok, result} ->
          {:ok, result}

        {:error, %Redix.Error{message: message} = error} ->
          if String.starts_with?(message, prefix),
            do: {:ok, :tolerated},
            else: {:error, map_error(error)}

        {:error, reason} ->
          {:error, map_error(reason)}
      end
    catch
      :exit, {:timeout, _} -> {:error, {:ambiguous, :timeout}}
      :exit, _reason -> {:error, :backend_unavailable}
    end

    defp pipeline(_handle, []), do: {:ok, []}

    defp pipeline(handle, cmds) do
      case client().pipeline(handle, cmds, timeout: @command_timeout) do
        {:ok, results} -> first_error(results)
        {:error, reason} -> {:error, map_error(reason)}
      end
    catch
      :exit, {:timeout, _} -> {:error, {:ambiguous, :timeout}}
      :exit, _reason -> {:error, :backend_unavailable}
    end

    defp first_error(results) do
      case Enum.find(results, &match?(%Redix.Error{}, &1)) do
        nil -> {:ok, results}
        error -> {:error, map_error(error)}
      end
    end

    defp map_error(%Redix.ConnectionError{}), do: :backend_unavailable
    defp map_error(%Redix.Error{message: message}), do: {:failed, message}
    defp map_error(:closed), do: :backend_unavailable
    defp map_error(:timeout), do: {:ambiguous, :timeout}
    defp map_error(reason) when is_atom(reason), do: reason
    defp map_error(reason), do: {:failed, reason}

    defp stringify_keys(map) when is_map(map) do
      Map.new(map, fn
        {key, value} when is_atom(key) -> {Atom.to_string(key), value}
        {key, value} -> {key, value}
      end)
    end

    defp stringify_keys(_other), do: %{}

    defp to_str(value) when is_integer(value), do: Integer.to_string(value)
    defp to_str(value) when is_binary(value), do: value
    defp to_str(value), do: to_string(value)

    defp iso(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
  end
end
