defmodule Rheo.Backend.Ecto do
  @moduledoc """
  SQL implementation of `Rheo.Backend` on top of a host-owned `Ecto.Repo`.

  Supports PostgreSQL (`Ecto.Adapters.Postgres`) and SQLite
  (`Ecto.Adapters.SQLite3`). Prefer the `Rheo` facade for application code.

  ## Supervision example

  Your app supervises the repo; Rheo only borrows it.

      children = [
        MyApp.Repo,
        {Rheo, name: MyRheo, backend: {Rheo.Backend.Ecto, repo: MyApp.Repo}}
      ]

  The opaque handle is the registered name of `Rheo.Backend.Ecto.Server`, which
  holds the resolved repo, dialect, and options.

  ## Tables

  See `Rheo.Backend.Ecto.Migrations`. `Rheo.ensure_indexes/1` creates them
  idempotently; hosts that prefer explicit migrations can run
  `mix rheo.ecto.gen_migration` instead.

  ## Options

    * `:repo` — required `Ecto.Repo` module
    * `:name` — handle / process name (default `default_handle/0`)
    * `:prefix` — PostgreSQL schema holding the Rheo tables (default `nil`)
    * `:notify` — when `true`, `NOTIFY rheo_events` after every append
      (PostgreSQL only, default `false`)

  ## Dialect differences

  PostgreSQL claims work use `FOR UPDATE SKIP LOCKED`, so many nodes can fetch
  concurrently (`distributed: true`). SQLite has a single writer and claims
  inside a plain transaction, so it is declared `distributed: false` — correct
  for one node, not for a shared network filesystem.

  Callback semantics are documented on `Rheo.Backend`.
  """

  @behaviour Rheo.Backend

  alias Rheo.Backend.Ecto.{Codec, Migrations, Server}
  alias Rheo.{Clock, Event, Id, Lag, Lease, Partition, Query, Telemetry}

  @streams "rheo_streams"
  @sequences "rheo_stream_sequences"
  @events "rheo_events"
  @groups "rheo_groups"
  @deliveries "rheo_deliveries"

  @event_columns ~s(id, stream, partition, sequence, "timestamp", "key", "type", metadata, payload)
  @terminal_statuses "('acked', 'rejected')"
  @claimable ~s{(status = 'available' OR (status = 'leased' AND expires_at IS NOT NULL AND expires_at <= ?))}
  @frontier_batch 500
  @notify_channel "rheo_events"

  @doc """
  Child spec for the configuration process that acts as the backend handle.

  ## Arguments

    * `opts` — keyword options, see the "Options" section above

  ## Examples

      iex> spec = Rheo.Backend.Ecto.child_spec(repo: MyApp.Repo, name: :demo_sql)
      iex> {spec.id, elem(spec.start, 0)}
      {{Rheo.Backend.Ecto.Server, :demo_sql}, Rheo.Backend.Ecto.Server}

  ## Returns

  A supervisor child spec map.

  ## Errors / raises

  Raises `ArgumentError` when `:repo` is missing.
  """
  @impl true
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    server_opts =
      opts
      |> validate_repo!()
      |> Keyword.put_new(:name, default_handle())

    %{
      id: {Server, Keyword.fetch!(server_opts, :name)},
      start: {Server, :start_link, [server_opts]},
      type: :worker,
      restart: :permanent
    }
  end

  @doc """
  Default handle name for the `Rheo` instance.

  ## Examples

      iex> Rheo.Backend.Ecto.default_handle()
      Rheo.Ecto

  ## Returns

  A process name atom.
  """
  @spec default_handle() :: atom()
  def default_handle, do: Rheo.Ecto

  @impl true
  def capabilities, do: build_capabilities(:postgres, false)

  @doc """
  Capabilities for a specific dialect or running handle.

  `capabilities/0` describes the PostgreSQL defaults. Pass `:sqlite` (or a live
  handle) to get the flags that actually apply to an instance, including whether
  `:notify` was enabled.

  ## Arguments

    * `dialect_or_handle` — `:postgres`, `:sqlite`, or a backend handle

  ## Examples

      iex> Rheo.Backend.Ecto.capabilities(:sqlite).distributed
      false

      iex> Rheo.Backend.Ecto.capabilities(:postgres).distributed
      true

  ## Returns

  A `t:Rheo.Backend.capabilities/0` map.
  """
  @spec capabilities(:postgres | :sqlite | Rheo.Backend.handle()) :: Rheo.Backend.capabilities()
  def capabilities(dialect) when dialect in [:postgres, :sqlite] do
    build_capabilities(dialect, false)
  end

  def capabilities(handle) do
    case Server.config(handle) do
      {:ok, config} -> build_capabilities(config.dialect, config.notify?)
      {:error, _reason} -> capabilities()
    end
  end

  @impl true
  def ping(handle) do
    with {:ok, config} <- Server.config(handle),
         {:ok, _result} <- run(config, "SELECT 1", []) do
      :ok
    end
  end

  @impl true
  def ensure_indexes(handle) do
    with {:ok, config} <- Server.config(handle) do
      Migrations.up(config.repo, dialect: config.dialect, prefix: config.prefix)
    end
  end

  @impl true
  def create_stream(handle, stream, opts \\ []) when is_binary(stream) do
    partition_count = Keyword.get(opts, :partition_count, 1)

    if is_integer(partition_count) and partition_count >= 1 do
      with {:ok, config} <- Server.config(handle),
           :ok <- ensure_indexes(handle),
           :ok <- insert_stream(config, stream, partition_count) do
        Telemetry.execute([:rheo, :stream, :create], %{count: 1}, %{stream: stream})
        :ok
      end
    else
      {:error, :invalid_partition_count}
    end
  end

  @impl true
  def create_group(handle, stream, group, opts \\ [])
      when is_binary(stream) and is_binary(group) do
    with {:ok, config} <- Server.config(handle),
         {:ok, stream_row} <- fetch_stream(config, stream),
         :ok <- insert_group(config, stream, group, stream_row, opts) do
      Telemetry.execute([:rheo, :group, :create], %{count: 1}, %{stream: stream, group: group})
      :ok
    end
  end

  @impl true
  def append(handle, stream, payload, opts \\ []) when is_map(payload) do
    Telemetry.span([:rheo, :append], %{stream: stream}, fn ->
      with {:ok, [event]} <- append_batch(handle, stream, [payload], opts), do: {:ok, event}
    end)
  end

  @impl true
  def append_batch(handle, stream, payloads, opts \\ []) when is_list(payloads) do
    Telemetry.span([:rheo, :append_batch], %{stream: stream, count: length(payloads)}, fn ->
      with {:ok, config} <- Server.config(handle),
           do: do_append_batch(config, stream, payloads, opts)
    end)
  end

  @impl true
  def read(handle, stream, opts \\ []) do
    with {:ok, config} <- Server.config(handle) do
      read_events(
        config,
        stream,
        Keyword.get(opts, :partition, 0),
        Keyword.get(opts, :after, 0),
        Keyword.get(opts, :limit, 100)
      )
    end
  end

  @impl true
  def query(handle, %Query{} = query) do
    Telemetry.span([:rheo, :query], %{stream: query.stream}, fn ->
      with {:ok, config} <- Server.config(handle), do: run_query(config, query)
    end)
  end

  @impl true
  def fetch(handle, stream, group, opts \\ []) do
    Telemetry.span([:rheo, :fetch], %{stream: stream, group: group}, fn ->
      with {:ok, config} <- Server.config(handle), do: do_fetch(config, stream, group, opts)
    end)
  end

  @impl true
  def renew(handle, %Lease{} = lease, opts \\ []) do
    Telemetry.span([:rheo, :lease, :renew], %{stream: lease.stream, group: lease.group}, fn ->
      with {:ok, config} <- Server.config(handle) do
        now = Clock.utc_now()
        expires_at = DateTime.add(now, lease_ms(opts), :millisecond)

        set = "expires_at = ?, renewed_at = ?"
        params = [timestamp(config, expires_at), timestamp(config, now)]

        with :ok <- mutate_lease(config, lease, set, params),
             do: {:ok, %{lease | expires_at: expires_at}}
      end
    end)
  end

  @impl true
  def ack(handle, %Lease{} = lease) do
    Telemetry.span([:rheo, :ack], %{stream: lease.stream, group: lease.group}, fn ->
      set = "status = 'acked', acked_at = ?, lease_id = NULL, expires_at = NULL"

      with {:ok, config} <- Server.config(handle),
           :ok <- mutate_lease(config, lease, set, [timestamp(config, Clock.utc_now())]) do
        advance_frontier(config, lease)
      end
    end)
  end

  @impl true
  def retry(handle, %Lease{} = lease, reason) do
    Telemetry.span([:rheo, :retry], %{stream: lease.stream, group: lease.group}, fn ->
      with {:ok, config} <- Server.config(handle),
           {:ok, group_row} <- fetch_group(config, lease.stream, lease.group) do
        do_retry(config, lease, reason, group_row["max_attempts"] || 5)
      end
    end)
  end

  @impl true
  def reject(handle, %Lease{} = lease, reason) do
    Telemetry.span([:rheo, :reject], %{stream: lease.stream, group: lease.group}, fn ->
      with {:ok, config} <- Server.config(handle), do: do_reject(config, lease, reason)
    end)
  end

  @impl true
  def replay(handle, stream, group, opts \\ []) do
    with {:ok, config} <- Server.config(handle),
         {:ok, group_row} <- fetch_group(config, stream, group),
         {:ok, stream_row} <- fetch_stream(config, stream),
         {:ok, partitions} <- resolve_assignment(opts, stream_row["partition_count"]) do
      do_replay(config, %{
        stream: stream,
        group: group,
        group_row: group_row,
        partitions: partitions,
        opts: opts
      })
    end
  end

  @impl true
  def reset_group(handle, stream, group, opts \\ []) do
    with {:ok, config} <- Server.config(handle),
         {:ok, group_row} <- fetch_group(config, stream, group),
         {:ok, stream_row} <- fetch_stream(config, stream),
         partition_count = stream_row["partition_count"],
         {:ok, partitions} <- resolve_assignment(opts, partition_count),
         {:ok, starts} <- resolve_group_start(config, stream, partition_count, opts),
         :ok <- delete_deliveries(config, stream, group, partitions) do
      cursors =
        Enum.reduce(partitions, group_cursors(group_row), fn partition, acc ->
          Map.put(acc, Partition.key(partition), Partition.map_get(starts, partition, 1))
        end)

      update_group_maps(
        config,
        stream,
        group,
        cursors,
        put_partitions(group_frontiers(group_row), partitions, 0)
      )
    end
  end

  @impl true
  def lag(handle, stream, group, opts \\ []) do
    with {:ok, config} <- Server.config(handle),
         {:ok, group_row} <- fetch_group(config, stream, group),
         {:ok, stream_row} <- fetch_stream(config, stream),
         {:ok, partitions} <- resolve_assignment(opts, stream_row["partition_count"]),
         {:ok, high_watermarks} <- stream_next_sequences(config, stream) do
      {:ok, Lag.from_maps(stream, group, group_frontiers(group_row), high_watermarks, partitions)}
    end
  end

  ## Streams and groups

  defp insert_stream(config, stream, partition_count) do
    transaction(config, fn ->
      if exists?(config, "SELECT 1 FROM #{table(config, @streams)} WHERE name = ?", [stream]) do
        {:error, :already_exists}
      else
        sql =
          "INSERT INTO #{table(config, @streams)} (name, partition_count, created_at) " <>
            "VALUES (?, ?, ?)"

        params = [stream, partition_count, timestamp(config, Clock.utc_now())]

        with {:ok, _result} <- run(config, sql, params),
             do: insert_sequence_rows(config, stream, partition_count)
      end
    end)
  end

  defp insert_sequence_rows(config, stream, partition_count) do
    sql =
      "INSERT INTO #{table(config, @sequences)} (stream, partition, next_sequence) " <>
        "VALUES (?, ?, 0)"

    Enum.reduce_while(0..(partition_count - 1), :ok, fn partition, :ok ->
      case run(config, sql, [stream, partition]) do
        {:ok, _result} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp insert_group(config, stream, group, stream_row, opts) do
    partition_count = stream_row["partition_count"]

    max_attempts =
      Keyword.get(opts, :max_attempts, Application.get_env(:rheo, :default_max_attempts, 5))

    with {:ok, cursors} <- resolve_group_start(config, stream, partition_count, opts) do
      transaction(config, fn ->
        group_filter = "SELECT 1 FROM #{table(config, @groups)} WHERE stream = ? AND name = ?"

        if exists?(config, group_filter, [stream, group]) do
          {:error, :already_exists}
        else
          write_group(config, stream, group, cursors, %{
            frontiers: string_partition_map(partition_count, 0),
            max_attempts: max_attempts
          })
        end
      end)
    end
  end

  defp write_group(config, stream, group, cursors, extra) do
    sql =
      "INSERT INTO #{table(config, @groups)} " <>
        "(stream, name, cursors, frontiers, max_attempts, created_at) VALUES (?, ?, ?, ?, ?, ?)"

    params = [
      stream,
      group,
      json(config, cursors),
      json(config, extra.frontiers),
      extra.max_attempts,
      timestamp(config, Clock.utc_now())
    ]

    with {:ok, _result} <- run(config, sql, params), do: :ok
  end

  defp resolve_group_start(config, stream, partition_count, opts) do
    with {:ok, selected} <- resolve_assignment(opts, partition_count) do
      cursors = string_partition_map(partition_count, 1)

      cond do
        Keyword.has_key?(opts, :start_after) ->
          {:ok, apply_start_after(cursors, selected, Keyword.fetch!(opts, :start_after))}

        Keyword.has_key?(opts, :start_at) ->
          apply_start_at(config, stream, cursors, selected, Keyword.fetch!(opts, :start_at))

        true ->
          {:ok, cursors}
      end
    end
  end

  defp apply_start_after(cursors, selected, start_after) do
    Enum.reduce(selected, cursors, fn partition, acc ->
      case start_after_for_partition(start_after, partition) do
        sequence when is_integer(sequence) -> Map.put(acc, Partition.key(partition), sequence + 1)
        _other -> acc
      end
    end)
  end

  defp apply_start_at(config, stream, cursors, selected, datetime) do
    Enum.reduce_while(selected, {:ok, cursors}, fn partition, {:ok, acc} ->
      case first_sequence_at(config, stream, partition, datetime) do
        {:ok, sequence} -> {:cont, {:ok, Map.put(acc, Partition.key(partition), sequence)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp first_sequence_at(config, stream, partition, datetime) do
    sql =
      "SELECT sequence FROM #{table(config, @events)} " <>
        ~s{WHERE stream = ? AND partition = ? AND "timestamp" >= ? ORDER BY sequence ASC LIMIT 1}

    case one(config, sql, [stream, partition, timestamp(config, datetime)]) do
      {:ok, nil} -> {:ok, 1}
      {:ok, row} -> {:ok, row["sequence"]}
      {:error, _reason} = error -> error
    end
  end

  ## Append

  defp do_append_batch(_config, _stream, [], _opts), do: {:ok, []}

  defp do_append_batch(config, stream, payloads, opts) do
    with {:ok, stream_row} <- fetch_stream(config, stream),
         {:ok, routed} <-
           resolve_payload_partitions(payloads, opts, stream_row["partition_count"]),
         {:ok, events} <- write_events(config, stream, routed, opts) do
      notify(config, stream, length(events))
      {:ok, events}
    end
  end

  defp write_events(config, stream, routed, opts) do
    transaction(config, fn ->
      with {:ok, starts} <- allocate_partition_sequences(config, stream, routed),
           do: insert_events(config, stream, routed, starts, opts)
    end)
  end

  defp allocate_partition_sequences(config, stream, routed) do
    routed
    |> Enum.frequencies_by(&elem(&1, 1))
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.reduce_while({:ok, %{}}, fn {partition, count}, {:ok, starts} ->
      case allocate_sequences(config, stream, partition, count) do
        {:ok, start} -> {:cont, {:ok, Map.put(starts, partition, start)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp allocate_sequences(config, stream, partition, count) do
    with {:ok, next} <- bump_sequence(config, stream, partition, count),
         do: {:ok, next - count + 1}
  end

  defp bump_sequence(%{dialect: :postgres} = config, stream, partition, count) do
    sql =
      "UPDATE #{table(config, @sequences)} SET next_sequence = next_sequence + ? " <>
        "WHERE stream = ? AND partition = ? RETURNING next_sequence"

    case run(config, sql, [count, stream, partition]) do
      {:ok, %{rows: [[next] | _]}} -> {:ok, next}
      {:ok, _result} -> insert_sequence_row(config, stream, partition, count)
      {:error, _reason} = error -> error
    end
  end

  defp bump_sequence(%{dialect: :sqlite} = config, stream, partition, count) do
    sql =
      "SELECT next_sequence FROM #{table(config, @sequences)} " <>
        "WHERE stream = ? AND partition = ?"

    case one(config, sql, [stream, partition]) do
      {:ok, nil} -> insert_sequence_row(config, stream, partition, count)
      {:ok, row} -> update_sequence_row(config, stream, partition, row["next_sequence"] + count)
      {:error, _reason} = error -> error
    end
  end

  defp insert_sequence_row(config, stream, partition, next) do
    sql =
      "INSERT INTO #{table(config, @sequences)} (stream, partition, next_sequence) " <>
        "VALUES (?, ?, ?)"

    with {:ok, _result} <- run(config, sql, [stream, partition, next]), do: {:ok, next}
  end

  defp update_sequence_row(config, stream, partition, next) do
    sql =
      "UPDATE #{table(config, @sequences)} SET next_sequence = ? " <>
        "WHERE stream = ? AND partition = ?"

    with {:ok, _result} <- run(config, sql, [next, stream, partition]), do: {:ok, next}
  end

  defp insert_events(config, stream, routed, starts, opts) do
    now = Clock.utc_now()

    {events, _offsets} =
      Enum.map_reduce(routed, %{}, fn {payload, partition}, offsets ->
        offset = Map.get(offsets, partition, 0)
        sequence = Map.fetch!(starts, partition) + offset
        event = build_event(stream, partition, sequence, payload, now, opts)
        {event, Map.put(offsets, partition, offset + 1)}
      end)

    Enum.reduce_while(events, {:ok, events}, fn event, acc ->
      case insert_event(config, event) do
        {:ok, _result} -> {:cont, acc}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp insert_event(config, %Event{} = event) do
    sql =
      "INSERT INTO #{table(config, @events)} (#{@event_columns}) " <>
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"

    run(config, sql, [
      event.id,
      event.stream,
      event.partition,
      event.sequence,
      timestamp(config, event.timestamp),
      event.key,
      event.type,
      json(config, event.metadata),
      json(config, event.payload)
    ])
  end

  defp notify(%{notify?: true} = config, stream, count) do
    payload = Jason.encode!(%{"stream" => stream, "count" => count})
    _ = run(config, "SELECT pg_notify(?, ?)", [@notify_channel, payload])
    :ok
  end

  defp notify(_config, _stream, _count), do: :ok

  ## Read and query

  defp read_events(config, stream, partition, after_sequence, limit) do
    sql =
      "SELECT #{@event_columns} FROM #{table(config, @events)} " <>
        "WHERE stream = ? AND partition = ? AND sequence > ? ORDER BY sequence ASC LIMIT ?"

    with {:ok, rows} <- all(config, sql, [stream, partition, after_sequence, limit]) do
      {:ok, Enum.map(rows, &Codec.event_from_row/1)}
    end
  end

  defp run_query(config, query) do
    query = Query.apply_cursor(query)
    {where, params} = build_where(config, query)

    sql =
      "SELECT #{@event_columns} FROM #{table(config, @events)} WHERE #{where} " <>
        "ORDER BY #{order_by(query.order_by)} LIMIT ?"

    with {:ok, rows} <- all(config, sql, params ++ [query.limit]) do
      {:ok, Enum.map(rows, &Codec.event_from_row/1)}
    end
  end

  defp build_where(config, %Query{} = query) do
    clauses =
      [{"stream = ?", [query.stream]}] ++
        Enum.map(query.where, &where_clause(config, &1)) ++
        bound_clauses(config, query)

    clauses = Enum.reject(clauses, &is_nil/1)

    {Enum.map_join(clauses, " AND ", &elem(&1, 0)), Enum.flat_map(clauses, &elem(&1, 1))}
  end

  defp bound_clauses(config, %Query{} = query) do
    [
      query.from && {~s{"timestamp" >= ?}, [timestamp(config, query.from)]},
      query.to && {~s{"timestamp" <= ?}, [timestamp(config, query.to)]},
      query.after_sequence && {"sequence > ?", [query.after_sequence]},
      query.until_sequence && {"sequence <= ?", [query.until_sequence]}
    ]
  end

  defp where_clause(_config, {:type, type}), do: {~s{"type" = ?}, [type]}
  defp where_clause(_config, {:key, key}), do: {~s{"key" = ?}, [key]}
  defp where_clause(_config, {:partition, partition}), do: {"partition = ?", [partition]}

  defp where_clause(config, {field, value}) when is_atom(field) do
    {column, path} = json_target(field)
    {"#{json_extract(config, column, path)} = ?", [to_string(value)]}
  end

  defp where_clause(_config, _other), do: nil

  @metadata_fields [:correlation_id, :causation_id, :producer, :schema, :schema_version]

  defp json_target(field) when field in @metadata_fields, do: {"metadata", field}
  defp json_target(field), do: {"payload", field}

  defp json_extract(%{dialect: :postgres}, column, path) do
    "#{column}->>'#{safe_path(path)}'"
  end

  defp json_extract(%{dialect: :sqlite}, column, path) do
    "CAST(json_extract(#{column}, '$.#{safe_path(path)}') AS TEXT)"
  end

  # Paths come from developer-written atoms, but never interpolate unchecked.
  defp safe_path(path) do
    path |> Atom.to_string() |> String.replace(~r/[^A-Za-z0-9_]/, "")
  end

  @order_columns %{
    sequence: "sequence",
    partition: "partition",
    timestamp: ~s("timestamp"),
    id: "id",
    type: ~s("type"),
    key: ~s("key")
  }

  defp order_by(order_by) when is_list(order_by) and order_by != [] do
    order_by
    |> Enum.map(&order_term/1)
    |> Enum.reject(&is_nil/1)
    |> case do
      [] -> "sequence ASC"
      terms -> Enum.join(terms, ", ")
    end
  end

  defp order_by(_order_by), do: "sequence ASC"

  defp order_term({field, direction}) do
    case Map.get(@order_columns, field) do
      nil -> nil
      column -> "#{column} #{direction(direction)}"
    end
  end

  defp order_term(_term), do: nil

  defp direction(direction) when direction in [:desc, -1], do: "DESC"
  defp direction(_direction), do: "ASC"

  ## Fetch, materialize, claim

  defp do_fetch(config, stream, group, opts) do
    with {:ok, _group_row} <- fetch_group(config, stream, group),
         {:ok, stream_row} <- fetch_stream(config, stream),
         {:ok, partitions} <- resolve_assignment(opts, stream_row["partition_count"]) do
      consumer_id = Keyword.get_lazy(opts, :consumer_id, &Id.generate/0)

      ctx = %{
        config: config,
        stream: stream,
        group: group,
        partitions: partitions,
        consumer_id: consumer_id,
        lease_ms: lease_ms(opts),
        limit: Keyword.get(opts, :limit, Application.get_env(:rheo, :default_max_demand, 10)),
        now: Clock.utc_now()
      }

      with {:ok, leases} = ok <- fetch_until(ctx, []) do
        Telemetry.execute([:rheo, :lease], %{count: length(leases)}, %{
          stream: stream,
          group: group,
          consumer_id: consumer_id
        })

        ok
      end
    end
  end

  defp fetch_until(%{limit: limit}, acc) when length(acc) >= limit, do: {:ok, acc}

  defp fetch_until(ctx, acc) do
    remaining = ctx.limit - length(acc)

    with {:ok, group_row} <- fetch_group(ctx.config, ctx.stream, ctx.group),
         :ok <- materialize_deliveries(ctx, group_row, max(remaining, 50)),
         {:ok, batch} <- claim_deliveries(ctx, remaining) do
      if batch == [], do: {:ok, acc}, else: fetch_until(ctx, acc ++ batch)
    end
  end

  defp materialize_deliveries(ctx, group_row, limit) do
    cursors = group_cursors(group_row)

    Enum.reduce_while(ctx.partitions, :ok, fn partition, :ok ->
      next_sequence = Partition.map_get(cursors, partition, 1)

      result =
        with {:ok, events} <-
               read_events(ctx.config, ctx.stream, partition, next_sequence - 1, limit),
             do: insert_deliveries(ctx, partition, events)

      case result do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp insert_deliveries(_ctx, _partition, []), do: :ok

  defp insert_deliveries(ctx, partition, events) do
    with :ok <- Enum.reduce_while(events, :ok, &insert_delivery(ctx, &1, &2)) do
      bump_cursor(ctx, partition, List.last(events).sequence + 1)
    end
  end

  defp insert_delivery(ctx, %Event{} = event, :ok) do
    config = ctx.config

    sql =
      "#{insert_ignore(config)} #{table(config, @deliveries)} " <>
        "(stream, group_name, event_id, partition, sequence, status, attempt, created_at) " <>
        "VALUES (?, ?, ?, ?, ?, 'available', 0, ?)#{on_conflict(config)}"

    params = [
      ctx.stream,
      ctx.group,
      event.id,
      event.partition,
      event.sequence,
      timestamp(config, Clock.utc_now())
    ]

    case run(config, sql, params) do
      {:ok, _result} -> {:cont, :ok}
      {:error, reason} -> {:halt, {:error, reason}}
    end
  end

  defp insert_ignore(%{dialect: :sqlite}), do: "INSERT OR IGNORE INTO"
  defp insert_ignore(_config), do: "INSERT INTO"

  defp on_conflict(%{dialect: :postgres}), do: " ON CONFLICT DO NOTHING"
  defp on_conflict(_config), do: ""

  defp bump_cursor(ctx, partition, next_sequence) do
    transaction(ctx.config, fn ->
      with {:ok, row} <- lock_group(ctx.config, ctx.stream, ctx.group) do
        cursors = group_cursors(row)

        if next_sequence > Partition.map_get(cursors, partition, 1) do
          updated = Map.put(cursors, Partition.key(partition), next_sequence)
          update_group_column(ctx.config, ctx.stream, ctx.group, "cursors", updated)
        else
          :ok
        end
      end
    end)
  end

  defp claim_deliveries(ctx, limit) when limit > 0 do
    config = ctx.config

    result =
      transaction(config, fn ->
        with {:ok, rows} <- select_claimable(ctx, limit),
             do: lease_rows(ctx, rows)
      end)

    with {:ok, claimed} <- result, do: build_leases(ctx, claimed)
  end

  defp claim_deliveries(_ctx, _limit), do: {:ok, []}

  defp select_claimable(ctx, limit) do
    config = ctx.config

    sql =
      "SELECT event_id, partition, sequence FROM #{table(config, @deliveries)} " <>
        "WHERE stream = ? AND group_name = ? AND partition IN #{placeholders(ctx.partitions)} " <>
        "AND #{@claimable} ORDER BY partition ASC, sequence ASC LIMIT ?#{lock_clause(config)}"

    params =
      [ctx.stream, ctx.group] ++
        ctx.partitions ++ [timestamp(config, ctx.now), limit]

    all(config, sql, params)
  end

  defp lock_clause(%{dialect: :postgres}), do: " FOR UPDATE SKIP LOCKED"
  defp lock_clause(_config), do: ""

  defp lease_rows(ctx, rows) do
    Enum.reduce_while(rows, {:ok, []}, fn row, {:ok, acc} ->
      case lease_row(ctx, row) do
        {:ok, nil} -> {:cont, {:ok, acc}}
        {:ok, claimed} -> {:cont, {:ok, [claimed | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, claimed} -> {:ok, Enum.reverse(claimed)}
      error -> error
    end
  end

  defp lease_row(ctx, row) do
    config = ctx.config
    lease_id = Id.generate()
    expires_at = DateTime.add(ctx.now, ctx.lease_ms, :millisecond)

    sql =
      "UPDATE #{table(config, @deliveries)} SET status = 'leased', lease_id = ?, " <>
        "consumer_id = ?, leased_at = ?, expires_at = ?, attempt = attempt + 1 " <>
        "WHERE stream = ? AND group_name = ? AND event_id = ? AND #{@claimable}"

    params = [
      lease_id,
      ctx.consumer_id,
      timestamp(config, ctx.now),
      timestamp(config, expires_at),
      ctx.stream,
      ctx.group,
      row["event_id"],
      timestamp(config, ctx.now)
    ]

    case run(config, sql, params) do
      {:ok, %{num_rows: 0}} -> {:ok, nil}
      {:ok, _result} -> {:ok, %{event_id: row["event_id"], lease_id: lease_id}}
      {:error, _reason} = error -> error
    end
  end

  defp build_leases(ctx, claimed) do
    expires_at = DateTime.add(ctx.now, ctx.lease_ms, :millisecond)

    claimed
    |> Enum.reduce_while({:ok, []}, fn entry, {:ok, acc} ->
      case load_lease(ctx, entry, expires_at) do
        {:ok, lease} -> {:cont, {:ok, [lease | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, leases} -> {:ok, Enum.reverse(leases)}
      error -> error
    end
  end

  defp load_lease(ctx, %{event_id: event_id, lease_id: lease_id}, expires_at) do
    with {:ok, event} <- fetch_event(ctx.config, event_id),
         {:ok, attempt} <- delivery_attempt(ctx, event_id) do
      maybe_redelivery(ctx.stream, ctx.group, event_id, attempt)

      {:ok,
       %Lease{
         lease_id: lease_id,
         stream: ctx.stream,
         group: ctx.group,
         event_id: event_id,
         event: event,
         consumer_id: ctx.consumer_id,
         attempt: attempt,
         leased_at: ctx.now,
         expires_at: expires_at
       }}
    end
  end

  defp delivery_attempt(ctx, event_id) do
    sql =
      "SELECT attempt FROM #{table(ctx.config, @deliveries)} " <>
        "WHERE stream = ? AND group_name = ? AND event_id = ?"

    case one(ctx.config, sql, [ctx.stream, ctx.group, event_id]) do
      {:ok, nil} -> {:error, :stale_lease}
      {:ok, row} -> {:ok, row["attempt"]}
      {:error, _reason} = error -> error
    end
  end

  defp maybe_redelivery(_stream, _group, _event_id, attempt) when attempt <= 1, do: :ok

  defp maybe_redelivery(stream, group, event_id, _attempt) do
    Telemetry.execute([:rheo, :redelivery], %{count: 1}, %{
      stream: stream,
      group: group,
      event_id: event_id
    })
  end

  ## Lease transitions

  defp mutate_lease(config, %Lease{} = lease, set, params) do
    sql =
      "UPDATE #{table(config, @deliveries)} SET #{set} WHERE stream = ? AND group_name = ? " <>
        "AND event_id = ? AND lease_id = ? AND status = 'leased'"

    fencing = [lease.stream, lease.group, lease.event_id, lease.lease_id]

    case run(config, sql, params ++ fencing) do
      {:ok, %{num_rows: 0}} -> {:error, :stale_lease}
      {:ok, _result} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp do_retry(config, lease, reason, max_attempts) when lease.attempt >= max_attempts do
    do_reject(config, lease, {:max_attempts, reason})
  end

  defp do_retry(config, lease, reason, _max_attempts) do
    set =
      "status = 'available', lease_id = NULL, consumer_id = NULL, expires_at = NULL, " <>
        "reason = ?, retried_at = ?"

    mutate_lease(config, lease, set, [
      encode_reason(reason),
      timestamp(config, Clock.utc_now())
    ])
  end

  defp do_reject(config, lease, reason) do
    set =
      "status = 'rejected', dead_lettered_at = ?, reason = ?, lease_id = NULL, expires_at = NULL"

    params = [timestamp(config, Clock.utc_now()), encode_reason(reason)]

    with :ok <- mutate_lease(config, lease, set, params) do
      Telemetry.execute([:rheo, :dead_letter], %{count: 1}, %{
        stream: lease.stream,
        group: lease.group,
        event_id: lease.event_id
      })

      advance_frontier(config, lease)
    end
  end

  ## Frontier

  defp advance_frontier(config, %Lease{} = lease) do
    partition = lease.event.partition

    _ =
      transaction(config, fn ->
        with {:ok, row} <- lock_group(config, lease.stream, lease.group),
             do: maybe_write_frontier(config, lease, group_frontiers(row), partition)
      end)

    :ok
  end

  defp maybe_write_frontier(config, lease, frontiers, partition) do
    old = Partition.map_get(frontiers, partition, 0)
    frontier = walk_frontier(config, lease.stream, lease.group, partition, old)

    if frontier > old do
      updated = Map.put(frontiers, Partition.key(partition), frontier)

      with :ok <-
             update_group_column(config, lease.stream, lease.group, "frontiers", updated) do
        Telemetry.execute([:rheo, :group, :frontier], %{frontier: frontier}, %{
          stream: lease.stream,
          group: lease.group,
          partition: partition,
          frontier: frontier
        })

        :ok
      end
    else
      :ok
    end
  end

  defp walk_frontier(config, stream, group, partition, frontier) do
    sql =
      "SELECT sequence FROM #{table(config, @deliveries)} WHERE stream = ? AND group_name = ? " <>
        "AND partition = ? AND status IN #{@terminal_statuses} AND sequence > ? " <>
        "ORDER BY sequence ASC LIMIT ?"

    case all(config, sql, [stream, group, partition, frontier, @frontier_batch]) do
      {:ok, rows} -> continue_walk(config, stream, group, partition, frontier, rows)
      {:error, _reason} -> frontier
    end
  end

  defp continue_walk(config, stream, group, partition, frontier, rows) do
    advanced =
      Enum.reduce_while(rows, frontier, fn row, acc ->
        if row["sequence"] == acc + 1, do: {:cont, acc + 1}, else: {:halt, acc}
      end)

    if length(rows) == @frontier_batch and advanced == frontier + @frontier_batch do
      walk_frontier(config, stream, group, partition, advanced)
    else
      advanced
    end
  end

  ## Replay and reset

  defp do_replay(config, ctx) do
    cond do
      events = Keyword.get(ctx.opts, :events) ->
        replay_events(config, ctx, Enum.filter(events, &(&1.partition in ctx.partitions)))

      ids = Keyword.get(ctx.opts, :event_ids) ->
        replay_event_ids(config, ctx, ids)

      Keyword.has_key?(ctx.opts, :from_sequence) ->
        replay_from_sequence(config, ctx, Keyword.fetch!(ctx.opts, :from_sequence) + 1)

      true ->
        {:error, :invalid_replay_opts}
    end
  end

  defp replay_events(_config, _ctx, []), do: :ok

  defp replay_events(config, ctx, events) do
    with :ok <- Enum.reduce_while(events, :ok, &upsert_replay_delivery(config, ctx, &1, &2)) do
      frontiers =
        Enum.reduce(events, group_frontiers(ctx.group_row), fn event, acc ->
          rewind(acc, event.partition, event.sequence)
        end)

      update_group_column(config, ctx.stream, ctx.group, "frontiers", frontiers)
    end
  end

  defp upsert_replay_delivery(config, ctx, %Event{} = event, :ok) do
    insert = replay_insert_sql(config)

    with {:ok, _result} <- run(config, insert, replay_params(config, ctx, event)),
         :ok <- reopen_delivery(config, ctx, event) do
      {:cont, :ok}
    else
      {:error, reason} -> {:halt, {:error, reason}}
    end
  end

  defp replay_insert_sql(config) do
    "#{insert_ignore(config)} #{table(config, @deliveries)} " <>
      "(stream, group_name, event_id, partition, sequence, status, attempt, created_at) " <>
      "VALUES (?, ?, ?, ?, ?, 'available', 0, ?)#{on_conflict(config)}"
  end

  defp replay_params(config, ctx, event) do
    [
      ctx.stream,
      ctx.group,
      event.id,
      event.partition,
      event.sequence,
      timestamp(config, Clock.utc_now())
    ]
  end

  defp reopen_delivery(config, ctx, %Event{} = event) do
    sql =
      "UPDATE #{table(config, @deliveries)} SET #{reopen_set()}, partition = ?, sequence = ? " <>
        "WHERE stream = ? AND group_name = ? AND event_id = ?"

    params =
      reopen_params(config) ++
        [event.partition, event.sequence, ctx.stream, ctx.group, event.id]

    with {:ok, _result} <- run(config, sql, params), do: :ok
  end

  defp replay_event_ids(_config, _ctx, []), do: :ok

  defp replay_event_ids(config, ctx, event_ids) do
    deliveries = table(config, @deliveries)

    filter =
      "stream = ? AND group_name = ? AND event_id IN #{placeholders(event_ids)} " <>
        "AND partition IN #{placeholders(ctx.partitions)}"

    scope = [ctx.stream, ctx.group] ++ event_ids ++ ctx.partitions
    select = "SELECT partition, sequence FROM #{deliveries} WHERE #{filter}"
    update = "UPDATE #{deliveries} SET #{reopen_set()} WHERE #{filter}"

    with {:ok, rows} <- all(config, select, scope),
         {:ok, _result} <- run(config, update, reopen_params(config) ++ scope) do
      frontiers =
        Enum.reduce(rows, group_frontiers(ctx.group_row), fn row, acc ->
          rewind(acc, row["partition"], row["sequence"])
        end)

      update_group_column(config, ctx.stream, ctx.group, "frontiers", frontiers)
    end
  end

  defp replay_from_sequence(config, ctx, next_sequence) do
    filter =
      "stream = ? AND group_name = ? AND partition IN #{placeholders(ctx.partitions)} " <>
        "AND sequence >= ?"

    params =
      reopen_params(config) ++ [ctx.stream, ctx.group] ++ ctx.partitions ++ [next_sequence]

    sql = "UPDATE #{table(config, @deliveries)} SET #{reopen_set()} WHERE #{filter}"

    with {:ok, _result} <- run(config, sql, params) do
      replay_frontier = max(next_sequence - 1, 0)

      frontiers =
        Enum.reduce(ctx.partitions, group_frontiers(ctx.group_row), fn partition, acc ->
          Map.put(
            acc,
            Partition.key(partition),
            min(Partition.map_get(acc, partition, 0), replay_frontier)
          )
        end)

      update_group_maps(
        config,
        ctx.stream,
        ctx.group,
        put_partitions(group_cursors(ctx.group_row), ctx.partitions, next_sequence),
        frontiers
      )
    end
  end

  defp reopen_set do
    "status = 'available', lease_id = NULL, consumer_id = NULL, expires_at = NULL, " <>
      "reason = 'replay', retried_at = ?"
  end

  defp reopen_params(config), do: [timestamp(config, Clock.utc_now())]

  defp rewind(frontiers, partition, sequence) when is_integer(sequence) do
    old = Partition.map_get(frontiers, partition, 0)
    Map.put(frontiers, Partition.key(partition), min(old, max(sequence - 1, 0)))
  end

  defp rewind(frontiers, _partition, _sequence), do: frontiers

  defp delete_deliveries(config, stream, group, partitions) do
    sql =
      "DELETE FROM #{table(config, @deliveries)} WHERE stream = ? AND group_name = ? " <>
        "AND partition IN #{placeholders(partitions)}"

    with {:ok, _result} <- run(config, sql, [stream, group] ++ partitions), do: :ok
  end

  ## Row helpers

  defp fetch_stream(config, stream) do
    sql = "SELECT name, partition_count FROM #{table(config, @streams)} WHERE name = ?"

    case one(config, sql, [stream]) do
      {:ok, nil} -> {:error, :stream_not_found}
      {:ok, row} -> {:ok, row}
      {:error, _reason} = error -> error
    end
  end

  defp fetch_group(config, stream, group) do
    sql =
      "SELECT stream, name, cursors, frontiers, max_attempts FROM #{table(config, @groups)} " <>
        "WHERE stream = ? AND name = ?"

    case one(config, sql, [stream, group]) do
      {:ok, nil} -> {:error, :group_not_found}
      {:ok, row} -> {:ok, row}
      {:error, _reason} = error -> error
    end
  end

  defp lock_group(config, stream, group) do
    sql =
      "SELECT cursors, frontiers FROM #{table(config, @groups)} " <>
        "WHERE stream = ? AND name = ?#{group_lock(config)}"

    case one(config, sql, [stream, group]) do
      {:ok, nil} -> {:error, :group_not_found}
      {:ok, row} -> {:ok, row}
      {:error, _reason} = error -> error
    end
  end

  defp group_lock(%{dialect: :postgres}), do: " FOR UPDATE"
  defp group_lock(_config), do: ""

  defp fetch_event(config, event_id) do
    sql = "SELECT #{@event_columns} FROM #{table(config, @events)} WHERE id = ?"

    case one(config, sql, [event_id]) do
      {:ok, nil} -> {:error, {:event_missing, event_id}}
      {:ok, row} -> {:ok, Codec.event_from_row(row)}
      {:error, _reason} = error -> error
    end
  end

  defp stream_next_sequences(config, stream) do
    sql =
      "SELECT partition, next_sequence FROM #{table(config, @sequences)} WHERE stream = ?"

    with {:ok, rows} <- all(config, sql, [stream]) do
      {:ok, Map.new(rows, &{Partition.key(&1["partition"]), &1["next_sequence"]})}
    end
  end

  defp update_group_column(config, stream, group, column, value) do
    sql =
      "UPDATE #{table(config, @groups)} SET #{column} = ? WHERE stream = ? AND name = ?"

    with {:ok, _result} <- run(config, sql, [json(config, value), stream, group]), do: :ok
  end

  defp update_group_maps(config, stream, group, cursors, frontiers) do
    sql =
      "UPDATE #{table(config, @groups)} SET cursors = ?, frontiers = ? " <>
        "WHERE stream = ? AND name = ?"

    params = [json(config, cursors), json(config, frontiers), stream, group]

    with {:ok, _result} <- run(config, sql, params), do: :ok
  end

  defp group_cursors(row), do: Codec.decode_json(row["cursors"])
  defp group_frontiers(row), do: Codec.decode_json(row["frontiers"])

  ## Event building

  defp build_event(stream, partition, sequence, payload, now, opts) do
    type = Map.get(payload, :type) || Map.get(payload, "type")
    key = Keyword.get(opts, :key) || Map.get(payload, :key) || Map.get(payload, "key")
    {metadata, body} = split_metadata(payload, Keyword.get(opts, :metadata, %{}))

    %Event{
      id: Keyword.get_lazy(opts, :id, &Id.generate/0),
      stream: stream,
      partition: partition,
      sequence: sequence,
      timestamp: Keyword.get(opts, :timestamp, now),
      key: key && to_string(key),
      type: type && to_string(type),
      metadata: stringify_keys(metadata),
      payload: stringify_keys(body)
    }
  end

  defp split_metadata(payload, metadata) do
    case Map.pop(payload, :metadata) do
      {nil, rest} -> pop_string_metadata(rest, metadata)
      {found, rest} -> {Map.merge(metadata, found), rest}
    end
  end

  defp pop_string_metadata(rest, metadata) do
    case Map.pop(rest, "metadata") do
      {nil, remaining} -> {metadata, remaining}
      {found, remaining} -> {Map.merge(metadata, found), remaining}
    end
  end

  defp stringify_keys(map) when is_map(map) do
    Map.new(map, fn
      {key, value} when is_atom(key) -> {Atom.to_string(key), stringify_value(value)}
      {key, value} -> {key, stringify_value(value)}
    end)
  end

  defp stringify_value(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
  defp stringify_value(map) when is_map(map), do: stringify_keys(map)
  defp stringify_value(list) when is_list(list), do: Enum.map(list, &stringify_value/1)
  defp stringify_value(other), do: other

  ## Partition helpers

  defp resolve_payload_partitions(payloads, opts, partition_count) do
    payloads
    |> Enum.reduce_while({:ok, []}, fn payload, {:ok, acc} ->
      case Partition.resolve(payload, opts, partition_count) do
        {:ok, partition} -> {:cont, {:ok, [{payload, partition} | acc]}}
        {:error, :invalid_partition} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, routed} -> {:ok, Enum.reverse(routed)}
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

    Partition.normalize_assignment(assignment, partition_count || 1)
  end

  defp string_partition_map(partition_count, value) do
    Map.new(0..(partition_count - 1), &{Partition.key(&1), value})
  end

  defp put_partitions(map, partitions, value) do
    Enum.reduce(partitions, map, &Map.put(&2, Partition.key(&1), value))
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

  defp start_after_for_partition(_sequences, _partition), do: nil

  ## SQL plumbing

  defp table(%{prefix: nil}, name), do: name
  defp table(%{prefix: prefix}, name), do: ~s("#{prefix}"."#{name}")

  defp json(config, value), do: Codec.encode_json(config.dialect, value)
  defp timestamp(config, value), do: Codec.encode_datetime(config.dialect, value)

  defp placeholders(values) when is_list(values) and values != [] do
    "(" <> Enum.map_join(values, ", ", fn _value -> "?" end) <> ")"
  end

  defp placeholders(_values), do: "(NULL)"

  defp run(config, sql, params) do
    case Ecto.Adapters.SQL.query(config.repo, bind(config.dialect, sql), params) do
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:error, map_backend_error(reason)}
    end
  end

  defp all(config, sql, params) do
    with {:ok, result} <- run(config, sql, params),
         do: {:ok, Codec.rows(result.columns, result.rows)}
  end

  defp one(config, sql, params) do
    with {:ok, rows} <- all(config, sql, params), do: {:ok, List.first(rows)}
  end

  defp exists?(config, sql, params) do
    match?({:ok, [_row | _rest]}, all(config, sql, params))
  end

  # Rheo writes portable `?` placeholders; PostgreSQL wants positional ones.
  defp bind(:sqlite, sql), do: sql

  defp bind(:postgres, sql) do
    [first | rest] = String.split(sql, "?")

    rest
    |> Enum.with_index(1)
    |> Enum.reduce(first, fn {part, index}, acc -> acc <> "$#{index}" <> part end)
  end

  defp transaction(config, fun) do
    config.repo.transaction(fn ->
      case fun.() do
        {:error, reason} -> config.repo.rollback(reason)
        other -> other
      end
    end)
    |> case do
      {:ok, result} -> result
      {:error, reason} -> {:error, reason}
    end
  end

  defp lease_ms(opts) do
    Keyword.get(opts, :lease_ms, Application.get_env(:rheo, :default_lease_ms, 30_000))
  end

  defp build_capabilities(dialect, notify?) do
    %{
      durable: true,
      distributed: dialect == :postgres,
      atomic_compare_and_set: true,
      notifications: notify?,
      change_feed: false,
      secondary_indexes: true,
      batch_writes: true,
      ordered_range_scan: true,
      replay: true,
      partitions: true,
      contiguous_frontier: true
    }
  end

  defp validate_repo!(opts) do
    if Keyword.has_key?(opts, :repo) do
      opts
    else
      raise ArgumentError,
            "Rheo.Backend.Ecto requires a host-owned :repo, e.g. " <>
              "{Rheo, backend: {Rheo.Backend.Ecto, repo: MyApp.Repo}}"
    end
  end

  defp encode_reason(reason) when is_binary(reason), do: reason
  defp encode_reason(reason), do: inspect(reason)

  defp map_backend_error(%DBConnection.ConnectionError{}), do: :backend_unavailable
  defp map_backend_error(reason), do: reason
end
