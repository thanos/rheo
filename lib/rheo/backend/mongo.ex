defmodule Rheo.Backend.Mongo do
  @moduledoc """
  MongoDB implementation of `Rheo.Backend`.

  Prefer the `Rheo` facade for application code. Use this module when you need
  the topology name or to embed the Mongo child spec directly.

  ## Collections

    * `streams` — stream registry and sequence allocator
    * `events` — immutable event log
    * `groups` — consumer group registry and materialization cursor
    * `deliveries` — per `(stream, group, event_id)` lease / ACK / DLQ state

  ## Supervision example

      children = [
        Rheo.Backend.Mongo.child_spec(url: "mongodb://localhost:27017/rheo", name: Rheo.Mongo)
      ]

  Callback semantics are documented on `Rheo.Backend`.
  """

  @behaviour Rheo.Backend

  alias Rheo.{Clock, Event, Id, Lease, Telemetry}

  @streams "streams"
  @events "events"
  @groups "groups"
  @deliveries "deliveries"

  @doc """
  Child spec for the Mongo topology process.

  ## Arguments

    * `opts` — keyword options:
      * `:url` — Mongo URL (default: `Application.get_env(:rheo, :mongo_url)`)
      * `:name` — topology name (default: `topology_name/0`)
      * `:pool_size` — pool size (default `5`)
      * plus other options accepted by `Mongo.start_link/1`

  ## Examples

      iex> spec = Rheo.Backend.Mongo.child_spec(url: "mongodb://localhost:27017/rheo", name: :demo_mongo)
      iex> {spec.id, elem(spec.start, 0), Keyword.fetch!(elem(spec.start, 2) |> hd(), :name)}
      {{Mongo, :demo_mongo}, Mongo, :demo_mongo}

  ## Returns

  A supervisor child spec map. Does not raise; missing `:mongo_url` when `:url`
  is omitted raises `ArgumentError` via `Application.fetch_env!/2` at start time.
  """
  @impl true
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    mongo_opts =
      opts
      |> Keyword.put_new(:name, topology_name())
      |> Keyword.put_new_lazy(:url, fn -> Application.fetch_env!(:rheo, :mongo_url) end)
      |> Keyword.put_new(:pool_size, 5)

    %{
      id: {Mongo, Keyword.fetch!(mongo_opts, :name)},
      start: {Mongo, :start_link, [mongo_opts]},
      type: :worker,
      restart: :permanent
    }
  end

  @doc """
  Returns the configured Mongo topology process name.

  ## Examples

      iex> Rheo.Backend.Mongo.topology_name()
      Rheo.Mongo

  ## Returns

  A process name atom (default `Rheo.Mongo`), from `Application.get_env(:rheo, :topology)`.
  """
  @spec topology_name() :: atom()
  def topology_name, do: Application.get_env(:rheo, :topology, Rheo.Mongo)

  @impl true
  def ping(topo) do
    case Mongo.command(topo, ping: 1) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def ensure_indexes(topo) do
    indexes()
    |> Enum.reduce_while(:ok, fn {coll, keys, opts}, :ok ->
      case create_index(topo, coll, keys, opts) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp indexes do
    [
      {@streams, %{"name" => 1}, [unique: true, name: "streams_name"]},
      {@events, %{"stream" => 1, "partition" => 1, "sequence" => 1},
       [unique: true, name: "events_stream_partition_sequence"]},
      {@events, %{"stream" => 1, "timestamp" => 1}, [name: "events_stream_timestamp"]},
      {@events, %{"stream" => 1, "type" => 1}, [name: "events_stream_type"]},
      {@events, %{"stream" => 1, "key" => 1}, [name: "events_stream_key"]},
      {@events, %{"stream" => 1, "payload.currency" => 1, "payload.curve" => 1},
       [name: "events_stream_currency_curve"]},
      {@events, %{"stream" => 1, "metadata.correlation_id" => 1},
       [name: "events_stream_correlation"]},
      {@groups, %{"stream" => 1, "name" => 1}, [unique: true, name: "groups_stream_name"]},
      {@deliveries, %{"stream" => 1, "group" => 1, "event_id" => 1},
       [unique: true, name: "deliveries_stream_group_event"]},
      {@deliveries, %{"stream" => 1, "group" => 1, "status" => 1, "expires_at" => 1},
       [name: "deliveries_claim"]}
    ]
  end

  @impl true
  def create_stream(topo, stream, opts \\ []) when is_binary(stream) do
    doc = %{
      "name" => stream,
      "partition_count" => Keyword.get(opts, :partition_count, 1),
      "next_sequence" => 0,
      "created_at" => Clock.utc_now()
    }

    case Mongo.insert_one(topo, @streams, doc) do
      {:ok, _} ->
        :ok = ensure_indexes(topo)
        Telemetry.execute([:rheo, :stream, :create], %{count: 1}, %{stream: stream})
        :ok

      {:error, reason} ->
        already_or_error(reason)
    end
  end

  @impl true
  def create_group(topo, stream, group, opts \\ [])
      when is_binary(stream) and is_binary(group) do
    with {:ok, _} <- fetch_stream(topo, stream), do: insert_group(topo, stream, group, opts)
  end

  defp insert_group(topo, stream, group, opts) do
    max_attempts =
      Keyword.get(opts, :max_attempts, Application.get_env(:rheo, :default_max_attempts, 5))

    doc = %{
      "stream" => stream,
      "name" => group,
      "next_sequence" => 1,
      "max_attempts" => max_attempts,
      "created_at" => Clock.utc_now()
    }

    case Mongo.insert_one(topo, @groups, doc) do
      {:ok, _} ->
        Telemetry.execute([:rheo, :group, :create], %{count: 1}, %{stream: stream, group: group})
        :ok

      {:error, reason} ->
        already_or_error(reason)
    end
  end

  @impl true
  def append(topo, stream, payload, opts \\ []) when is_map(payload) do
    Telemetry.span([:rheo, :append], %{stream: stream}, fn ->
      with {:ok, [event]} <- append_batch(topo, stream, [payload], opts), do: {:ok, event}
    end)
  end

  @impl true
  def append_batch(topo, stream, payloads, opts \\ []) when is_list(payloads) do
    Telemetry.span([:rheo, :append_batch], %{stream: stream, count: length(payloads)}, fn ->
      do_append_batch(topo, stream, payloads, opts)
    end)
  end

  defp do_append_batch(_topo, _stream, [], _opts), do: {:ok, []}

  defp do_append_batch(topo, stream, payloads, opts) do
    with {:ok, _} <- fetch_stream(topo, stream),
         {:ok, start_seq} <- allocate_sequences(topo, stream, length(payloads)) do
      insert_event_docs(topo, stream, payloads, start_seq, opts)
    end
  end

  defp insert_event_docs(topo, stream, payloads, start_seq, opts) do
    now = Clock.utc_now()
    partition = Keyword.get(opts, :partition, 0)

    docs =
      payloads
      |> Enum.with_index()
      |> Enum.map(fn {payload, idx} ->
        build_event_doc(stream, partition, start_seq + idx, payload, now, opts)
      end)

    case Mongo.insert_many(topo, @events, docs, ordered: true) do
      {:ok, _} -> {:ok, Enum.map(docs, &Event.from_doc/1)}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def read(topo, stream, opts \\ []) do
    after_seq = Keyword.get(opts, :after, 0)
    limit = Keyword.get(opts, :limit, 100)
    partition = Keyword.get(opts, :partition, 0)

    filter = %{
      "stream" => stream,
      "partition" => partition,
      "sequence" => %{"$gt" => after_seq}
    }

    events =
      topo
      |> Mongo.find(@events, filter, sort: %{"sequence" => 1}, limit: limit)
      |> Enum.map(&Event.from_doc/1)

    {:ok, events}
  end

  @impl true
  def query(topo, stream, opts \\ []) do
    Telemetry.span([:rheo, :query], %{stream: stream}, fn ->
      filter = build_query_filter(stream, opts)
      limit = Keyword.get(opts, :limit, 100)
      sort = Keyword.get(opts, :sort, %{"sequence" => 1})

      events =
        topo
        |> Mongo.find(@events, filter, sort: sort, limit: limit)
        |> Enum.map(&Event.from_doc/1)

      {:ok, events}
    end)
  end

  @impl true
  def fetch(topo, stream, group, opts \\ []) do
    Telemetry.span([:rheo, :fetch], %{stream: stream, group: group}, fn ->
      do_fetch(topo, stream, group, opts)
    end)
  end

  defp do_fetch(topo, stream, group, opts) do
    with {:ok, _} <- fetch_group(topo, stream, group) do
      limit = Keyword.get(opts, :limit, Application.get_env(:rheo, :default_max_demand, 10))
      consumer_id = Keyword.get_lazy(opts, :consumer_id, &Id.generate/0)

      lease_ms =
        Keyword.get(opts, :lease_ms, Application.get_env(:rheo, :default_lease_ms, 30_000))

      now = Clock.utc_now()

      case fetch_until(topo, stream, group, consumer_id, lease_ms, limit, now, []) do
        {:ok, leases} = ok ->
          Telemetry.execute([:rheo, :lease], %{count: length(leases)}, %{
            stream: stream,
            group: group,
            consumer_id: consumer_id
          })

          ok

        other ->
          other
      end
    end
  end

  defp fetch_until(_topo, _stream, _group, _consumer_id, _lease_ms, limit, _now, acc)
       when length(acc) >= limit do
    {:ok, acc}
  end

  defp fetch_until(topo, stream, group, consumer_id, lease_ms, limit, now, acc) do
    remaining = limit - length(acc)

    with {:ok, group_doc} <- fetch_group(topo, stream, group),
         :ok <- materialize_deliveries(topo, stream, group, group_doc, max(remaining, 50)),
         {:ok, batch} <-
           claim_deliveries(topo, stream, group, consumer_id, lease_ms, remaining, now) do
      if batch == [] do
        {:ok, acc}
      else
        fetch_until(topo, stream, group, consumer_id, lease_ms, limit, now, acc ++ batch)
      end
    end
  end

  @impl true
  def ack(topo, %Lease{} = lease) do
    Telemetry.span([:rheo, :ack], %{stream: lease.stream, group: lease.group}, fn ->
      update = %{
        "$set" => %{
          "status" => "acked",
          "acked_at" => Clock.utc_now(),
          "lease_id" => nil,
          "expires_at" => nil
        }
      }

      mutate_lease(topo, lease, update)
    end)
  end

  @impl true
  def retry(topo, %Lease{} = lease, reason) do
    Telemetry.span([:rheo, :retry], %{stream: lease.stream, group: lease.group}, fn ->
      with {:ok, group_doc} <- fetch_group(topo, lease.stream, lease.group) do
        max_attempts = group_doc["max_attempts"] || group_doc[:max_attempts] || 5
        do_retry(topo, lease, reason, max_attempts)
      end
    end)
  end

  defp do_retry(topo, lease, reason, max_attempts) when lease.attempt >= max_attempts do
    reject(topo, lease, {:max_attempts, reason})
  end

  defp do_retry(topo, lease, reason, _max_attempts) do
    update = %{
      "$set" => %{
        "status" => "available",
        "lease_id" => nil,
        "consumer_id" => nil,
        "expires_at" => nil,
        "reason" => encode_reason(reason),
        "retried_at" => Clock.utc_now()
      }
    }

    mutate_lease(topo, lease, update)
  end

  @impl true
  def reject(topo, %Lease{} = lease, reason) do
    Telemetry.span([:rheo, :reject], %{stream: lease.stream, group: lease.group}, fn ->
      update = %{
        "$set" => %{
          "status" => "rejected",
          "dead_lettered_at" => Clock.utc_now(),
          "reason" => encode_reason(reason),
          "lease_id" => nil,
          "expires_at" => nil
        }
      }

      case mutate_lease(topo, lease, update) do
        :ok ->
          Telemetry.execute([:rheo, :dead_letter], %{count: 1}, %{
            stream: lease.stream,
            group: lease.group,
            event_id: lease.event_id
          })

          :ok

        error ->
          error
      end
    end)
  end

  defp mutate_lease(topo, lease, update) do
    case Mongo.find_one_and_update(topo, @deliveries, lease_filter(lease), update,
           return_document: :after
         ) do
      {:ok, %Mongo.FindAndModifyResult{value: nil}} -> {:error, :stale_lease}
      {:ok, %Mongo.FindAndModifyResult{value: _}} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp lease_filter(lease) do
    %{
      "stream" => lease.stream,
      "group" => lease.group,
      "event_id" => lease.event_id,
      "lease_id" => lease.lease_id,
      "status" => "leased"
    }
  end

  defp allocate_sequences(topo, stream, count) do
    case Mongo.find_one_and_update(
           topo,
           @streams,
           %{"name" => stream},
           %{"$inc" => %{"next_sequence" => count}},
           return_document: :after
         ) do
      {:ok, result} -> sequences_from_result(result, count)
      {:error, reason} -> {:error, reason}
    end
  end

  defp sequences_from_result(result, count) do
    case get_value(result) do
      %{"next_sequence" => next} -> {:ok, next - count + 1}
      %{next_sequence: next} -> {:ok, next - count + 1}
      nil -> {:error, :stream_not_found}
    end
  end

  defp build_event_doc(stream, partition, sequence, payload, now, opts) do
    id = Keyword.get_lazy(opts, :id, &Id.generate/0)
    type = Map.get(payload, :type) || Map.get(payload, "type")
    key = Keyword.get(opts, :key) || Map.get(payload, :key) || Map.get(payload, "key")
    metadata = Keyword.get(opts, :metadata, %{})
    timestamp = Keyword.get(opts, :timestamp, now)
    {event_meta, event_payload} = split_metadata(payload, metadata)

    %{
      "_id" => id,
      "stream" => stream,
      "partition" => partition,
      "sequence" => sequence,
      "timestamp" => timestamp,
      "key" => key,
      "type" => type,
      "metadata" => event_meta,
      "payload" => event_payload
    }
  end

  defp split_metadata(payload, metadata) do
    case Map.pop(payload, :metadata) do
      {nil, rest} -> pop_string_metadata(rest, metadata)
      {m, rest} -> {Map.merge(metadata, stringify_keys(m)), stringify_keys(rest)}
    end
  end

  defp pop_string_metadata(rest, metadata) do
    case Map.pop(rest, "metadata") do
      {nil, r} -> {metadata, stringify_keys(r)}
      {m, r} -> {Map.merge(metadata, stringify_keys(m)), stringify_keys(r)}
    end
  end

  defp materialize_deliveries(topo, stream, group, group_doc, limit) do
    next_seq = group_doc["next_sequence"] || group_doc[:next_sequence] || 1
    {:ok, events} = read(topo, stream, after: next_seq - 1, limit: limit)
    insert_delivery_docs(topo, stream, group, next_seq, events)
  end

  defp insert_delivery_docs(_topo, _stream, _group, _next_seq, []), do: :ok

  defp insert_delivery_docs(topo, stream, group, next_seq, events) do
    docs = Enum.map(events, &delivery_doc(stream, group, &1))
    _ = ignore_duplicate(Mongo.insert_many(topo, @deliveries, docs, ordered: false))
    last_seq = List.last(events).sequence

    _ =
      Mongo.find_one_and_update(
        topo,
        @groups,
        %{"stream" => stream, "name" => group, "next_sequence" => next_seq},
        %{"$set" => %{"next_sequence" => last_seq + 1}},
        return_document: :after
      )

    :ok
  end

  defp delivery_doc(stream, group, %Event{} = event) do
    %{
      "stream" => stream,
      "group" => group,
      "event_id" => event.id,
      "partition" => event.partition,
      "sequence" => event.sequence,
      "status" => "available",
      "attempt" => 0,
      "lease_id" => nil,
      "consumer_id" => nil,
      "expires_at" => nil,
      "created_at" => Clock.utc_now()
    }
  end

  defp claim_deliveries(topo, stream, group, consumer_id, lease_ms, limit, now) do
    claim_loop(topo, stream, group, consumer_id, lease_ms, limit, now, [])
  end

  defp claim_loop(_topo, _stream, _group, _consumer_id, _lease_ms, 0, _now, acc) do
    {:ok, Enum.reverse(acc)}
  end

  defp claim_loop(topo, stream, group, consumer_id, lease_ms, remaining, now, acc) do
    case claim_one(topo, stream, group, consumer_id, lease_ms, now) do
      {:ok, nil} ->
        {:ok, Enum.reverse(acc)}

      {:ok, lease} ->
        claim_loop(topo, stream, group, consumer_id, lease_ms, remaining - 1, now, [lease | acc])

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp claim_one(topo, stream, group, consumer_id, lease_ms, now) do
    lease_id = Id.generate()
    expires_at = DateTime.add(now, lease_ms, :millisecond)

    filter = %{
      "stream" => stream,
      "group" => group,
      "$or" => [
        %{"status" => "available"},
        %{"status" => "leased", "expires_at" => %{"$lte" => now}}
      ]
    }

    update = %{
      "$set" => %{
        "status" => "leased",
        "lease_id" => lease_id,
        "consumer_id" => consumer_id,
        "leased_at" => now,
        "expires_at" => expires_at
      },
      "$inc" => %{"attempt" => 1}
    }

    case Mongo.find_one_and_update(topo, @deliveries, filter, update,
           sort: %{"sequence" => 1},
           return_document: :after
         ) do
      {:ok, result} ->
        to_lease(topo, stream, group, consumer_id, lease_id, expires_at, now, get_value(result))

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp to_lease(_topo, _stream, _group, _consumer_id, _lease_id, _expires_at, _now, nil) do
    {:ok, nil}
  end

  defp to_lease(topo, stream, group, consumer_id, lease_id, expires_at, now, delivery) do
    event_id = delivery["event_id"] || delivery[:event_id]

    case Mongo.find_one(topo, @events, %{"_id" => event_id}) do
      nil ->
        {:error, {:event_missing, event_id}}

      event_doc ->
        attempt = delivery["attempt"] || delivery[:attempt]
        maybe_redelivery(stream, group, event_id, attempt)

        {:ok,
         %Lease{
           lease_id: lease_id,
           stream: stream,
           group: group,
           event_id: event_id,
           event: Event.from_doc(event_doc),
           consumer_id: consumer_id,
           attempt: attempt,
           leased_at: now,
           expires_at: expires_at
         }}
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

  defp fetch_stream(topo, stream) do
    case Mongo.find_one(topo, @streams, %{"name" => stream}) do
      nil -> {:error, :stream_not_found}
      doc -> {:ok, doc}
    end
  end

  defp fetch_group(topo, stream, group) do
    case Mongo.find_one(topo, @groups, %{"stream" => stream, "name" => group}) do
      nil -> {:error, :group_not_found}
      doc -> {:ok, doc}
    end
  end

  defp build_query_filter(stream, opts) do
    Enum.reduce(opts, %{"stream" => stream}, &apply_query_opt/2)
  end

  defp apply_query_opt({:type, type}, acc), do: Map.put(acc, "type", type)
  defp apply_query_opt({:key, key}, acc), do: Map.put(acc, "key", key)
  defp apply_query_opt({:partition, p}, acc), do: Map.put(acc, "partition", p)
  defp apply_query_opt({:currency, c}, acc), do: Map.put(acc, "payload.currency", c)
  defp apply_query_opt({:curve, c}, acc), do: Map.put(acc, "payload.curve", c)

  defp apply_query_opt({:correlation_id, id}, acc),
    do: Map.put(acc, "metadata.correlation_id", id)

  defp apply_query_opt({:producer, p}, acc), do: Map.put(acc, "metadata.producer", p)

  defp apply_query_opt({:from, %DateTime{} = dt}, acc),
    do: put_in_range(acc, "timestamp", "$gte", dt)

  defp apply_query_opt({:to, %DateTime{} = dt}, acc),
    do: put_in_range(acc, "timestamp", "$lte", dt)

  defp apply_query_opt({:limit, _}, acc), do: acc
  defp apply_query_opt({:sort, _}, acc), do: acc

  defp apply_query_opt({field, value}, acc) when is_atom(field),
    do: Map.put(acc, "payload.#{field}", value)

  defp apply_query_opt(_, acc), do: acc

  defp put_in_range(acc, field, op, value) do
    existing = Map.get(acc, field, %{})
    Map.put(acc, field, Map.put(existing, op, value))
  end

  defp create_index(topo, coll, keys, opts) do
    index = [
      key: keys,
      name: Keyword.fetch!(opts, :name),
      unique: Keyword.get(opts, :unique, false)
    ]

    Mongo.create_indexes(topo, coll, [index])
  end

  defp get_value(%Mongo.FindAndModifyResult{value: value}), do: value
  defp get_value(_), do: nil

  defp already_or_error(reason) do
    if duplicate_key_error?(reason), do: {:error, :already_exists}, else: {:error, reason}
  end

  defp ignore_duplicate(result) do
    case result do
      {:ok, _} -> :ok
      {:error, reason} -> if duplicate_key_error?(reason), do: :ok, else: {:error, reason}
      _ -> :ok
    end
  end

  defp duplicate_key_error?(%Mongo.WriteError{write_errors: errors}) when is_list(errors) do
    Enum.any?(errors, &duplicate_code?/1)
  end

  defp duplicate_key_error?(_), do: false

  defp duplicate_code?(%{"code" => 11_000}), do: true
  defp duplicate_code?(%{code: 11_000}), do: true
  defp duplicate_code?(_), do: false

  defp encode_reason(reason) when is_binary(reason), do: reason
  defp encode_reason(reason), do: inspect(reason)

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
end
