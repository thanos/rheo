# Optional integration: compiled only when `:mongodb_driver` is present.
if Code.ensure_loaded?(Mongo) do
  defmodule Rheo.Backend.Mongo do
    @moduledoc """
    MongoDB implementation of `Rheo.Backend`.

    Requires `{:mongodb_driver, "~> 1.5"}` in your dependencies.

    Prefer the `Rheo` facade for application code. The opaque handle is the Mongo
    process name (or pid) started via `child_spec/1`.

    ## Collections

      * `streams` — stream registry and sequence allocator
      * `events` — immutable event log
      * `groups` — consumer group registry and materialization cursor
      * `deliveries` — per `(stream, group, event_id)` lease / ACK / DLQ state

    ## Supervision example

        children = [
          {Rheo, name: MyRheo, backend: {Rheo.Backend.Mongo, url: "mongodb://localhost:27017/rheo"}}
        ]

    Callback semantics are documented on `Rheo.Backend`.
    """

    @behaviour Rheo.Backend

    alias Rheo.Backend.Mongo.Client
    alias Rheo.Backend.Mongo.Codec
    alias Rheo.{Clock, DeadLetter, Event, GroupInfo, Id, Lag, Lease, Partition, Query, Telemetry}

    @streams "streams"
    @events "events"
    @groups "groups"
    @deliveries "deliveries"

    @doc """
    Child spec for the Mongo connection process (backend handle).

    ## Arguments

      * `opts` — keyword options:
        * `:url` — Mongo URL (default: `Application.get_env(:rheo, :mongo_url)`)
        * `:name` — handle / process name (default: `default_handle/0`)
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
        |> Keyword.put_new(:name, default_handle())
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
    Default Mongo handle name for the `Rheo` instance.

    ## Examples

        iex> Rheo.Backend.Mongo.default_handle()
        Rheo.Mongo

    ## Returns

    A process name atom (default `Rheo.Mongo`).
    """
    @spec default_handle() :: atom()
    def default_handle, do: Application.get_env(:rheo, :topology, Rheo.Mongo)

    @doc false
    @deprecated "Use default_handle/0"
    def topology_name, do: default_handle()

    @impl true
    def capabilities do
      Rheo.Backend.Capabilities.new(%{
        durable: true,
        distributed: false,
        atomic_compare_and_set: true,
        notifications: false,
        change_feed: false,
        secondary_indexes: true,
        batch_writes: true,
        ordered_range_scan: true,
        replay: true,
        partitions: true,
        contiguous_frontier: true
      })
    end

    @impl true
    def ping(topo) do
      case client().command(topo, ping: 1) do
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
        {@events, %{"stream" => 1, "metadata.schema" => 1}, [name: "events_stream_schema"]},
        {@groups, %{"stream" => 1, "name" => 1}, [unique: true, name: "groups_stream_name"]},
        {@deliveries, %{"stream" => 1, "group" => 1, "event_id" => 1},
         [unique: true, name: "deliveries_stream_group_event"]},
        {@deliveries, %{"stream" => 1, "group" => 1, "partition" => 1, "sequence" => 1},
         [name: "deliveries_frontier"]},
        {@deliveries, %{"stream" => 1, "group" => 1, "status" => 1, "expires_at" => 1},
         [name: "deliveries_claim"]}
      ]
    end

    @impl true
    def create_stream(topo, stream, opts \\ []) when is_binary(stream) do
      partition_count = Keyword.get(opts, :partition_count, 1)

      if is_integer(partition_count) and partition_count >= 1 do
        doc = %{
          "name" => stream,
          "partition_count" => partition_count,
          "next_sequences" => string_partition_map(partition_count, 0),
          "created_at" => Clock.utc_now()
        }

        case client().insert_one(topo, @streams, doc) do
          {:ok, _} ->
            :ok = ensure_indexes(topo)
            Telemetry.execute([:rheo, :stream, :create], %{count: 1}, %{stream: stream})
            :ok

          {:error, reason} ->
            already_or_error(reason)
        end
      else
        {:error, :invalid_partition_count}
      end
    end

    @impl true
    def create_group(topo, stream, group, opts \\ [])
        when is_binary(stream) and is_binary(group) do
      with {:ok, stream_doc} <- fetch_stream(topo, stream),
           do: insert_group(topo, stream, group, stream_doc, opts)
    end

    defp insert_group(topo, stream, group, stream_doc, opts) do
      max_attempts =
        Keyword.get(opts, :max_attempts, Application.get_env(:rheo, :default_max_attempts, 5))

      partition_count = partition_count(stream_doc)

      with {:ok, cursors} <- resolve_group_start(topo, stream, partition_count, opts) do
        doc = %{
          "stream" => stream,
          "name" => group,
          "cursors" => cursors,
          "frontiers" => string_partition_map(partition_count, 0),
          "max_attempts" => max_attempts,
          "created_at" => Clock.utc_now()
        }

        case client().insert_one(topo, @groups, doc) do
          {:ok, _} ->
            Telemetry.execute([:rheo, :group, :create], %{count: 1}, %{
              stream: stream,
              group: group
            })

            :ok

          {:error, reason} ->
            already_or_error(reason)
        end
      end
    end

    defp resolve_group_start(topo, stream, partition_count, opts) do
      with {:ok, selected} <- resolve_assignment(opts, partition_count) do
        cursors = string_partition_map(partition_count, 1)

        cond do
          Keyword.has_key?(opts, :start_after) ->
            start_after = Keyword.fetch!(opts, :start_after)

            {:ok,
             Enum.reduce(selected, cursors, fn partition, acc ->
               case start_after_for_partition(start_after, partition) do
                 sequence when is_integer(sequence) ->
                   Map.put(acc, Partition.key(partition), sequence + 1)

                 _ ->
                   acc
               end
             end)}

          Keyword.has_key?(opts, :start_at) ->
            datetime = Keyword.fetch!(opts, :start_at)

            {:ok,
             Enum.reduce(selected, cursors, fn partition, acc ->
               Map.put(
                 acc,
                 Partition.key(partition),
                 first_sequence_at(topo, stream, partition, datetime)
               )
             end)}

          true ->
            {:ok, cursors}
        end
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
      with {:ok, stream_doc} <- fetch_stream(topo, stream),
           {:ok, routed} <-
             resolve_payload_partitions(payloads, opts, partition_count(stream_doc)),
           {:ok, starts} <- allocate_partition_sequences(topo, stream, stream_doc, routed) do
        insert_event_docs(topo, stream, routed, starts, opts)
      end
    end

    defp insert_event_docs(topo, stream, routed, starts, opts) do
      now = Clock.utc_now()

      {docs, _offsets} =
        Enum.map_reduce(routed, %{}, fn {payload, partition}, offsets ->
          offset = Map.get(offsets, partition, 0)
          sequence = Map.fetch!(starts, partition) + offset
          doc = build_event_doc(stream, partition, sequence, payload, now, opts)
          {doc, Map.put(offsets, partition, offset + 1)}
        end)

      case client().insert_many(topo, @events, docs, ordered: true) do
        {:ok, _} -> {:ok, Enum.map(docs, &Codec.event_from_doc/1)}
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
        |> then(&client().find(&1, @events, filter, sort: %{"sequence" => 1}, limit: limit))
        |> Enum.map(&Codec.event_from_doc/1)

      {:ok, events}
    end

    @impl true
    def query(topo, %Query{} = query) do
      Telemetry.span([:rheo, :query], %{stream: query.stream}, fn ->
        query = Query.apply_cursor(query)
        filter = build_query_filter(query)
        sort = order_to_sort(query.order_by)

        events =
          topo
          |> then(&client().find(&1, @events, filter, sort: sort, limit: query.limit))
          |> Enum.map(&Codec.event_from_doc/1)

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
      with {:ok, _} <- fetch_group(topo, stream, group),
           {:ok, stream_doc} <- fetch_stream(topo, stream),
           {:ok, partitions} <- resolve_assignment(opts, partition_count(stream_doc)) do
        limit = Keyword.get(opts, :limit, Application.get_env(:rheo, :default_max_demand, 10))
        consumer_id = Keyword.get_lazy(opts, :consumer_id, &Id.generate/0)

        lease_ms =
          Keyword.get(opts, :lease_ms, Application.get_env(:rheo, :default_lease_ms, 30_000))

        now = Clock.utc_now()

        ctx = %{
          topo: topo,
          stream: stream,
          group: group,
          partitions: partitions,
          consumer_id: consumer_id,
          lease_ms: lease_ms,
          limit: limit,
          now: now
        }

        case fetch_until(ctx, []) do
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

    defp fetch_until(%{limit: limit}, acc) when length(acc) >= limit do
      {:ok, acc}
    end

    defp fetch_until(ctx, acc) do
      %{
        topo: topo,
        stream: stream,
        group: group,
        partitions: partitions,
        consumer_id: consumer_id,
        lease_ms: lease_ms,
        limit: limit,
        now: now
      } = ctx

      remaining = limit - length(acc)

      with {:ok, group_doc} <- fetch_group(topo, stream, group),
           :ok <-
             materialize_deliveries(
               topo,
               stream,
               group,
               group_doc,
               partitions,
               max(remaining, 50)
             ),
           {:ok, batch} <-
             claim_deliveries(
               topo,
               stream,
               group,
               partitions,
               consumer_id,
               lease_ms,
               remaining,
               now
             ) do
        if batch == [] do
          {:ok, acc}
        else
          fetch_until(ctx, acc ++ batch)
        end
      end
    end

    @impl true
    def renew(topo, %Lease{} = lease, opts \\ []) do
      Telemetry.span([:rheo, :lease, :renew], %{stream: lease.stream, group: lease.group}, fn ->
        lease_ms =
          Keyword.get(opts, :lease_ms, Application.get_env(:rheo, :default_lease_ms, 30_000))

        now = Clock.utc_now()
        expires_at = DateTime.add(now, lease_ms, :millisecond)

        update = %{
          "$set" => %{
            "expires_at" => expires_at,
            "renewed_at" => now
          }
        }

        case client().find_one_and_update(topo, @deliveries, lease_filter(lease), update,
               return_document: :after
             ) do
          {:ok, %Mongo.FindAndModifyResult{value: nil}} ->
            {:error, :stale_lease}

          {:ok, %Mongo.FindAndModifyResult{value: _}} ->
            {:ok, %{lease | expires_at: expires_at}}

          {:error, reason} ->
            {:error, map_backend_error(reason)}
        end
      end)
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

        with :ok <- mutate_lease(topo, lease, update) do
          advance_frontier(topo, lease)
          :ok
        end
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

            advance_frontier(topo, lease)
            :ok

          error ->
            error
        end
      end)
    end

    @impl true
    def replay(topo, stream, group, opts \\ []) do
      with {:ok, group_doc} <- fetch_group(topo, stream, group),
           {:ok, stream_doc} <- fetch_stream(topo, stream),
           {:ok, partitions} <- resolve_assignment(opts, partition_count(stream_doc)) do
        cond do
          events = Keyword.get(opts, :events) ->
            selected = Enum.filter(events, &(&1.partition in partitions))

            with :ok <- reopen_deliveries_with_events(topo, stream, group, selected) do
              rewind_frontiers_for_events(topo, stream, group, group_doc, selected)
            end

          ids = Keyword.get(opts, :event_ids) ->
            reopen_deliveries(topo, stream, group, group_doc, ids, partitions)

          Keyword.has_key?(opts, :from_sequence) ->
            from_seq = Keyword.fetch!(opts, :from_sequence)
            reopen_from_sequence(topo, stream, group, group_doc, from_seq + 1, partitions)

          true ->
            {:error, :invalid_replay_opts}
        end
      end
    end

    @impl true
    def reset_group(topo, stream, group, opts \\ []) do
      with {:ok, group_doc} <- fetch_group(topo, stream, group),
           {:ok, stream_doc} <- fetch_stream(topo, stream),
           {:ok, partitions} <- resolve_assignment(opts, partition_count(stream_doc)),
           {:ok, starts} <-
             resolve_group_start(topo, stream, partition_count(stream_doc), opts) do
        filter = %{
          "stream" => stream,
          "group" => group,
          "partition" => %{"$in" => partitions}
        }

        case client().delete_many(topo, @deliveries, filter) do
          {:ok, _} ->
            cursors =
              Enum.reduce(partitions, group_cursors(group_doc), fn partition, acc ->
                Map.put(acc, Partition.key(partition), Partition.map_get(starts, partition, 1))
              end)

            frontiers =
              Enum.reduce(partitions, group_frontiers(group_doc), fn partition, acc ->
                Map.put(acc, Partition.key(partition), 0)
              end)

            case client().find_one_and_update(
                   topo,
                   @groups,
                   %{"stream" => stream, "name" => group},
                   %{"$set" => %{"cursors" => cursors, "frontiers" => frontiers}},
                   return_document: :after
                 ) do
              {:ok, _} -> :ok
              {:error, reason} -> {:error, map_backend_error(reason)}
            end

          {:error, reason} ->
            {:error, map_backend_error(reason)}
        end
      end
    end

    @impl true
    def lag(topo, stream, group, opts \\ []) do
      with {:ok, group_doc} <- fetch_group(topo, stream, group),
           {:ok, stream_doc} <- fetch_stream(topo, stream),
           {:ok, partitions} <- resolve_assignment(opts, partition_count(stream_doc)) do
        {:ok,
         Lag.from_maps(
           stream,
           group,
           group_frontiers(group_doc),
           stream_next_sequences(stream_doc),
           partitions
         )}
      end
    end

    @impl true
    def list_streams(topo, _opts \\ []) do
      names =
        topo
        |> client().find(@streams, %{}, sort: %{"name" => 1}, projection: %{"name" => 1})
        |> Enum.map(fn doc -> doc["name"] || doc[:name] end)
        |> Enum.reject(&is_nil/1)

      {:ok, names}
    rescue
      error -> {:error, map_backend_error(error)}
    end

    @impl true
    def list_groups(topo, stream, _opts \\ []) do
      with {:ok, _} <- fetch_stream(topo, stream) do
        names =
          topo
          |> client().find(@groups, %{"stream" => stream},
            sort: %{"name" => 1},
            projection: %{"name" => 1}
          )
          |> Enum.map(fn doc -> doc["name"] || doc[:name] end)
          |> Enum.reject(&is_nil/1)

        {:ok, names}
      end
    rescue
      error -> {:error, map_backend_error(error)}
    end

    @impl true
    def dead_letters(topo, stream, group, opts \\ []) do
      with {:ok, _} <- fetch_group(topo, stream, group) do
        limit = Keyword.get(opts, :limit, 100)
        after_id = Keyword.get(opts, :after)

        docs =
          topo
          |> client().find(
            @deliveries,
            %{"stream" => stream, "group" => group, "status" => "rejected"},
            sort: %{"sequence" => 1, "event_id" => 1}
          )
          |> Enum.to_list()
          |> drop_after_event_id(after_id)
          |> Enum.take(limit)

        {:ok, Enum.map(docs, &mongo_dead_letter(topo, &1))}
      end
    rescue
      error -> {:error, map_backend_error(error)}
    end

    @impl true
    def group_info(topo, stream, group, opts \\ []) do
      with {:ok, lag} <- lag(topo, stream, group, opts) do
        inflight =
          count_deliveries(topo, stream, group, "leased")

        dead =
          count_deliveries(topo, stream, group, "rejected")

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

    defp count_deliveries(topo, stream, group, status) do
      topo
      |> client().find(
        @deliveries,
        %{"stream" => stream, "group" => group, "status" => status},
        projection: %{"_id" => 1}
      )
      |> Enum.count()
    rescue
      _ -> 0
    end

    defp mongo_dead_letter(topo, delivery) do
      event_id = doc_get(delivery, "event_id")

      %DeadLetter{
        stream: doc_get(delivery, "stream"),
        group: doc_get(delivery, "group"),
        event_id: event_id,
        partition: doc_get(delivery, "partition") || 0,
        sequence: doc_get(delivery, "sequence"),
        reason: doc_get(delivery, "reason"),
        dead_lettered_at: doc_get(delivery, "dead_lettered_at"),
        event: load_event(topo, event_id)
      }
    end

    defp load_event(topo, event_id) do
      case client().find_one(topo, @events, %{"_id" => event_id}) do
        nil -> nil
        doc -> Codec.event_from_doc(doc)
      end
    end

    defp doc_get(doc, key) when is_map(doc) do
      case Map.fetch(doc, key) do
        {:ok, value} -> value
        :error -> Map.get(doc, atom_field(key))
      end
    end

    defp atom_field("event_id"), do: :event_id
    defp atom_field("stream"), do: :stream
    defp atom_field("group"), do: :group
    defp atom_field("partition"), do: :partition
    defp atom_field("sequence"), do: :sequence
    defp atom_field("reason"), do: :reason
    defp atom_field("dead_lettered_at"), do: :dead_lettered_at
    defp atom_field(other), do: other

    defp drop_after_event_id(docs, nil), do: docs

    defp drop_after_event_id(docs, after_id) when is_binary(after_id) do
      case Enum.find_index(docs, &(doc_get(&1, "event_id") == after_id)) do
        nil -> docs
        idx -> Enum.drop(docs, idx + 1)
      end
    end

    defp reopen_from_sequence(topo, stream, group, group_doc, next_sequence, partitions) do
      filter = %{
        "stream" => stream,
        "group" => group,
        "partition" => %{"$in" => partitions},
        "sequence" => %{"$gte" => next_sequence}
      }

      update = %{
        "$set" => %{
          "status" => "available",
          "lease_id" => nil,
          "consumer_id" => nil,
          "expires_at" => nil,
          "reason" => "replay",
          "retried_at" => Clock.utc_now()
        }
      }

      case client().update_many(topo, @deliveries, filter, update, []) do
        {:ok, _} ->
          cursors = put_partitions(group_cursors(group_doc), partitions, next_sequence)

          frontiers =
            Enum.reduce(partitions, group_frontiers(group_doc), fn partition, acc ->
              replay_frontier = max(next_sequence - 1, 0)
              old = Partition.map_get(acc, partition, 0)
              Map.put(acc, Partition.key(partition), min(old, replay_frontier))
            end)

          update_group_maps(topo, stream, group, cursors, frontiers)

        {:error, reason} ->
          {:error, map_backend_error(reason)}
      end
    end

    defp reopen_deliveries(_topo, _stream, _group, _group_doc, [], _partitions), do: :ok

    defp reopen_deliveries(topo, stream, group, group_doc, event_ids, partitions)
         when is_list(event_ids) do
      filter = %{
        "stream" => stream,
        "group" => group,
        "event_id" => %{"$in" => event_ids},
        "partition" => %{"$in" => partitions}
      }

      deliveries =
        Enum.to_list(client().find(topo, @deliveries, filter, sort: %{"partition" => 1}))

      update = %{
        "$set" => %{
          "status" => "available",
          "lease_id" => nil,
          "consumer_id" => nil,
          "expires_at" => nil,
          "reason" => "replay",
          "retried_at" => Clock.utc_now()
        }
      }

      case client().update_many(topo, @deliveries, filter, update, []) do
        {:ok, _} -> rewind_frontiers_for_deliveries(topo, stream, group, group_doc, deliveries)
        {:error, reason} -> {:error, map_backend_error(reason)}
      end
    end

    # When full event structs are provided, upsert missing delivery rows as available.
    defp reopen_deliveries_with_events(topo, stream, group, events) when is_list(events) do
      Enum.reduce_while(events, :ok, fn event, :ok ->
        filter = %{"stream" => stream, "group" => group, "event_id" => event.id}

        update = %{
          "$set" => %{
            "status" => "available",
            "lease_id" => nil,
            "consumer_id" => nil,
            "expires_at" => nil,
            "reason" => "replay",
            "retried_at" => Clock.utc_now(),
            "partition" => event.partition,
            "sequence" => event.sequence
          },
          "$setOnInsert" => %{
            "stream" => stream,
            "group" => group,
            "event_id" => event.id,
            "attempt" => 0,
            "created_at" => Clock.utc_now()
          }
        }

        case client().find_one_and_update(topo, @deliveries, filter, update,
               upsert: true,
               return_document: :after
             ) do
          {:ok, _} -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, map_backend_error(reason)}}
        end
      end)
    end

    defp mutate_lease(topo, lease, update) do
      case client().find_one_and_update(topo, @deliveries, lease_filter(lease), update,
             return_document: :after
           ) do
        {:ok, %Mongo.FindAndModifyResult{value: nil}} -> {:error, :stale_lease}
        {:ok, %Mongo.FindAndModifyResult{value: _}} -> :ok
        {:error, reason} -> {:error, map_backend_error(reason)}
      end
    end

    defp advance_frontier(topo, %Lease{} = lease) do
      partition = lease.event.partition

      case fetch_group(topo, lease.stream, lease.group) do
        {:ok, group_doc} ->
          old_frontier = Partition.map_get(group_frontiers(group_doc), partition, 0)
          frontier = walk_frontier(topo, lease.stream, lease.group, partition, old_frontier)

          if frontier > old_frontier do
            path = "frontiers.#{Partition.key(partition)}"

            case client().find_one_and_update(
                   topo,
                   @groups,
                   %{"stream" => lease.stream, "name" => lease.group},
                   %{"$max" => %{path => frontier}},
                   return_document: :after
                 ) do
              {:ok, _} ->
                Telemetry.execute(
                  [:rheo, :group, :frontier],
                  %{frontier: frontier},
                  %{
                    stream: lease.stream,
                    group: lease.group,
                    partition: partition,
                    frontier: frontier
                  }
                )

              {:error, _reason} ->
                :ok
            end
          end

        {:error, _reason} ->
          :ok
      end
    end

    defp walk_frontier(topo, stream, group, partition, frontier) do
      next = frontier + 1

      case client().find_one(topo, @deliveries, %{
             "stream" => stream,
             "group" => group,
             "partition" => partition,
             "sequence" => next,
             "status" => %{"$in" => ["acked", "rejected"]}
           }) do
        nil -> frontier
        _delivery -> walk_frontier(topo, stream, group, partition, next)
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

    defp rewind_frontiers_for_events(_topo, _stream, _group, _group_doc, []), do: :ok

    defp rewind_frontiers_for_events(topo, stream, group, group_doc, events) do
      frontiers =
        Enum.reduce(events, group_frontiers(group_doc), fn event, acc ->
          old = Partition.map_get(acc, event.partition, 0)
          Map.put(acc, Partition.key(event.partition), min(old, max(event.sequence - 1, 0)))
        end)

      update_group_frontiers(topo, stream, group, frontiers)
    end

    defp rewind_frontiers_for_deliveries(topo, stream, group, group_doc, deliveries) do
      frontiers =
        Enum.reduce(deliveries, group_frontiers(group_doc), fn delivery, acc ->
          partition = delivery["partition"] || delivery[:partition] || 0
          sequence = delivery["sequence"] || delivery[:sequence]
          old = Partition.map_get(acc, partition, 0)

          if is_integer(sequence) do
            Map.put(acc, Partition.key(partition), min(old, max(sequence - 1, 0)))
          else
            acc
          end
        end)

      update_group_frontiers(topo, stream, group, frontiers)
    end

    defp update_group_frontiers(topo, stream, group, frontiers) do
      case client().find_one_and_update(
             topo,
             @groups,
             %{"stream" => stream, "name" => group},
             %{"$set" => %{"frontiers" => frontiers}},
             return_document: :after
           ) do
        {:ok, _} -> :ok
        {:error, reason} -> {:error, map_backend_error(reason)}
      end
    end

    defp update_group_maps(topo, stream, group, cursors, frontiers) do
      case client().find_one_and_update(
             topo,
             @groups,
             %{"stream" => stream, "name" => group},
             %{"$set" => %{"cursors" => cursors, "frontiers" => frontiers}},
             return_document: :after
           ) do
        {:ok, _} -> :ok
        {:error, reason} -> {:error, map_backend_error(reason)}
      end
    end

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

    defp allocate_partition_sequences(topo, stream, stream_doc, routed) do
      counts = Enum.frequencies_by(routed, &elem(&1, 1))

      counts
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.reduce_while({:ok, %{}}, fn {partition, count}, {:ok, starts} ->
        case allocate_sequences(topo, stream, stream_doc, partition, count) do
          {:ok, start} -> {:cont, {:ok, Map.put(starts, partition, start)}}
          {:error, _} = error -> {:halt, error}
        end
      end)
    end

    defp allocate_sequences(topo, stream, stream_doc, partition, count) do
      field = sequence_field(stream_doc, partition)

      case client().find_one_and_update(
             topo,
             @streams,
             %{"name" => stream},
             %{"$inc" => %{field => count}},
             return_document: :after
           ) do
        {:ok, result} -> sequences_from_result(result, count, partition, field)
        {:error, reason} -> {:error, reason}
      end
    end

    defp sequences_from_result(result, count, partition, field) do
      case get_value(result) do
        doc when is_map(doc) ->
          next =
            if field == "next_sequence",
              do: doc["next_sequence"] || doc[:next_sequence],
              else:
                Partition.map_get(
                  doc["next_sequences"] || doc[:next_sequences] || %{},
                  partition,
                  0
                )

          if is_integer(next), do: {:ok, next - count + 1}, else: {:error, :stream_not_found}

        nil ->
          {:error, :stream_not_found}
      end
    end

    defp sequence_field(stream_doc, 0) do
      sequences = stream_doc["next_sequences"] || stream_doc[:next_sequences]
      legacy = stream_doc["next_sequence"] || stream_doc[:next_sequence]

      cond do
        is_map(sequences) and
            (Map.has_key?(sequences, "0") or Map.has_key?(sequences, 0)) ->
          "next_sequences.0"

        is_integer(legacy) ->
          "next_sequence"

        true ->
          "next_sequences.0"
      end
    end

    defp sequence_field(_stream_doc, partition), do: "next_sequences.#{Partition.key(partition)}"

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

    defp materialize_deliveries(topo, stream, group, group_doc, partitions, limit) do
      Enum.reduce_while(partitions, :ok, fn partition, :ok ->
        next_sequence = Partition.map_get(group_cursors(group_doc), partition, 1)

        {:ok, events} =
          read(topo, stream, partition: partition, after: next_sequence - 1, limit: limit)

        case insert_delivery_docs(topo, stream, group, partition, events) do
          :ok -> {:cont, :ok}
          {:error, _} = error -> {:halt, error}
        end
      end)
    end

    defp insert_delivery_docs(_topo, _stream, _group, _partition, []), do: :ok

    defp insert_delivery_docs(topo, stream, group, partition, events) do
      docs = Enum.map(events, &delivery_doc(stream, group, &1))

      with :ok <- ignore_duplicate(client().insert_many(topo, @deliveries, docs, ordered: false)) do
        last_seq = List.last(events).sequence
        path = "cursors.#{Partition.key(partition)}"

        case client().find_one_and_update(
               topo,
               @groups,
               %{"stream" => stream, "name" => group},
               %{"$max" => %{path => last_seq + 1}},
               return_document: :after
             ) do
          {:ok, _} -> :ok
          {:error, reason} -> {:error, map_backend_error(reason)}
        end
      end
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

    defp claim_deliveries(
           topo,
           stream,
           group,
           partitions,
           consumer_id,
           lease_ms,
           limit,
           now
         ) do
      ctx = %{
        topo: topo,
        stream: stream,
        group: group,
        partitions: partitions,
        consumer_id: consumer_id,
        lease_ms: lease_ms,
        now: now
      }

      claim_loop(ctx, limit, [])
    end

    defp claim_loop(_ctx, 0, acc) do
      {:ok, Enum.reverse(acc)}
    end

    defp claim_loop(ctx, remaining, acc) do
      %{
        topo: topo,
        stream: stream,
        group: group,
        partitions: partitions,
        consumer_id: consumer_id,
        lease_ms: lease_ms,
        now: now
      } = ctx

      case claim_one(topo, stream, group, partitions, consumer_id, lease_ms, now) do
        {:ok, nil} ->
          {:ok, Enum.reverse(acc)}

        {:ok, lease} ->
          claim_loop(ctx, remaining - 1, [lease | acc])

        {:error, reason} ->
          {:error, reason}
      end
    end

    defp claim_one(topo, stream, group, partitions, consumer_id, lease_ms, now) do
      lease_id = Id.generate()
      expires_at = DateTime.add(now, lease_ms, :millisecond)

      filter = %{
        "stream" => stream,
        "group" => group,
        "partition" => %{"$in" => partitions},
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

      case client().find_one_and_update(topo, @deliveries, filter, update,
             sort: %{"partition" => 1, "sequence" => 1},
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

      case client().find_one(topo, @events, %{"_id" => event_id}) do
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
             event: Codec.event_from_doc(event_doc),
             consumer_id: consumer_id,
             attempt: attempt,
             leased_at: now,
             expires_at: expires_at,
             receipt: lease_id
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
      case client().find_one(topo, @streams, %{"name" => stream}) do
        nil -> {:error, :stream_not_found}
        doc -> {:ok, doc}
      end
    end

    defp fetch_group(topo, stream, group) do
      case client().find_one(topo, @groups, %{"stream" => stream, "name" => group}) do
        nil -> {:error, :group_not_found}
        doc -> {:ok, doc}
      end
    end

    defp partition_count(doc), do: doc["partition_count"] || doc[:partition_count] || 1

    defp stream_next_sequences(doc) do
      case doc["next_sequences"] || doc[:next_sequences] do
        map when is_map(map) -> with_legacy_partition_zero(map, doc, 0)
        _ -> %{"0" => doc["next_sequence"] || doc[:next_sequence] || 0}
      end
    end

    defp group_cursors(doc) do
      case doc["cursors"] || doc[:cursors] do
        map when is_map(map) -> with_legacy_partition_zero(map, doc, 1)
        _ -> %{"0" => doc["next_sequence"] || doc[:next_sequence] || 1}
      end
    end

    defp with_legacy_partition_zero(map, doc, default) do
      if Map.has_key?(map, "0") or Map.has_key?(map, 0) do
        map
      else
        legacy = doc["next_sequence"] || doc[:next_sequence] || default
        Map.put(map, "0", legacy)
      end
    end

    defp group_frontiers(doc) do
      case doc["frontiers"] || doc[:frontiers] do
        map when is_map(map) -> map
        _ -> %{}
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

    defp string_partition_map(partition_count, value) do
      Map.new(0..(partition_count - 1), fn partition -> {Partition.key(partition), value} end)
    end

    defp put_partitions(map, partitions, value) do
      Enum.reduce(partitions, map, fn partition, acc ->
        Map.put(acc, Partition.key(partition), value)
      end)
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

    defp start_after_for_partition(_, _), do: nil

    defp first_sequence_at(topo, stream, partition, datetime) do
      filter = %{
        "stream" => stream,
        "partition" => partition,
        "timestamp" => %{"$gte" => datetime}
      }

      topo
      |> then(&client().find(&1, @events, filter, sort: %{"sequence" => 1}, limit: 1))
      |> Enum.find_value(1, fn doc -> doc["sequence"] || doc[:sequence] end)
    end

    defp build_query_filter(%Query{} = query) do
      base = %{"stream" => query.stream}

      base
      |> then(fn acc -> Enum.reduce(query.where, acc, &apply_where_opt/2) end)
      |> then(fn acc ->
        if query.from, do: put_in_range(acc, "timestamp", "$gte", query.from), else: acc
      end)
      |> then(fn acc ->
        if query.to, do: put_in_range(acc, "timestamp", "$lte", query.to), else: acc
      end)
      |> then(fn acc ->
        if query.after_sequence,
          do: put_in_range(acc, "sequence", "$gt", query.after_sequence),
          else: acc
      end)
      |> then(fn acc ->
        if query.until_sequence,
          do: put_in_range(acc, "sequence", "$lte", query.until_sequence),
          else: acc
      end)
    end

    defp apply_where_opt({:type, type}, acc), do: Map.put(acc, "type", type)
    defp apply_where_opt({:key, key}, acc), do: Map.put(acc, "key", key)
    defp apply_where_opt({:partition, p}, acc), do: Map.put(acc, "partition", p)
    defp apply_where_opt({:currency, c}, acc), do: Map.put(acc, "payload.currency", c)
    defp apply_where_opt({:curve, c}, acc), do: Map.put(acc, "payload.curve", c)

    defp apply_where_opt({:correlation_id, id}, acc),
      do: Map.put(acc, "metadata.correlation_id", id)

    defp apply_where_opt({:causation_id, id}, acc),
      do: Map.put(acc, "metadata.causation_id", id)

    defp apply_where_opt({:producer, p}, acc), do: Map.put(acc, "metadata.producer", p)

    defp apply_where_opt({:schema, s}, acc), do: Map.put(acc, "metadata.schema", s)

    defp apply_where_opt({:schema_version, v}, acc),
      do: Map.put(acc, "metadata.schema_version", v)

    defp apply_where_opt({field, value}, acc) when is_atom(field),
      do: Map.put(acc, "payload.#{field}", value)

    defp apply_where_opt(_, acc), do: acc

    defp order_to_sort([]) do
      %{"sequence" => 1}
    end

    defp order_to_sort(order_by) when is_list(order_by) do
      Map.new(order_by, fn
        {field, :asc} -> {Atom.to_string(field), 1}
        {field, :desc} -> {Atom.to_string(field), -1}
      end)
    end

    defp put_in_range(acc, field, op, value) do
      existing = Map.get(acc, field, %{})
      Map.put(acc, field, Map.put(existing, op, value))
    end

    defp map_backend_error(%DBConnection.ConnectionError{}), do: :backend_unavailable

    defp map_backend_error(%Mongo.Error{code: code}) when code in [6, 7, 89],
      do: :backend_unavailable

    defp map_backend_error(reason) when is_atom(reason), do: reason
    defp map_backend_error(reason), do: {:failed, reason}

    defp create_index(topo, coll, keys, opts) do
      index = [
        key: keys,
        name: Keyword.fetch!(opts, :name),
        unique: Keyword.get(opts, :unique, false)
      ]

      case client().create_indexes(topo, coll, [index]) do
        :ok -> :ok
        {:ok, _} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end

    defp client, do: Client.current()

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

    # Reasons are diagnostics, not payloads: keep them short and never store
    # handler terms verbatim.
    defp encode_reason(reason) when is_binary(reason), do: String.slice(reason, 0, 1_000)
    defp encode_reason(reason), do: reason |> inspect(limit: 50) |> String.slice(0, 1_000)

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
end
