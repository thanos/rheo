defmodule Rheo do
  @moduledoc """
  Rheo provides durable, searchable, replayable consumer-group semantics over
  storage systems.

  Events are immutable. Consumption records progress, leases, retries, and
  dead-letters in separate consumer-group state. Delivery is at-least-once:
  after a crash or lease expiry an event may be delivered again, so handlers
  must be idempotent on `Rheo.Event.id`.

  Streams may have several partitions. Sequences and ordering are per
  partition, key routing uses `:erlang.phash2/2`, and group progress is a
  contiguous ACK frontier (`Rheo.lag/3`). There is no global order across
  partitions.

  ## Backends

  A Rheo instance runs on one `Rheo.Backend`:

    * `Rheo.Backend.ETS` — in-memory, ephemeral; always available
    * `Rheo.Backend.Mongo` — MongoDB; requires `mongodb_driver`
    * `Rheo.Backend.Ecto` — PostgreSQL or SQLite on a host-owned `Ecto.Repo`;
      requires `ecto_sql` and the repo's adapter

  Integration modules compile only when their dependency is present
  (ADR 020).

  ## Supervision

      children = [
        {Rheo, name: MyRheo, backend: {Rheo.Backend.Mongo, url: "mongodb://localhost:27017/rheo"}},
        {MyApp.RiskConsumer, rheo: MyRheo, concurrency: 8, max_demand: 100}
      ]

      Supervisor.start_link(children, strategy: :one_for_one)

  Ecto (the host owns the repo):

      children = [
        MyApp.Repo,
        {Rheo, name: MyRheo, backend: {Rheo.Backend.Ecto, repo: MyApp.Repo}},
        {MyApp.RiskConsumer, rheo: MyRheo, concurrency: 8, max_demand: 100}
      ]

  Public APIs accept an optional `:rheo` option targeting a named instance
  (default `Rheo`).

  ## Typical low-level flow

      Rheo.create_stream("market-events", partition_count: 4)
      {:ok, event} = Rheo.append("market-events", %{type: "curve_update", key: "EUR-1"})
      Rheo.create_group("market-events", "risk")
      {:ok, leases} = Rheo.fetch("market-events", "risk", limit: 10)
      :ok = Rheo.ack(hd(leases))
      {:ok, _} = Rheo.query("market-events", type: "curve_update")
      {:ok, lag} = Rheo.lag("market-events", "risk")

  Search history with `query/2`, `query_page/2`, and `stream_query/2`. Replay
  without copying events via `create_group/3` start cursors, `replay/3`, or
  `reset_group/3` (`confirm: true`). See `Rheo.Event.Lineage` for correlation
  metadata and `Rheo.Partition` for routing helpers.

  See `Rheo.Consumer` for the OTP handler API, `Rheo.Producer` and
  `Rheo.Broadway` for the GenStage/Broadway surface, and `Rheo.Backend` for
  adapters.

  ## Ops (v0.10+)

  Inventory and health without a second settle path: `list_streams/1`,
  `list_groups/2`, `dead_letters/3`, `group_info/3`, plus Mix inspect tasks.
  Optional `Rheo.LiveDashboard.Page` when `phoenix_live_dashboard` is present
  (ADR 027). See the [ops guide](ops.html).
  """

  use Supervisor

  alias Rheo.{Event, Instance, Lease, Query, Telemetry}

  @typedoc "Name of an event stream."
  @type stream :: String.t()

  @typedoc "Name of a consumer group on a stream."
  @type group :: String.t()

  @doc """
  Starts a Rheo instance supervisor.

  ## Options

    * `:name` — instance name (default `Rheo`). Also used as the supervisor name.
    * `:backend` — `{module, opts}` or a module. Required unless `:url` is
      given, in which case `Rheo.Backend.Mongo` is used with the remaining
      options (`:url`, `:pool_size`) when that module is available.

  ## Examples

      iex> name = String.to_atom("rheo_doc_#{System.unique_integer([:positive])}")
      iex> url = System.get_env("RHEO_MONGO_URL", "mongodb://localhost:27017/rheo_test")
      iex> {:ok, pid} = Rheo.start_link(name: name, backend: {Rheo.Backend.Mongo, url: url})
      iex> is_pid(pid)
      true

  ## Returns

    * `{:ok, pid}` on success
    * `{:error, {:already_started, pid}}` if the instance name is taken
    * `{:error, reason}` on backend start failure

  ## Errors

  Raises `ArgumentError` when no `:backend` is given and the Mongo shorthand
  cannot be used.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    # Resolved here so a configuration error raises in the caller.
    backend = resolve_backend_opts(opts)
    Supervisor.start_link(__MODULE__, {name, backend}, name: name)
  end

  @impl true
  def init({rheo, {backend_mod, backend_opts}}) do
    handle = Keyword.get(backend_opts, :name) || default_backend_handle(rheo, backend_mod)
    backend_opts = Keyword.put(backend_opts, :name, handle)

    children = [
      backend_mod.child_spec(backend_opts),
      {Instance, name: rheo, backend: backend_mod, handle: handle},
      {Task.Supervisor, name: Rheo.Names.task_supervisor(rheo)},
      {Rheo.GroupSupervisor, rheo: rheo}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc false
  def child_spec(opts) do
    name = Keyword.get(opts, :name, __MODULE__)

    %{
      id: name,
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  @doc """
  Creates a named stream.

  ## Arguments

    * `stream` — unique stream name (`t:stream/0`)
    * `opts` — optional keyword list:
      * `:rheo` — instance name (default `Rheo`)
      * `:partition_count` — number of partitions (default `1`). Sequences are
        per-partition; key routing uses `:erlang.phash2/2` when `:key` is set
        on append (ADR 016).

  ## Examples

      iex> stream = "doc-create-" <> Integer.to_string(System.unique_integer([:positive]))
      iex> Rheo.create_stream(stream)
      :ok
      iex> Rheo.create_stream(stream)
      {:error, :already_exists}

  ## Returns

    * `:ok` when the stream is created
    * `{:error, :already_exists}` when the name is taken
    * `{:error, reason}` on backend failure
  """
  @spec create_stream(stream(), keyword()) :: :ok | {:error, term()}
  def create_stream(stream, opts \\ []) do
    {backend, handle, opts} = resolve(opts)
    backend.create_stream(handle, stream, opts)
  end

  @doc """
  Creates a consumer group on an existing stream.

  Groups consume independently: an ACK in `"risk"` does not ACK `"surveillance"`.

  ## Arguments

    * `stream` — existing stream name
    * `group` — consumer group name
    * `opts` — optional keyword list:
      * `:rheo` — instance name (default `Rheo`)
      * `:max_attempts` — attempts before dead-letter on retry (default from
        application env, typically `5`)
      * `:start_after` — exclusive sequence (integer for all selected partitions,
        or `%{partition => sequence}` map); group begins materializing after this
      * `:start_at` — `DateTime`; begin at the first event at/after this time
      * `:partition` / `:partitions` — limit start cursors to those partitions
        (default all)
  ## Examples

      iex> stream = "doc-group-" <> Integer.to_string(System.unique_integer([:positive]))
      iex> :ok = Rheo.create_stream(stream)
      iex> Rheo.create_group(stream, "risk")
      :ok
      iex> Rheo.create_group(stream, "risk")
      {:error, :already_exists}
      iex> Rheo.create_group("missing-stream", "risk")
      {:error, :stream_not_found}

  ## Returns

    * `:ok`
    * `{:error, :already_exists}`
    * `{:error, :stream_not_found}`
    * `{:error, reason}`
  """
  @spec create_group(stream(), group(), keyword()) :: :ok | {:error, term()}
  def create_group(stream, group, opts \\ []) do
    {backend, handle, opts} = resolve(opts)
    backend.create_group(handle, stream, group, opts)
  end

  @doc """
  Appends a single event to a stream.

  Allocates the next sequence number atomically and persists an immutable
  `%Rheo.Event{}`.

  ## Arguments

    * `stream` — existing stream name
    * `payload` — map body. Common keys:
      * `:type` / `"type"` — event type (also copied to `Event.type`)
      * `:key` / `"key"` — optional key
      * `:metadata` / `"metadata"` — merged into `Event.metadata`
      * remaining keys become `Event.payload`
    * `opts` — optional keyword list:
      * `:rheo` — instance name (default `Rheo`)
      * `:id` — explicit event id (default: generated)
      * `:key` — routing / payload key; hashed into a partition when `:partition` omitted
      * `:metadata` — extra metadata map
      * `:timestamp` — `DateTime.t()` (default: clock now)
      * `:partition` — explicit partition (overrides key routing)

  ## Examples

      iex> stream = "doc-append-" <> Integer.to_string(System.unique_integer([:positive]))
      iex> :ok = Rheo.create_stream(stream)
      iex> {:ok, event} = Rheo.append(stream, %{
      ...>   type: "curve_update",
      ...>   currency: "EUR",
      ...>   curve: "EUR-EURIBOR-6M",
      ...>   price: 2.913,
      ...>   metadata: %{correlation_id: "abc", producer: "pricing-v3"}
      ...> })
      iex> {event.sequence, event.type, event.payload["currency"], event.metadata["correlation_id"]}
      {1, "curve_update", "EUR", "abc"}
      iex> Rheo.append("no-such-stream", %{type: "x"})
      {:error, :stream_not_found}

  ## Returns

    * `{:ok, %Rheo.Event{}}`
    * `{:error, :stream_not_found}`
    * `{:error, reason}`
  """
  @spec append(stream(), map(), keyword()) :: {:ok, Event.t()} | {:error, term()}
  def append(stream, payload, opts \\ []) when is_map(payload) do
    {backend, handle, opts} = resolve(opts)
    backend.append(handle, stream, payload, opts)
  end

  @doc """
  Appends multiple events, allocating contiguous sequences.

  ## Arguments

    * `stream` — existing stream name
    * `payloads` — list of payload maps (same shape as `append/3`)
    * `opts` — shared options applied to each event (see `append/3`)

  ## Examples

      iex> stream = "doc-batch-" <> Integer.to_string(System.unique_integer([:positive]))
      iex> :ok = Rheo.create_stream(stream)
      iex> {:ok, events} = Rheo.append_batch(stream, [
      ...>   %{type: "tick", n: 1},
      ...>   %{type: "tick", n: 2},
      ...>   %{type: "tick", n: 3}
      ...> ])
      iex> Enum.map(events, & &1.sequence)
      [1, 2, 3]
      iex> Rheo.append_batch(stream, [])
      {:ok, []}

  ## Returns

    * `{:ok, [%Rheo.Event{}]}` — possibly empty
    * `{:error, :stream_not_found}`
    * `{:error, reason}`
  """
  @spec append_batch(stream(), [map()], keyword()) :: {:ok, [Event.t()]} | {:error, term()}
  def append_batch(stream, payloads, opts \\ []) when is_list(payloads) do
    {backend, handle, opts} = resolve(opts)
    backend.append_batch(handle, stream, payloads, opts)
  end

  @doc """
  Reads events by sequence without affecting consumer-group state.

  ## Arguments

    * `stream` — stream name
    * `opts` — optional keyword list:
      * `:rheo` — instance name (default `Rheo`)
      * `:after` — return sequences strictly greater than this (default `0`)
      * `:limit` — max events (default `100`)
      * `:partition` — partition (default `0`)

  ## Examples

      iex> stream = "doc-read-" <> Integer.to_string(System.unique_integer([:positive]))
      iex> :ok = Rheo.create_stream(stream)
      iex> {:ok, _} = Rheo.append_batch(stream, [%{type: "a"}, %{type: "b"}, %{type: "c"}])
      iex> {:ok, [first, second]} = Rheo.read(stream, after: 0, limit: 2)
      iex> {first.sequence, second.sequence}
      {1, 2}
      iex> {:ok, [third]} = Rheo.read(stream, after: 2, limit: 10)
      iex> third.type
      "c"

  ## Returns

    * `{:ok, [%Rheo.Event{}]}` — empty list when nothing matches
  """
  @spec read(stream(), keyword()) :: {:ok, [Event.t()]} | {:error, term()}
  def read(stream, opts \\ []) do
    {backend, handle, opts} = resolve(opts)
    backend.read(handle, stream, opts)
  end

  @doc """
  Queries historical events. Consumption never removes events from the log.

  Accepts a `%Rheo.Query{}` or a stream name plus keyword filters (converted via
  `Rheo.Query.new/2`).

  ## Arguments

    * `query_or_stream` — `%Rheo.Query{}` or stream name
    * `opts` — when the first argument is a stream, filter options (see
      `Rheo.Query`); always accepts `:rheo`

  ## Examples

      iex> stream = "doc-query-" <> Integer.to_string(System.unique_integer([:positive]))
      iex> :ok = Rheo.create_stream(stream)
      iex> {:ok, _} = Rheo.append(stream, %{
      ...>   type: "curve_update",
      ...>   currency: "EUR",
      ...>   curve: "EUR-EURIBOR-6M",
      ...>   price: 2.913
      ...> })
      iex> {:ok, _} = Rheo.append(stream, %{type: "curve_update", currency: "USD", curve: "USD-SOFR"})
      iex> {:ok, [event]} = Rheo.query(stream, type: "curve_update", currency: "EUR")
      iex> event.payload["curve"]
      "EUR-EURIBOR-6M"
      iex> q = Rheo.Query.new(stream, where: [type: "curve_update", currency: "USD"])
      iex> {:ok, [usd]} = Rheo.query(q)
      iex> usd.payload["currency"]
      "USD"

  ## Returns

    * `{:ok, [%Rheo.Event{}]}`
    * `{:error, reason}` on backend failure
  """
  @spec query(Query.t() | stream(), keyword()) :: {:ok, [Event.t()]} | {:error, term()}
  def query(query_or_stream, opts \\ [])

  def query(%Query{} = query, opts) when is_list(opts) do
    {backend, handle, _} = resolve(opts)
    backend.query(handle, query)
  end

  def query(stream, opts) when is_binary(stream) and is_list(opts) do
    {rheo_opts, query_opts} = Keyword.split(opts, [:rheo])
    query(Query.new(stream, query_opts), rheo_opts)
  end

  @doc """
  Queries one page of historical events.

  Returns `{:ok, %Rheo.Page{}}`. When more results may exist, `page.next_cursor`
  is set; pass it as `cursor:` on the next call (or embed on `%Rheo.Query{}`).
  Cursor pagination is defined for ascending sequence order.
  """
  @spec query_page(Query.t() | stream(), keyword()) :: {:ok, Rheo.Page.t()} | {:error, term()}
  def query_page(query_or_stream, opts \\ [])

  def query_page(%Query{} = query, opts) when is_list(opts) do
    {backend, handle, _} = resolve(opts)
    query = Query.apply_cursor(query)

    case backend.query(handle, query) do
      {:ok, events} ->
        next =
          if length(events) >= query.limit and events != [] do
            %{after_sequence: List.last(events).sequence}
          end

        {:ok, %Rheo.Page{events: events, next_cursor: next}}

      error ->
        error
    end
  end

  def query_page(stream, opts) when is_binary(stream) and is_list(opts) do
    {rheo_opts, query_opts} = Keyword.split(opts, [:rheo])
    query_page(Query.new(stream, query_opts), rheo_opts)
  end

  @doc """
  Lazily streams query results page by page.

  Each element is a `%Rheo.Event{}`. Uses `query_page/2` internally.
  """
  @spec stream_query(Query.t() | stream(), keyword()) :: Enumerable.t()
  def stream_query(query_or_stream, opts \\ []) do
    query =
      case query_or_stream do
        %Query{} = q -> q
        stream when is_binary(stream) -> Query.new(stream, Keyword.drop(opts, [:rheo]))
      end

    rheo_opts = Keyword.take(opts, [:rheo])

    Stream.resource(
      fn -> query end,
      fn
        :done ->
          {:halt, :done}

        %Query{} = q ->
          case query_page(q, rheo_opts) do
            {:ok, %Rheo.Page{events: [], next_cursor: _}} ->
              {:halt, :done}

            {:ok, %Rheo.Page{events: events, next_cursor: nil}} ->
              {events, :done}

            {:ok, %Rheo.Page{events: events, next_cursor: cursor}} ->
              {events, %{q | cursor: cursor, after_sequence: nil}}

            {:error, reason} ->
              raise "Rheo.stream_query failed: #{inspect(reason)}"
          end
      end,
      fn _ -> :ok end
    )
  end

  @doc """
  Replays history for a consumer group without copying events.

  Options (one of):

    * `:from_sequence` — exclusive lower bound; deliveries from the next sequence
      become available again
    * `:from` — `DateTime`; resolved to a sequence then same as `:from_sequence`
    * `:query` — `%Rheo.Query{}` or keyword filters on the stream; matching
      events are re-opened for this group

  See ADR 015. Prefer a new group with `:start_after` / `:start_at` when isolating
  replay from production consumers.
  """
  @spec replay(stream(), group(), keyword()) :: :ok | {:error, term()}
  def replay(stream, group, opts \\ []) when is_binary(stream) and is_binary(group) do
    {backend, handle, opts} = resolve(opts)
    scope = Keyword.take(opts, [:partition, :partitions])

    result =
      cond do
        Keyword.has_key?(opts, :from_sequence) ->
          backend.replay(
            handle,
            stream,
            group,
            [from_sequence: Keyword.fetch!(opts, :from_sequence)] ++ scope
          )

        Keyword.has_key?(opts, :from) ->
          with {:ok, seq} <-
                 sequence_at_or_after(backend, handle, stream, Keyword.fetch!(opts, :from)) do
            # exclusive: replay from just before that event
            backend.replay(handle, stream, group, [from_sequence: seq - 1] ++ scope)
          end

        Keyword.has_key?(opts, :query) ->
          replay_query(backend, handle, stream, group, Keyword.fetch!(opts, :query), opts)

        true ->
          {:error, :invalid_replay_opts}
      end

    case result do
      :ok ->
        Telemetry.execute([:rheo, :group, :replay], %{count: 1}, %{stream: stream, group: group})
        :ok

      other ->
        other
    end
  end

  @doc """
  Destructively clears deliveries for one group and resets its cursor.

  Requires `confirm: true`. Never deletes immutable events. Other groups are
  unaffected. Optional `:start_after` sets the post-reset materialization cursor
  (exclusive), default `0` (replay from the beginning).
  """
  @spec reset_group(stream(), group(), keyword()) :: :ok | {:error, term()}
  def reset_group(stream, group, opts \\ []) when is_binary(stream) and is_binary(group) do
    if Keyword.get(opts, :confirm) != true do
      {:error, :confirm_required}
    else
      {backend, handle, opts} = resolve(opts)

      case backend.reset_group(handle, stream, group, opts) do
        :ok ->
          Telemetry.execute([:rheo, :group, :reset], %{count: 1}, %{stream: stream, group: group})
          :ok

        error ->
          error
      end
    end
  end

  @doc """
  Returns contiguous-frontier lag for a consumer group (v0.5+).

  Per-partition lag is `max(high_watermark - frontier, 0)`. Aggregate `lag` is
  the sum across partitions. See `Rheo.Lag` and ADR 016.
  """
  @spec lag(stream(), group(), keyword()) :: {:ok, Rheo.Lag.t()} | {:error, term()}
  def lag(stream, group, opts \\ []) when is_binary(stream) and is_binary(group) do
    {backend, handle, opts} = resolve(opts)
    backend.lag(handle, stream, group, opts)
  end

  @doc """
  Lists registered stream names (ops inspect, v0.10+ / ADR 027).

  Returns `{:error, :unsupported}` when the backend does not implement
  `list_streams/2`.
  """
  @spec list_streams(keyword()) :: {:ok, [stream()]} | {:error, term()}
  def list_streams(opts \\ []) when is_list(opts) do
    {backend, handle, opts} = resolve(opts)
    dispatch_ops(backend, :list_streams, [handle, opts])
  end

  @doc """
  Lists consumer group names for a stream (ops inspect, v0.10+ / ADR 027).

  Returns `{:error, :unsupported}` when the backend does not implement
  `list_groups/3`.
  """
  @spec list_groups(stream(), keyword()) :: {:ok, [group()]} | {:error, term()}
  def list_groups(stream, opts \\ []) when is_binary(stream) and is_list(opts) do
    {backend, handle, opts} = resolve(opts)
    dispatch_ops(backend, :list_groups, [handle, stream, opts])
  end

  @doc """
  Lists dead-lettered deliveries for a group (ops inspect, v0.10+ / ADR 027).

  Options:

    * `:limit` — max rows (default 100)
    * `:after` — skip until after this `event_id` (cursor)
    * `:rheo` — instance name

  Returns `{:error, :unsupported}` when the backend does not implement
  `dead_letters/4`.
  """
  @spec dead_letters(stream(), group(), keyword()) ::
          {:ok, [Rheo.DeadLetter.t()]} | {:error, term()}
  def dead_letters(stream, group, opts \\ [])
      when is_binary(stream) and is_binary(group) and is_list(opts) do
    {backend, handle, opts} = resolve(opts)
    dispatch_ops(backend, :dead_letters, [handle, stream, group, opts])
  end

  @doc """
  Returns group health: lag plus inflight and dead-letter counts (v0.10+ / ADR 027).

  Returns `{:error, :unsupported}` when the backend does not implement
  `group_info/4`.
  """
  @spec group_info(stream(), group(), keyword()) ::
          {:ok, Rheo.GroupInfo.t()} | {:error, term()}
  def group_info(stream, group, opts \\ [])
      when is_binary(stream) and is_binary(group) and is_list(opts) do
    {backend, handle, opts} = resolve(opts)
    dispatch_ops(backend, :group_info, [handle, stream, group, opts])
  end

  defp dispatch_ops(backend, fun, args) do
    arity = length(args)

    if function_exported?(backend, fun, arity) do
      apply(backend, fun, args)
    else
      {:error, :unsupported}
    end
  end

  defp sequence_at_or_after(backend, handle, stream, %DateTime{} = dt) do
    case backend.query(handle, %Query{
           stream: stream,
           from: dt,
           order_by: [sequence: :asc],
           limit: 1
         }) do
      {:ok, [%{sequence: seq} | _]} -> {:ok, seq}
      {:ok, []} -> {:error, :no_events_in_range}
      error -> error
    end
  end

  defp replay_query(backend, handle, stream, group, %Query{} = query, _opts) do
    query = %{query | stream: stream, limit: max(query.limit, 10_000)}

    case backend.query(handle, query) do
      {:ok, events} ->
        backend.replay(handle, stream, group,
          event_ids: Enum.map(events, & &1.id),
          events: events
        )

      error ->
        error
    end
  end

  defp replay_query(backend, handle, stream, group, query_opts, opts)
       when is_list(query_opts) do
    replay_query(backend, handle, stream, group, Query.new(stream, query_opts), opts)
  end

  @doc """
  Fetches up to `:limit` events for a consumer group, creating leases.

  Competing workers in the same group receive distinct leases. Independent
  groups may lease the same events concurrently.

  ## Arguments

    * `stream` — stream name
    * `group` — consumer group name
    * `opts` — optional keyword list:
      * `:rheo` — instance name (default `Rheo`)
      * `:limit` — max leases / demand bound (default from config)
      * `:consumer_id` — worker identity (default: generated)
      * `:lease_ms` — lease TTL in milliseconds (default from config)

  ## Examples

      iex> stream = "doc-fetch-" <> Integer.to_string(System.unique_integer([:positive]))
      iex> :ok = Rheo.create_stream(stream)
      iex> :ok = Rheo.create_group(stream, "risk")
      iex> {:ok, _} = Rheo.append(stream, %{type: "order", id: 1})
      iex> {:ok, [lease]} = Rheo.fetch(stream, "risk", limit: 10, consumer_id: "c1")
      iex> {lease.group, lease.attempt, lease.event.type}
      {"risk", 1, "order"}
      iex> Rheo.fetch(stream, "missing", limit: 1)
      {:error, :group_not_found}

  ## Returns

    * `{:ok, [%Rheo.Lease{}]}` — empty when no eligible work
    * `{:error, :group_not_found}`
    * `{:error, reason}`
  """
  @spec fetch(stream(), group(), keyword()) :: {:ok, [Lease.t()]} | {:error, term()}
  def fetch(stream, group, opts \\ []) do
    {backend, handle, opts} = resolve(opts)
    backend.fetch(handle, stream, group, opts)
  end

  @doc """
  Extends an active lease when `lease_id` still matches.

  ## Arguments

    * `lease` — `%Rheo.Lease{}` from `fetch/3`
    * `opts` — optional keyword list:
      * `:rheo` — instance name (default `Rheo`)
      * `:lease_ms` — new TTL from now (default from config)

  ## Returns

    * `{:ok, %Rheo.Lease{}}` with updated `expires_at`
    * `{:error, :stale_lease}`
    * `{:error, :backend_unavailable}`
    * `{:error, reason}`
  """
  @spec renew(Lease.t(), keyword()) :: {:ok, Lease.t()} | {:error, term()}
  def renew(%Lease{} = lease, opts \\ []) do
    {backend, handle, opts} = resolve(opts)
    backend.renew(handle, lease, opts)
  end

  @doc """
  Acknowledges successful processing of a leased event.

  Durable for that consumer group only. Requires the current `lease_id`.

  ## Arguments

    * `lease` — `%Rheo.Lease{}` returned by `fetch/3`
    * `opts` — optional `:rheo` instance name

  ## Examples

      iex> stream = "doc-ack-" <> Integer.to_string(System.unique_integer([:positive]))
      iex> :ok = Rheo.create_stream(stream)
      iex> :ok = Rheo.create_group(stream, "risk")
      iex> {:ok, _} = Rheo.append(stream, %{type: "once"})
      iex> {:ok, [lease]} = Rheo.fetch(stream, "risk", limit: 1, consumer_id: "c1")
      iex> Rheo.ack(lease)
      :ok
      iex> Rheo.ack(lease)
      {:error, :stale_lease}
      iex> Rheo.fetch(stream, "risk", limit: 1, consumer_id: "c2")
      {:ok, []}

  ## Returns

    * `:ok`
    * `{:error, :stale_lease}` — lease expired, already ACKed, or replaced
    * `{:error, reason}`
  """
  @spec ack(Lease.t(), keyword()) :: :ok | {:error, term()}
  def ack(%Lease{} = lease, opts \\ []) do
    {backend, handle, _} = resolve(opts)
    backend.ack(handle, lease)
  end

  @doc """
  Returns a leased event for retry / redelivery according to group policy.

  Sets delivery status back to available (or dead-letters when
  `attempt >= max_attempts`).

  ## Arguments

    * `lease` — `%Rheo.Lease{}` from `fetch/3`
    * `reason` — any term stored for diagnostics (default `:retry`)
    * `opts` — optional `:rheo` instance name

  ## Examples

      iex> stream = "doc-nack-" <> Integer.to_string(System.unique_integer([:positive]))
      iex> :ok = Rheo.create_stream(stream)
      iex> :ok = Rheo.create_group(stream, "risk", max_attempts: 5)
      iex> {:ok, _} = Rheo.append(stream, %{type: "tmp"})
      iex> {:ok, [lease]} = Rheo.fetch(stream, "risk", limit: 1, consumer_id: "c1")
      iex> Rheo.nack(lease, :temporary_error)
      :ok
      iex> {:ok, [again]} = Rheo.fetch(stream, "risk", limit: 1, consumer_id: "c2")
      iex> again.attempt
      2

  ## Returns

    * `:ok`
    * `{:error, :stale_lease}`
    * `{:error, reason}`
  """
  @spec nack(Lease.t(), term(), keyword()) :: :ok | {:error, term()}
  def nack(lease, reason \\ :retry, opts \\ [])

  def nack(%Lease{} = lease, reason, opts) when is_list(opts) do
    {backend, handle, _} = resolve(opts)
    backend.retry(handle, lease, reason)
  end

  @doc """
  Permanently rejects a leased event for this consumer group (dead-letter).

  Other groups are unaffected. The immutable event remains queryable.

  ## Arguments

    * `lease` — `%Rheo.Lease{}` from `fetch/3`
    * `reason` — any term stored on the delivery (default `:rejected`)
    * `opts` — optional `:rheo` instance name

  ## Examples

      iex> stream = "doc-reject-" <> Integer.to_string(System.unique_integer([:positive]))
      iex> :ok = Rheo.create_stream(stream)
      iex> :ok = Rheo.create_group(stream, "risk")
      iex> {:ok, _} = Rheo.append(stream, %{type: "poison"})
      iex> {:ok, [lease]} = Rheo.fetch(stream, "risk", limit: 1, consumer_id: "c1")
      iex> Rheo.reject(lease, :invalid_schema)
      :ok
      iex> Rheo.fetch(stream, "risk", limit: 1, consumer_id: "c2")
      {:ok, []}

  ## Returns

    * `:ok`
    * `{:error, :stale_lease}`
    * `{:error, reason}`
  """
  @spec reject(Lease.t(), term(), keyword()) :: :ok | {:error, term()}
  def reject(lease, reason \\ :rejected, opts \\ [])

  def reject(%Lease{} = lease, reason, opts) when is_list(opts) do
    {backend, handle, _} = resolve(opts)
    backend.reject(handle, lease, reason)
  end

  @doc """
  Verifies connectivity to the configured backend.

  ## Arguments

    * `opts` — optional `:rheo` instance name

  ## Examples

      iex> Rheo.ping()
      :ok

  ## Returns

    * `:ok`
    * `{:error, reason}` when the backend is unreachable
  """
  @spec ping(keyword()) :: :ok | {:error, term()}
  def ping(opts \\ []) do
    {backend, handle, _} = resolve(opts)
    backend.ping(handle)
  end

  @doc """
  Ensures backend indexes exist (safe to call repeatedly).

  ## Arguments

    * `opts` — optional `:rheo` instance name

  ## Examples

      iex> Rheo.ensure_indexes()
      :ok

  ## Returns

    * `:ok`
    * `{:error, reason}` on index creation failure
  """
  @spec ensure_indexes(keyword()) :: :ok | {:error, term()}
  def ensure_indexes(opts \\ []) do
    {backend, handle, _} = resolve(opts)
    backend.ensure_indexes(handle)
  end

  defp resolve(opts) do
    rheo = Keyword.get(opts, :rheo, __MODULE__)
    %Instance{backend: backend, handle: handle} = Instance.fetch!(rheo)
    {backend, handle, Keyword.delete(opts, :rheo)}
  end

  @mongo_backend Rheo.Backend.Mongo

  defp resolve_backend_opts(opts) do
    case Keyword.get(opts, :backend) do
      {mod, backend_opts} when is_atom(mod) and is_list(backend_opts) ->
        {mod, backend_opts}

      mod when is_atom(mod) and not is_nil(mod) ->
        {mod, Keyword.drop(opts, [:backend, :name])}

      nil ->
        mongo_shorthand!(opts)
    end
  end

  # `{Rheo, url: ...}` predates explicit backends. It stays only while Mongo
  # ships in the same package; core never references the module otherwise.
  defp mongo_shorthand!(opts) do
    if Keyword.has_key?(opts, :url) and Code.ensure_loaded?(@mongo_backend) do
      {@mongo_backend, Keyword.drop(opts, [:backend, :name])}
    else
      raise ArgumentError,
            "Rheo requires a :backend, e.g. {Rheo, backend: Rheo.Backend.ETS} or " <>
              "{Rheo, backend: {Rheo.Backend.Mongo, url: \"mongodb://...\"}}"
    end
  end

  defp default_backend_handle(rheo, backend), do: Module.concat(rheo, backend_suffix(backend))

  defp backend_suffix(backend) do
    backend |> Module.split() |> List.last()
  end
end
