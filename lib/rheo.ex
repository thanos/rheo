defmodule Rheo do
  @moduledoc """
  Rheo provides durable consumer-group semantics over searchable databases.

  Events are **immutable**. Consumption records progress, leases, retries, and
  dead-letters in separate consumer-group state. Delivery is **at-least-once**:
  after a crash or lease expiry an event may be delivered again. Make handlers
  idempotent using stable event ids (`Rheo.Event.id`).

  ## Supervision

      children = [
        {Rheo, url: "mongodb://localhost:27017/rheo"},
        {MyApp.RiskConsumer, []}
      ]

      Supervisor.start_link(children, strategy: :one_for_one)

  ## Typical low-level flow

      Rheo.create_stream("market-events")
      {:ok, event} = Rheo.append("market-events", %{type: "curve_update", currency: "EUR"})
      Rheo.create_group("market-events", "risk")
      {:ok, leases} = Rheo.fetch("market-events", "risk", limit: 10)
      :ok = Rheo.ack(hd(leases))
      {:ok, _} = Rheo.query("market-events", type: "curve_update", currency: "EUR")

  See also `Rheo.Consumer` for the OTP handler API and `Rheo.Backend` for adapters.
  """

  use Supervisor

  alias Rheo.{Event, Lease}

  @typedoc "Name of an event stream."
  @type stream :: String.t()

  @typedoc "Name of a consumer group on a stream."
  @type group :: String.t()

  @doc """
  Starts Rheo under a supervisor (usually your application supervisor).

  Options are forwarded to the backend child spec (`Rheo.Backend.Mongo` by
  default).

  ## Options

    * `:url` — MongoDB connection URL (required unless configured globally)
    * `:name` — Mongo topology process name (default `Rheo.Mongo`)
    * `:pool_size` — connection pool size (default `5`)
    * `:backend` — backend module implementing `Rheo.Backend` (default
      `Rheo.Backend.Mongo`)
    * `:supervisor_name` — name for this Rheo supervisor (default `Rheo`)

  ## Examples

      iex> name = String.to_atom("mongo_doc_#{System.unique_integer([:positive])}")
      iex> sup = String.to_atom("rheo_doc_#{System.unique_integer([:positive])}")
      iex> url = System.get_env("RHEO_MONGO_URL", "mongodb://localhost:27017/rheo_test")
      iex> {:ok, pid} = Rheo.start_link(url: url, name: name, supervisor_name: sup)
      iex> is_pid(pid)
      true

  ## Returns

    * `{:ok, pid}` on success
    * `{:error, {:already_started, pid}}` if `supervisor_name` is taken
    * `{:error, reason}` on backend start failure

  ## Errors / raises

  Does not raise for normal connection failures; they surface as `{:error, reason}`.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :supervisor_name, __MODULE__))
  end

  @impl true
  def init(opts) do
    backend = Keyword.get(opts, :backend, Rheo.Backend.Mongo)
    backend_opts = Keyword.drop(opts, [:backend, :supervisor_name])

    children = [
      backend.child_spec(backend_opts)
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc false
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  @doc """
  Creates a named stream.

  ## Arguments

    * `stream` — unique stream name (`t:stream/0`)
    * `opts` — optional keyword list:
      * `:partition_count` — number of partitions (default `1`; MVP consumes
        partition `0` only)

  ## Examples

      iex> stream = "doc-create-#{System.unique_integer([:positive])}"
      iex> Rheo.create_stream(stream)
      :ok
      iex> Rheo.create_stream(stream)
      {:error, :already_exists}

  ## Returns

    * `:ok` when the stream is created
    * `{:error, :already_exists}` when the name is taken
    * `{:error, reason}` on backend failure

  ## Errors / raises

  Does not raise for duplicate names; returns `{:error, :already_exists}`.
  """
  @spec create_stream(stream(), keyword()) :: :ok | {:error, term()}
  def create_stream(stream, opts \\ []) do
    backend().create_stream(topology(), stream, opts)
  end

  @doc """
  Creates a consumer group on an existing stream.

  Groups consume independently: an ACK in `"risk"` does not ACK `"surveillance"`.

  ## Arguments

    * `stream` — existing stream name
    * `group` — consumer group name
    * `opts` — optional keyword list:
      * `:max_attempts` — attempts before dead-letter on retry (default from
        application env, typically `5`)

  ## Examples

      iex> stream = "doc-group-#{System.unique_integer([:positive])}"
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
    backend().create_group(topology(), stream, group, opts)
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
      * `:id` — explicit event id (default: generated)
      * `:key` — overrides payload key
      * `:metadata` — extra metadata map
      * `:timestamp` — `DateTime.t()` (default: clock now)
      * `:partition` — partition number (default `0`)

  ## Examples

      iex> stream = "doc-append-#{System.unique_integer([:positive])}"
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

  ## Errors / raises

  Raises `FunctionClauseError` when `payload` is not a map.
  """
  @spec append(stream(), map(), keyword()) :: {:ok, Event.t()} | {:error, term()}
  def append(stream, payload, opts \\ []) when is_map(payload) do
    backend().append(topology(), stream, payload, opts)
  end

  @doc """
  Appends multiple events, allocating contiguous sequences.

  ## Arguments

    * `stream` — existing stream name
    * `payloads` — list of payload maps (same shape as `append/3`)
    * `opts` — shared options applied to each event (see `append/3`)

  ## Examples

      iex> stream = "doc-batch-#{System.unique_integer([:positive])}"
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

  ## Errors / raises

  Raises `FunctionClauseError` when `payloads` is not a list.
  """
  @spec append_batch(stream(), [map()], keyword()) :: {:ok, [Event.t()]} | {:error, term()}
  def append_batch(stream, payloads, opts \\ []) when is_list(payloads) do
    backend().append_batch(topology(), stream, payloads, opts)
  end

  @doc """
  Reads events by sequence without affecting consumer-group state.

  ## Arguments

    * `stream` — stream name
    * `opts` — optional keyword list:
      * `:after` — return sequences strictly greater than this (default `0`)
      * `:limit` — max events (default `100`)
      * `:partition` — partition (default `0`)

  ## Examples

      iex> stream = "doc-read-#{System.unique_integer([:positive])}"
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
    backend().read(topology(), stream, opts)
  end

  @doc """
  Queries historical events. Consumption never removes events from the log.

  ## Arguments

    * `stream` — stream name
    * `opts` — filter and control options:
      * `:type` — match `Event.type`
      * `:key` — match `Event.key`
      * `:partition` — partition filter
      * `:currency` — `payload.currency`
      * `:curve` — `payload.curve`
      * `:correlation_id` — `metadata.correlation_id`
      * `:producer` — `metadata.producer`
      * `:from` / `:to` — `DateTime.t()` timestamp range (inclusive)
      * `:limit` — max results (default `100`)
      * `:sort` — Mongo sort document (default `%{"sequence" => 1}`)
      * any other atom key — matched as `payload.<key>`

  ## Examples

      iex> stream = "doc-query-#{System.unique_integer([:positive])}"
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

  ## Returns

    * `{:ok, [%Rheo.Event{}]}`
    * `{:error, reason}` on backend failure
  """
  @spec query(stream(), keyword()) :: {:ok, [Event.t()]} | {:error, term()}
  def query(stream, opts \\ []) do
    backend().query(topology(), stream, opts)
  end

  @doc """
  Fetches up to `:limit` events for a consumer group, creating leases.

  Competing workers in the same group receive distinct leases. Independent
  groups may lease the same events concurrently.

  ## Arguments

    * `stream` — stream name
    * `group` — consumer group name
    * `opts` — optional keyword list:
      * `:limit` — max leases / demand bound (default from config)
      * `:consumer_id` — worker identity (default: generated)
      * `:lease_ms` — lease TTL in milliseconds (default from config)

  ## Examples

      iex> stream = "doc-fetch-#{System.unique_integer([:positive])}"
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
    backend().fetch(topology(), stream, group, opts)
  end

  @doc """
  Acknowledges successful processing of a leased event.

  Durable for that consumer group only. Requires the current `lease_id`.

  ## Arguments

    * `lease` — `%Rheo.Lease{}` returned by `fetch/3`

  ## Examples

      iex> stream = "doc-ack-#{System.unique_integer([:positive])}"
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
  @spec ack(Lease.t()) :: :ok | {:error, term()}
  def ack(%Lease{} = lease), do: backend().ack(topology(), lease)

  @doc """
  Returns a leased event for retry / redelivery according to group policy.

  Sets delivery status back to available (or dead-letters when
  `attempt >= max_attempts`).

  ## Arguments

    * `lease` — `%Rheo.Lease{}` from `fetch/3`
    * `reason` — any term stored for diagnostics (default `:retry`)

  ## Examples

      iex> stream = "doc-nack-#{System.unique_integer([:positive])}"
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
  @spec nack(Lease.t(), term()) :: :ok | {:error, term()}
  def nack(%Lease{} = lease, reason \\ :retry), do: backend().retry(topology(), lease, reason)

  @doc """
  Permanently rejects a leased event for this consumer group (dead-letter).

  Other groups are unaffected. The immutable event remains queryable.

  ## Arguments

    * `lease` — `%Rheo.Lease{}` from `fetch/3`
    * `reason` — any term stored on the delivery (default `:rejected`)

  ## Examples

      iex> stream = "doc-reject-#{System.unique_integer([:positive])}"
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
  @spec reject(Lease.t(), term()) :: :ok | {:error, term()}
  def reject(%Lease{} = lease, reason \\ :rejected),
    do: backend().reject(topology(), lease, reason)

  @doc """
  Verifies connectivity to the configured backend.

  ## Examples

      iex> Rheo.ping()
      :ok

  ## Returns

    * `:ok`
    * `{:error, reason}` when the backend is unreachable
  """
  @spec ping() :: :ok | {:error, term()}
  def ping, do: backend().ping(topology())

  @doc """
  Ensures backend indexes exist (safe to call repeatedly).

  ## Examples

      iex> Rheo.ensure_indexes()
      :ok

  ## Returns

    * `:ok`
    * `{:error, reason}` on index creation failure
  """
  @spec ensure_indexes() :: :ok | {:error, term()}
  def ensure_indexes, do: backend().ensure_indexes(topology())

  defp topology do
    Application.get_env(:rheo, :topology, Rheo.Mongo)
  end

  defp backend do
    Application.get_env(:rheo, :backend, Rheo.Backend.Mongo)
  end
end
