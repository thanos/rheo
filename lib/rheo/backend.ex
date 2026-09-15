defmodule Rheo.Backend do
  @moduledoc """
  Behaviour for Rheo storage backends.

  Implementations own durable storage for streams, events, groups, and
  deliveries. MongoDB (`Rheo.Backend.Mongo`) is the only MVP adapter.

  Application code normally calls `Rheo` rather than backends directly.

  ## Implementing a backend

      defmodule MyApp.Backend.Postgres do
        @behaviour Rheo.Backend

        @impl true
        def child_spec(opts) do
          %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
        end

        @impl true
        def ping(topo), do: # ...

        # implement remaining callbacks...
      end

  ## Types

  See `t:topology/0`, `t:stream/0`, `t:group/0`, and `t:opts/0`.
  """

  alias Rheo.{Event, Lease}

  @typedoc "Process name or pid for the backend connection/topology."
  @type topology :: GenServer.server()

  @typedoc "Stream name."
  @type stream :: String.t()

  @typedoc "Consumer group name."
  @type group :: String.t()

  @typedoc "Backend-specific options (keyword list)."
  @type opts :: keyword()

  @doc """
  Returns a child spec that starts the backend connection under Rheo's supervisor.

  ## Arguments

    * `opts` — connection options (for Mongo: `:url`, `:name`, `:pool_size`, …)

  ## Returns

  A `Supervisor.child_spec/0` map.

  ## Example

      Rheo.Backend.Mongo.child_spec(url: "mongodb://localhost:27017/rheo", name: Rheo.Mongo)
  """
  @callback child_spec(opts()) :: Supervisor.child_spec()

  @doc """
  Creates indexes required for correct and efficient operation.

  ## Arguments

    * `topology` — backend connection

  ## Returns

    * `:ok`
    * `{:error, reason}`
  """
  @callback ensure_indexes(topology()) :: :ok | {:error, term()}

  @doc """
  Creates a stream registry entry and sequence allocator.

  ## Arguments

    * `topology` — backend connection
    * `stream` — unique stream name
    * `opts` — e.g. `:partition_count`

  ## Returns

    * `:ok`
    * `{:error, :already_exists}`
    * `{:error, reason}`
  """
  @callback create_stream(topology(), stream(), opts()) :: :ok | {:error, term()}

  @doc """
  Registers a consumer group on a stream.

  ## Arguments

    * `topology` — backend connection
    * `stream` — existing stream
    * `group` — group name
    * `opts` — e.g. `:max_attempts`

  ## Returns

    * `:ok`
    * `{:error, :already_exists | :stream_not_found}`
    * `{:error, reason}`
  """
  @callback create_group(topology(), stream(), group(), opts()) :: :ok | {:error, term()}

  @doc """
  Appends one event and returns the persisted `%Rheo.Event{}`.

  ## Returns

    * `{:ok, Event.t()}`
    * `{:error, :stream_not_found}`
    * `{:error, reason}`
  """
  @callback append(topology(), stream(), map(), opts()) :: {:ok, Event.t()} | {:error, term()}

  @doc """
  Appends many events with contiguous sequences.

  ## Returns

    * `{:ok, [Event.t()]}`
    * `{:error, term()}`
  """
  @callback append_batch(topology(), stream(), [map()], opts()) ::
              {:ok, [Event.t()]} | {:error, term()}

  @doc """
  Reads events by sequence without mutating consumer state.

  ## Returns

    * `{:ok, [Event.t()]}`
    * `{:error, term()}`
  """
  @callback read(topology(), stream(), opts()) :: {:ok, [Event.t()]} | {:error, term()}

  @doc """
  Queries historical events with backend-supported filters.

  ## Returns

    * `{:ok, [Event.t()]}`
    * `{:error, term()}`
  """
  @callback query(topology(), stream(), opts()) :: {:ok, [Event.t()]} | {:error, term()}

  @doc """
  Claims up to `opts[:limit]` events for a group, returning leases.

  ## Returns

    * `{:ok, [Lease.t()]}`
    * `{:error, :group_not_found}`
    * `{:error, term()}`
  """
  @callback fetch(topology(), stream(), group(), opts()) :: {:ok, [Lease.t()]} | {:error, term()}

  @doc """
  Acknowledges a lease when `lease_id` still matches.

  ## Returns

    * `:ok`
    * `{:error, :stale_lease}`
    * `{:error, term()}`
  """
  @callback ack(topology(), Lease.t()) :: :ok | {:error, term()}

  @doc """
  Marks a lease for retry / redelivery (or dead-letters at max attempts).

  ## Returns

    * `:ok`
    * `{:error, :stale_lease}`
    * `{:error, term()}`
  """
  @callback retry(topology(), Lease.t(), term()) :: :ok | {:error, term()}

  @doc """
  Dead-letters a lease for this group only.

  ## Returns

    * `:ok`
    * `{:error, :stale_lease}`
    * `{:error, term()}`
  """
  @callback reject(topology(), Lease.t(), term()) :: :ok | {:error, term()}

  @doc """
  Health-checks the backend connection.

  ## Returns

    * `:ok`
    * `{:error, term()}`
  """
  @callback ping(topology()) :: :ok | {:error, term()}
end
