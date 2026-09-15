defmodule Rheo.Backend do
  @moduledoc """
  Behaviour for Rheo storage backends.

  Implementations own durable storage for streams, events, groups, and
  deliveries. The handle is an opaque backend-specific value (Mongo process
  name, ETS table prefix, Repo, …) — not necessarily a GenServer.

  Application code normally calls `Rheo` rather than backends directly.
  """

  alias Rheo.{Event, Lease, Query}

  @typedoc "Opaque backend connection handle (process name, pid, table ref, …)."
  @type handle :: term()

  @typedoc "Stream name."
  @type stream :: String.t()

  @typedoc "Consumer group name."
  @type group :: String.t()

  @typedoc "Backend-specific options (keyword list)."
  @type opts :: keyword()

  @doc """
  Returns a child spec that starts backend resources under a Rheo instance.
  """
  @callback child_spec(opts()) :: Supervisor.child_spec()

  @doc "Creates indexes / schema needed for correct operation."
  @callback ensure_indexes(handle()) :: :ok | {:error, term()}

  @doc "Creates a stream registry entry and sequence allocator."
  @callback create_stream(handle(), stream(), opts()) :: :ok | {:error, term()}

  @doc "Registers a consumer group on a stream."
  @callback create_group(handle(), stream(), group(), opts()) :: :ok | {:error, term()}

  @doc "Appends one event."
  @callback append(handle(), stream(), map(), opts()) :: {:ok, Event.t()} | {:error, term()}

  @doc "Appends many events with contiguous sequences."
  @callback append_batch(handle(), stream(), [map()], opts()) ::
              {:ok, [Event.t()]} | {:error, term()}

  @doc "Reads events by sequence without mutating consumer state."
  @callback read(handle(), stream(), opts()) :: {:ok, [Event.t()]} | {:error, term()}

  @doc "Queries historical events using a portable `Rheo.Query`."
  @callback query(handle(), Query.t()) :: {:ok, [Event.t()]} | {:error, term()}

  @doc "Claims up to `opts[:limit]` events for a group."
  @callback fetch(handle(), stream(), group(), opts()) :: {:ok, [Lease.t()]} | {:error, term()}

  @doc "Extends an active lease's expiry when `lease_id` still matches."
  @callback renew(handle(), Lease.t(), opts()) :: {:ok, Lease.t()} | {:error, term()}

  @doc "Acknowledges a lease when `lease_id` still matches."
  @callback ack(handle(), Lease.t()) :: :ok | {:error, term()}

  @doc "Marks a lease for retry / redelivery (or dead-letters at max attempts)."
  @callback retry(handle(), Lease.t(), term()) :: :ok | {:error, term()}

  @doc "Dead-letters a lease for this group only."
  @callback reject(handle(), Lease.t(), term()) :: :ok | {:error, term()}

  @doc "Health-checks the backend connection."
  @callback ping(handle()) :: :ok | {:error, term()}
end
