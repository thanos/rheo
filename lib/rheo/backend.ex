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

  @typedoc """
  Declared backend capabilities.

  Required keys for v0.3+: `:durable`, `:distributed`, `:atomic_compare_and_set`,
  `:notifications`, `:change_feed`, `:secondary_indexes`, `:batch_writes`,
  `:ordered_range_scan`. Optional from v0.4+: `:replay`.
  """
  @type capabilities :: %{
          optional(atom()) => boolean(),
          durable: boolean(),
          distributed: boolean(),
          atomic_compare_and_set: boolean(),
          notifications: boolean(),
          change_feed: boolean(),
          secondary_indexes: boolean(),
          batch_writes: boolean(),
          ordered_range_scan: boolean()
        }

  @doc """
  Returns a child spec that starts backend resources under a Rheo instance.
  """
  @callback child_spec(opts()) :: Supervisor.child_spec()

  @doc """
  Returns static capability flags for this backend module.

  Used for documentation and conformance gating — not to weaken fencing.
  """
  @callback capabilities() :: capabilities()

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

  @doc """
  Re-opens deliveries for a group for replay (does not copy events).

  Options typically include `:from_sequence`, `:from` (`DateTime`), or a list of
  event ids under `:event_ids`.
  """
  @callback replay(handle(), stream(), group(), opts()) :: :ok | {:error, term()}

  @doc """
  Clears all deliveries for a group and resets its materialization cursor.

  Never deletes immutable events. Callers must pass `confirm: true` via `Rheo`.
  """
  @callback reset_group(handle(), stream(), group(), opts()) :: :ok | {:error, term()}

  @doc "Health-checks the backend connection."
  @callback ping(handle()) :: :ok | {:error, term()}
end
