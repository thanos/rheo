defmodule Rheo.Backend do
  @moduledoc """
  Behaviour for Rheo storage backends.

  A backend owns durable storage for streams, events, groups, and deliveries
  behind an opaque handle (a process name, pid, table reference, repo, …).
  Application code calls `Rheo`, which resolves the handle for the named
  instance and dispatches here.

  ## Semantic contract

  Callbacks describe what Rheo needs, not how storage does it:

    * log — `append/4`, `append_batch/4`, `read/3`, `query/2`
    * delivery — `fetch/4`, `renew/3`, `ack/2`, `retry/3`, `reject/3`
    * group progress — `replay/4`, `reset_group/4`, `lag/4`
    * lifecycle and health — `child_spec/1`, `capabilities/0`,
      `ensure_indexes/1`, `ping/1`

  Delivery rows, SQL locking, and native consumer-group commands stay inside the
  adapter. A backend built on Redis Streams may implement `fetch` with
  `XREADGROUP`, `ack` with a fenced `XACK`, and `lag` with `XINFO GROUPS`
  without pretending to keep a deliveries table.

  ## Fencing and receipts

  `fetch/4` returns leases carrying a unique `lease_id`. Every settle callback
  (`renew/3`, `ack/2`, `retry/3`, `reject/3`) must fail with
  `{:error, :stale_lease}` when the delivery is no longer held under that
  `lease_id`. A backend may also set `lease.receipt` to an opaque native claim
  identity (ADR 021); if it does, settle callbacks must compare it with term
  equality and fail with `{:error, :receipt_mismatch}` when it differs. Database
  backends usually leave `receipt` equal to `lease_id`.

  ## Error vocabulary

  Map driver failures into `Rheo.Settle` reasons at the adapter boundary:
  `:backend_unavailable` when the backend cannot be reached, `{:failed, cause}`
  for definite failures, `{:ambiguous, cause}` when the outcome is unknown. Do
  not return driver exception structs directly.

  ## Capabilities

  `capabilities/0` returns a `Rheo.Backend.Capabilities` struct. The conformance
  suite (`test/support/backend_contract.ex`) gates optional cases on declared
  guarantees; it never skips fencing or at-least-once checks.
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

  Backends that borrow host-owned resources (an `Ecto.Repo`) still return a
  small configuration process so the instance has a handle.
  """
  @callback child_spec(opts()) :: Supervisor.child_spec()

  @doc "Returns the backend's static capability declaration."
  @callback capabilities() :: Rheo.Backend.Capabilities.t()

  @doc "Creates indexes / schema needed for correct operation. Idempotent."
  @callback ensure_indexes(handle()) :: :ok | {:error, term()}

  @doc "Creates a stream registry entry and its per-partition sequence allocators."
  @callback create_stream(handle(), stream(), opts()) :: :ok | {:error, term()}

  @doc "Registers a consumer group on a stream."
  @callback create_group(handle(), stream(), group(), opts()) :: :ok | {:error, term()}

  @doc "Appends one event."
  @callback append(handle(), stream(), map(), opts()) :: {:ok, Event.t()} | {:error, term()}

  @doc "Appends many events with contiguous per-partition sequences."
  @callback append_batch(handle(), stream(), [map()], opts()) ::
              {:ok, [Event.t()]} | {:error, term()}

  @doc "Reads events by sequence without mutating consumer state."
  @callback read(handle(), stream(), opts()) :: {:ok, [Event.t()]} | {:error, term()}

  @doc "Queries historical events using a portable `Rheo.Query`."
  @callback query(handle(), Query.t()) :: {:ok, [Event.t()]} | {:error, term()}

  @doc "Claims up to `opts[:limit]` events for a group, returning fenced leases."
  @callback fetch(handle(), stream(), group(), opts()) :: {:ok, [Lease.t()]} | {:error, term()}

  @doc "Extends an active lease's expiry when `lease_id` (and receipt) still match."
  @callback renew(handle(), Lease.t(), opts()) :: {:ok, Lease.t()} | {:error, term()}

  @doc "Acknowledges a lease when `lease_id` (and receipt) still match."
  @callback ack(handle(), Lease.t()) :: :ok | {:error, term()}

  @doc "Marks a lease for redelivery, or dead-letters it at the group's max attempts."
  @callback retry(handle(), Lease.t(), term()) :: :ok | {:error, term()}

  @doc "Dead-letters a lease for this group only."
  @callback reject(handle(), Lease.t(), term()) :: :ok | {:error, term()}

  @doc """
  Re-opens deliveries for a group for replay without copying events.

  Options typically include `:from_sequence`, `:from` (`DateTime`), or a list of
  event ids under `:event_ids`.
  """
  @callback replay(handle(), stream(), group(), opts()) :: :ok | {:error, term()}

  @doc """
  Clears all deliveries for a group and resets its materialization cursor.

  Never deletes immutable events. `Rheo.reset_group/3` requires `confirm: true`
  before calling this.
  """
  @callback reset_group(handle(), stream(), group(), opts()) :: :ok | {:error, term()}

  @doc "Returns committed frontier vs high-watermark lag for a group."
  @callback lag(handle(), stream(), group(), opts()) :: {:ok, Rheo.Lag.t()} | {:error, term()}

  @doc "Health-checks the backend connection."
  @callback ping(handle()) :: :ok | {:error, term()}
end
