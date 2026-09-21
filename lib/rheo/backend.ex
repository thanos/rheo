defmodule Rheo.Backend do
  @moduledoc """
  Behaviour for Rheo storage backends.

  A backend owns storage for streams, events, groups, and deliveries behind an
  opaque handle (a process name, pid, table reference, repo, …). Application
  code calls `Rheo`, which resolves the handle for the named instance and
  dispatches here.

  Part of the SemVer-frozen surface (ADR 029 / ADR 030). Required callbacks and
  the fencing / settle vocabulary will not change meaning without a major
  version. Ops inspect callbacks remain optional.

  ## Architecture

      +------------------+
      |  Application     |
      |  (Rheo facade)   |
      +--------+---------+
               |
               | resolve name -> {backend, handle}
               v
      +--------+---------+       +---------------------------+
      | Rheo.Instance    |------>| Rheo.Backend callbacks    |
      | (supervisor)     |       | child_spec / append / …   |
      +------------------+       +-------------+-------------+
                                               |
                     +-------------------------+-------------------------+
                     |                         |                         |
                     v                         v                         v
              +------------+            +------------+            +------------+
              | ETS/Mnesia |            | Mongo/Ecto |            | Redis /    |
              | (tables)   |            | (docs/SQL) |            | custom     |
              +------------+            +------------+            +------------+

  Delivery rows, SQL locking, and native consumer-group commands stay inside the
  adapter. A backend built on Redis Streams may implement `fetch` with
  `XREADGROUP`, `ack` with a fenced `XACK`, and `lag` with `XINFO GROUPS`
  without pretending to keep a deliveries table.

  ## Semantic contract

  Callbacks describe what Rheo needs, not how storage does it:

    * lifecycle and health — `child_spec/1`, `capabilities/0`,
      `ensure_indexes/1`, `ping/1`
    * streams and groups — `create_stream/3`, `create_group/4`
    * log — `append/4`, `append_batch/4`, `read/3`, `query/2`
    * delivery — `fetch/4`, `renew/3`, `ack/2`, `retry/3`, `reject/3`
    * group progress — `replay/4`, `reset_group/4`, `lag/4`
    * ops inspect (optional) — `list_streams/2`, `list_groups/3`,
      `dead_letters/4`, `group_info/4` (ADR 027)

  ## Fencing and receipts

  `fetch/4` returns leases carrying a unique `lease_id`. Every settle callback
  (`renew/3`, `ack/2`, `retry/3`, `reject/3`) must fail with
  `{:error, :stale_lease}` when the delivery is no longer held under that
  `lease_id`. A backend may also set `lease.receipt` to an opaque native claim
  identity (ADR 021); if it does, settle callbacks must compare it with term
  equality and fail with `{:error, :receipt_mismatch}` when it differs. Database
  backends usually leave `receipt` equal to `lease_id`.

  ## Error vocabulary

  Map driver failures into portable atoms at the adapter boundary. Do not return
  driver exception structs directly.

    * `:stream_not_found` — stream missing (checked before group)
    * `:group_not_found` — stream exists; group does not
    * `:already_exists` — duplicate stream or group
    * `:stale_lease` — settle lost the fencing token
    * `:receipt_mismatch` — native receipt no longer matches
    * `:unsupported` — optional ops callback not implemented
    * `:backend_unavailable` — backend unreachable / process down
    * `{:failed, cause}` — definite failure
    * `{:ambiguous, cause}` — outcome unknown (timeout, disconnect mid-write)

  See `Rheo.Settle` for how Group / Producer classify settle errors.

  ## Capabilities

  `capabilities/0` returns a `Rheo.Backend.Capabilities` struct. The conformance
  suite (`test/support/backend_contract.ex`) gates optional cases on declared
  guarantees; it never skips fencing or at-least-once checks.

  ## Implementing a backend

  Prefer the `Rheo` facade in application code. A minimal adapter is a GenServer
  (or borrowed connection) that implements the required callbacks and declares
  honest capabilities. Abbreviated ETS-style sketch:

      defmodule MyApp.MemoryBackend do
        @behaviour Rheo.Backend
        use GenServer

        @impl true
        def capabilities do
          Rheo.Backend.Capabilities.new(%{
            durable: false,
            distributed: false,
            batch_writes: true,
            ordered_range_scan: true,
            replay: true,
            partitions: true,
            contiguous_frontier: true
          })
        end

        @impl true
        def child_spec(opts) do
          name = Keyword.get(opts, :name, __MODULE__)

          %{
            id: {__MODULE__, name},
            start: {__MODULE__, :start_link, [opts]},
            type: :worker,
            restart: :permanent
          }
        end

        @impl true
        def ping(handle), do: GenServer.call(handle, :ping)

        @impl true
        def ensure_indexes(_handle), do: :ok

        @impl true
        def create_stream(handle, stream, opts),
          do: GenServer.call(handle, {:create_stream, stream, opts})

        # create_group, append, append_batch, read, query,
        # fetch, renew, ack, retry, reject, replay, reset_group, lag …
        #
        # Settle must return {:error, :stale_lease} when lease_id no longer
        # matches. Map driver failures to :backend_unavailable,
        # {:failed, cause}, or {:ambiguous, cause}.
      end

  See the [building your own backend](building-your-own-backend.html) guide and
  run the shared contract suite against the adapter before shipping.
  """

  alias Rheo.{DeadLetter, Event, GroupInfo, Lease, Query}

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

  ## Returns

  A `t:Supervisor.child_spec/0` map. Started by `Rheo` under the instance
  supervisor; the registered name (or equivalent) becomes the opaque handle.
  """
  @callback child_spec(opts()) :: Supervisor.child_spec()

  @doc """
  Returns the backend's static capability declaration.

  ## Returns

  A `Rheo.Backend.Capabilities.t()`. Guarantees gate conformance cases;
  mechanisms select optional optimizations. `at_least_once` and `lease_fencing`
  cannot be declared `false`.
  """
  @callback capabilities() :: Rheo.Backend.Capabilities.t()

  @doc """
  Creates indexes / schema needed for correct operation.

  Must be idempotent. Called from `Rheo.ensure_indexes/1`.

  ## Returns

    * `:ok`
    * `{:error, :backend_unavailable}`
    * `{:error, {:failed, cause}}` / `{:error, {:ambiguous, cause}}`
    * `{:error, term()}`
  """
  @callback ensure_indexes(handle()) :: :ok | {:error, term()}

  @doc """
  Creates a stream registry entry and its per-partition sequence allocators.

  ## Options

    * `:partition_count` — number of partitions (default `1`, must be `>= 1`)

  ## Returns

    * `:ok`
    * `{:error, :already_exists}`
    * `{:error, :invalid_partition_count}`
    * `{:error, :backend_unavailable}`
    * `{:error, term()}`
  """
  @callback create_stream(handle(), stream(), opts()) :: :ok | {:error, term()}

  @doc """
  Registers a consumer group on an existing stream.

  ## Options

    * `:max_attempts` — attempts before dead-letter on retry (default from env)
    * `:start_after` — exclusive sequence (integer or `%{partition => sequence}`)
    * `:start_at` — `DateTime`; begin at the first event at/after this time
    * `:partition` / `:partitions` — limit start cursors (default all)

  ## Returns

    * `:ok`
    * `{:error, :stream_not_found}`
    * `{:error, :already_exists}`
    * `{:error, :backend_unavailable}`
    * `{:error, term()}`
  """
  @callback create_group(handle(), stream(), group(), opts()) :: :ok | {:error, term()}

  @doc """
  Appends one event to a stream, allocating the next per-partition sequence.

  ## Options

    * `:id` — explicit event id (default generated)
    * `:key` — routing / payload key
    * `:metadata` — extra metadata map
    * `:timestamp` — `DateTime.t()`
    * `:partition` — explicit partition (overrides key routing)

  ## Returns

    * `{:ok, Event.t()}`
    * `{:error, :stream_not_found}`
    * `{:error, :backend_unavailable}`
    * `{:error, {:failed, cause}}` / `{:error, {:ambiguous, cause}}`
    * `{:error, term()}`
  """
  @callback append(handle(), stream(), map(), opts()) :: {:ok, Event.t()} | {:error, term()}

  @doc """
  Appends many events with contiguous per-partition sequences.

  Shared options apply to each payload (same keys as `c:append/4`).

  ## Returns

    * `{:ok, [Event.t()]}` — possibly empty
    * `{:error, :stream_not_found}`
    * `{:error, :backend_unavailable}`
    * `{:error, term()}`
  """
  @callback append_batch(handle(), stream(), [map()], opts()) ::
              {:ok, [Event.t()]} | {:error, term()}

  @doc """
  Reads events by sequence without mutating consumer-group state.

  ## Options

    * `:after` — return sequences strictly greater than this (default `0`)
    * `:limit` — max events (default `100`)
    * `:partition` — partition (default `0`)

  ## Returns

    * `{:ok, [Event.t()]}` — empty list when nothing matches
    * `{:error, :stream_not_found}` when the backend distinguishes missing streams
    * `{:error, :backend_unavailable}`
    * `{:error, term()}`
  """
  @callback read(handle(), stream(), opts()) :: {:ok, [Event.t()]} | {:error, term()}

  @doc """
  Queries historical events using a portable `Rheo.Query`.

  Filtering may use secondary indexes when declared, or walk and filter in the
  adapter when `secondary_indexes: false`.

  ## Returns

    * `{:ok, [Event.t()]}`
    * `{:error, :backend_unavailable}`
    * `{:error, term()}`
  """
  @callback query(handle(), Query.t()) :: {:ok, [Event.t()]} | {:error, term()}

  @doc """
  Claims up to `opts[:limit]` events for a group, returning fenced leases.

  ## Options

    * `:limit` — max leases (default from env / `default_max_demand`)
    * `:consumer_id` — claimant identity (default generated)
    * `:lease_ms` — lease TTL in milliseconds
    * `:partition` / `:partitions` — claim only those partitions (default all)

  ## Returns

    * `{:ok, [Lease.t()]}` — empty when nothing is claimable
    * `{:error, :stream_not_found}`
    * `{:error, :group_not_found}`
    * `{:error, :backend_unavailable}`
    * `{:error, term()}`
  """
  @callback fetch(handle(), stream(), group(), opts()) :: {:ok, [Lease.t()]} | {:error, term()}

  @doc """
  Extends an active lease's expiry when `lease_id` (and receipt) still match.

  ## Options

    * `:lease_ms` — new TTL from now (default from config)

  ## Returns

    * `{:ok, Lease.t()}` with updated `expires_at`
    * `{:error, :stale_lease}`
    * `{:error, :receipt_mismatch}`
    * `{:error, :backend_unavailable}`
    * `{:error, term()}`
  """
  @callback renew(handle(), Lease.t(), opts()) :: {:ok, Lease.t()} | {:error, term()}

  @doc """
  Acknowledges a lease when `lease_id` (and receipt) still match.

  Durable for that consumer group only. Never deletes immutable log events.

  ## Returns

    * `:ok`
    * `{:error, :stale_lease}`
    * `{:error, :receipt_mismatch}`
    * `{:error, :backend_unavailable}`
    * `{:error, {:failed, cause}}` / `{:error, {:ambiguous, cause}}`
    * `{:error, term()}`
  """
  @callback ack(handle(), Lease.t()) :: :ok | {:error, term()}

  @doc """
  Marks a lease for redelivery, or dead-letters it at the group's max attempts.

  Must fence on `lease_id` (and receipt). When `lease.attempt >= max_attempts`,
  dead-letter instead of reopening.

  ## Returns

    * `:ok`
    * `{:error, :stale_lease}`
    * `{:error, :receipt_mismatch}`
    * `{:error, :group_not_found}`
    * `{:error, :backend_unavailable}`
    * `{:error, term()}`
  """
  @callback retry(handle(), Lease.t(), term()) :: :ok | {:error, term()}

  @doc """
  Dead-letters a lease for this group only.

  Must fence on `lease_id` (and receipt). Does not delete the event from the log.

  ## Returns

    * `:ok`
    * `{:error, :stale_lease}`
    * `{:error, :receipt_mismatch}`
    * `{:error, :backend_unavailable}`
    * `{:error, term()}`
  """
  @callback reject(handle(), Lease.t(), term()) :: :ok | {:error, term()}

  @doc """
  Re-opens deliveries for a group for replay without copying events.

  ## Options

  Backends typically accept one of:

    * `:from_sequence` — reopen from this exclusive sequence onward
    * `:event_ids` — reopen specific event ids
    * `:events` — reopen from concrete `%Rheo.Event{}` structs
    * `:partition` / `:partitions` — scope the rewind

  The `Rheo.replay/3` facade may translate `:from` (`DateTime`) or `:query`
  before calling this callback.

  ## Returns

    * `:ok`
    * `{:error, :stream_not_found}`
    * `{:error, :group_not_found}`
    * `{:error, :invalid_replay_opts}`
    * `{:error, :backend_unavailable}`
    * `{:error, term()}`
  """
  @callback replay(handle(), stream(), group(), opts()) :: :ok | {:error, term()}

  @doc """
  Clears deliveries for a group and resets its materialization cursor.

  Never deletes immutable events. `Rheo.reset_group/3` requires `confirm: true`
  before calling this.

  ## Options

    * `:start_after` / `:start_at` — new start cursors (same as `c:create_group/4`)
    * `:partition` / `:partitions` — scope the reset (default all)

  ## Returns

    * `:ok`
    * `{:error, :stream_not_found}`
    * `{:error, :group_not_found}`
    * `{:error, :backend_unavailable}`
    * `{:error, term()}`
  """
  @callback reset_group(handle(), stream(), group(), opts()) :: :ok | {:error, term()}

  @doc """
  Returns committed frontier vs high-watermark lag for a group.

  ## Options

    * `:partition` / `:partitions` — report lag for those partitions (default all)

  ## Returns

    * `{:ok, Rheo.Lag.t()}`
    * `{:error, :stream_not_found}`
    * `{:error, :group_not_found}`
    * `{:error, :backend_unavailable}`
    * `{:error, term()}`
  """
  @callback lag(handle(), stream(), group(), opts()) :: {:ok, Rheo.Lag.t()} | {:error, term()}

  @doc """
  Health-checks the backend connection.

  ## Returns

    * `:ok`
    * `{:error, :backend_unavailable}`
    * `{:error, term()}`
  """
  @callback ping(handle()) :: :ok | {:error, term()}

  @doc """
  Lists registered stream names (ops inspect, ADR 027).

  Optional — when unimplemented, `Rheo.list_streams/1` returns
  `{:error, :unsupported}`.

  ## Returns

    * `{:ok, [stream()]}`
    * `{:error, :unsupported}`
    * `{:error, :backend_unavailable}`
    * `{:error, term()}`
  """
  @callback list_streams(handle(), opts()) :: {:ok, [stream()]} | {:error, term()}

  @doc """
  Lists consumer group names for a stream (ops inspect, ADR 027).

  Optional — when unimplemented, `Rheo.list_groups/2` returns
  `{:error, :unsupported}`.

  ## Returns

    * `{:ok, [group()]}`
    * `{:error, :stream_not_found}`
    * `{:error, :unsupported}`
    * `{:error, :backend_unavailable}`
    * `{:error, term()}`
  """
  @callback list_groups(handle(), stream(), opts()) :: {:ok, [group()]} | {:error, term()}

  @doc """
  Lists dead-lettered deliveries for a group (ops inspect, ADR 027).

  ## Options

    * `:limit` — max rows (default `100`)
    * `:after` — event id cursor for pagination

  Optional — when unimplemented, `Rheo.dead_letters/3` returns
  `{:error, :unsupported}`.

  ## Returns

    * `{:ok, [DeadLetter.t()]}`
    * `{:error, :group_not_found}`
    * `{:error, :cursor_not_found}`
    * `{:error, :unsupported}`
    * `{:error, :backend_unavailable}`
    * `{:error, term()}`
  """
  @callback dead_letters(handle(), stream(), group(), opts()) ::
              {:ok, [DeadLetter.t()]} | {:error, term()}

  @doc """
  Returns lag plus inflight and dead-letter counts (ops inspect, ADR 027).

  ## Options

    * `:partition` / `:partitions` — scope lag (default all)

  Optional — when unimplemented, `Rheo.group_info/3` returns
  `{:error, :unsupported}`.

  ## Returns

    * `{:ok, GroupInfo.t()}`
    * `{:error, :stream_not_found}`
    * `{:error, :group_not_found}`
    * `{:error, :unsupported}`
    * `{:error, :backend_unavailable}`
    * `{:error, term()}`
  """
  @callback group_info(handle(), stream(), group(), opts()) ::
              {:ok, GroupInfo.t()} | {:error, term()}

  @optional_callbacks list_streams: 2,
                      list_groups: 3,
                      dead_letters: 4,
                      group_info: 4
end
