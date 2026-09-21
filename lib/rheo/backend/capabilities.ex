defmodule Rheo.Backend.Capabilities do
  @moduledoc """
  Typed backend capability declaration (ADR 023).

  A backend declares two kinds of facts:

    * **guarantees** — semantic promises Rheo makes through this backend. The
      conformance suite gates correctness tests on them.
    * **mechanisms** — how the backend implements or optimizes delivery. They
      select optional conformance cases and runtime optimizations, never
      correctness.

  `at_least_once` and `lease_fencing` are Rheo product invariants: they default
  to `true` and may not be declared `false`.

  ## Guarantees

  | Key | Meaning |
  |---|---|
  | `:durable` | Events and delivery state survive a backend process restart |
  | `:distributed` | Several BEAM nodes may fetch from the same group concurrently |
  | `:at_least_once` | Always `true` |
  | `:lease_fencing` | Always `true` |
  | `:partitions` | Multi-partition streams with per-partition sequences |
  | `:contiguous_frontier` | `Rheo.lag/3` reports a contiguous ACK frontier |
  | `:replay` | `Rheo.replay/3` and `Rheo.reset_group/3` are supported |

  See the [building your own backend](building-your-own-backend.html) guide for a
  per-backend comparison (Redis / Postgres yes; ETS / SQLite / Mnesia v0.11 no).

  ## Mechanisms

  | Key | Meaning |
  |---|---|
  | `:atomic_compare_and_set` | Claims use a native atomic compare-and-set |
  | `:ordered_range_scan` | Sequence ranges are read with an ordered scan |
  | `:secondary_indexes` | Payload / metadata filters use secondary indexes |
  | `:batch_writes` | `append_batch` is a single backend write |
  | `:notifications` | The backend can signal new events (for example `NOTIFY`) |
  | `:change_feed` | The backend exposes a change feed |
  | `:native_consumer_groups` | The backend owns consumer-group state (Redis Streams) |
  | `:native_pending_list` | Pending deliveries are backend-native (`XPENDING`) |
  | `:native_reclaim` | Expired claims are reclaimed natively (`XAUTOCLAIM`) |
  | `:blocking_reads` | Fetch can block until work arrives (`XREADGROUP BLOCK`) |
  | `:native_group_lag` | Lag comes from the backend (`XINFO GROUPS`) |

  ## Examples

      iex> caps = Rheo.Backend.Capabilities.new(durable: true, partitions: true, batch_writes: true)
      iex> {caps.guarantees.durable, caps.guarantees.at_least_once, caps.mechanisms.batch_writes}
      {true, true, true}

      iex> Rheo.Backend.Capabilities.guarantee?(Rheo.Backend.Capabilities.new([]), :durable)
      false

      iex> Rheo.Backend.Capabilities.new(durable: "yes")
      ** (ArgumentError) capability :durable must be a boolean, got: "yes"

      iex> Rheo.Backend.Capabilities.new(lease_fencing: false)
      ** (ArgumentError) capability :lease_fencing is a Rheo invariant and cannot be false
  """

  @guarantee_keys [
    :durable,
    :distributed,
    :at_least_once,
    :lease_fencing,
    :partitions,
    :contiguous_frontier,
    :replay
  ]

  @mechanism_keys [
    :atomic_compare_and_set,
    :ordered_range_scan,
    :secondary_indexes,
    :batch_writes,
    :notifications,
    :change_feed,
    :native_consumer_groups,
    :native_pending_list,
    :native_reclaim,
    :blocking_reads,
    :native_group_lag
  ]

  @invariants [:at_least_once, :lease_fencing]

  @enforce_keys [:guarantees, :mechanisms]
  defstruct [:guarantees, :mechanisms]

  @typedoc "Raw boolean map passed to `new/1` before defaults are filled."
  @type flags :: %{required(atom()) => boolean()}

  @typedoc "Declared guarantees and mechanisms; every known key is present."
  @type t :: %__MODULE__{guarantees: flags(), mechanisms: flags()}

  @doc "Known guarantee keys."
  @spec guarantee_keys() :: [atom()]
  def guarantee_keys, do: @guarantee_keys

  @doc "Known mechanism keys."
  @spec mechanism_keys() :: [atom()]
  def mechanism_keys, do: @mechanism_keys

  @doc """
  Builds a validated declaration.

  ## Arguments

    * `flags` — keyword list or map of known keys to booleans. Omitted keys are
      `false`, except the invariants, which are `true`.

  ## Errors

  Raises `ArgumentError` on an unknown key, a non-boolean value, or an
  invariant declared `false`.
  """
  @spec new(keyword() | map()) :: t()
  def new(flags) when is_list(flags), do: flags |> Map.new() |> new()

  def new(%{} = flags) do
    Enum.each(flags, &validate!/1)

    %__MODULE__{
      guarantees: build(@guarantee_keys, flags),
      mechanisms: build(@mechanism_keys, flags)
    }
  end

  @doc "Whether the backend declares guarantee `key`."
  @spec guarantee?(t(), atom()) :: boolean()
  def guarantee?(%__MODULE__{guarantees: guarantees}, key) when key in @guarantee_keys do
    Map.fetch!(guarantees, key)
  end

  @doc "Whether the backend declares mechanism `key`."
  @spec mechanism?(t(), atom()) :: boolean()
  def mechanism?(%__MODULE__{mechanisms: mechanisms}, key) when key in @mechanism_keys do
    Map.fetch!(mechanisms, key)
  end

  defp build(keys, flags) do
    Map.new(keys, fn key -> {key, Map.get(flags, key, key in @invariants)} end)
  end

  defp validate!({key, value}) when key in @guarantee_keys or key in @mechanism_keys do
    cond do
      not is_boolean(value) ->
        raise ArgumentError,
              "capability #{inspect(key)} must be a boolean, got: #{inspect(value)}"

      key in @invariants and value == false ->
        raise ArgumentError,
              "capability #{inspect(key)} is a Rheo invariant and cannot be false"

      true ->
        :ok
    end
  end

  defp validate!({key, _value}) do
    raise ArgumentError, "unknown capability key: #{inspect(key)}"
  end
end
