defmodule Rheo.Backend.Capabilities do
  @moduledoc """
  Typed backend capability model (v0.8 / ADR 023).

  Separates **semantic guarantees** (what Rheo promises through this backend)
  from **mechanisms** (how the backend implements or optimizes delivery).

  Flat boolean maps from v0.3–v0.7 remain accepted via `normalize/1` for
  migration; prefer `new/1` for new backends.
  """

  @guarantee_keys [
    :durable,
    :distributed,
    :at_least_once,
    :lease_fencing,
    :partitions,
    :contiguous_frontier,
    :replay,
    :ordered_range_scan,
    :secondary_indexes,
    :batch_writes
  ]

  @mechanism_keys [
    :atomic_compare_and_set,
    :notifications,
    :change_feed,
    :native_consumer_groups,
    :native_pending_list,
    :native_reclaim,
    :blocking_reads,
    :native_group_lag
  ]

  @enforce_keys [:guarantees, :mechanisms]
  defstruct [:guarantees, :mechanisms]

  @type guarantees :: %{optional(atom()) => boolean()}
  @type mechanisms :: %{optional(atom()) => boolean()}

  @type t :: %__MODULE__{
          guarantees: guarantees(),
          mechanisms: mechanisms()
        }

  @doc "Known guarantee keys."
  @spec guarantee_keys() :: [atom()]
  def guarantee_keys, do: @guarantee_keys

  @doc "Known mechanism keys."
  @spec mechanism_keys() :: [atom()]
  def mechanism_keys, do: @mechanism_keys

  @doc """
  Builds a capabilities struct from keyword or map options.

  ## Options

    * `:guarantees` — map of guarantee flags
    * `:mechanisms` — map of mechanism flags
    * legacy flat keys (`:durable`, `:notifications`, …) are accepted and split
  """
  @spec new(keyword() | map()) :: t()
  def new(opts) when is_list(opts), do: opts |> Map.new() |> new()

  def new(%{} = opts) do
    {guarantees, mechanisms, rest} = split_legacy(opts)

    guarantees =
      guarantees
      |> Map.merge(Map.get(opts, :guarantees, %{}))
      |> Map.put_new(:at_least_once, true)
      |> Map.put_new(:lease_fencing, true)

    mechanisms = Map.merge(mechanisms, Map.get(opts, :mechanisms, %{}))

    if map_size(rest) > 0 do
      unknown = Map.keys(rest) -- [:guarantees, :mechanisms]
      if unknown != [], do: raise(ArgumentError, "unknown capability keys: #{inspect(unknown)}")
    end

    %__MODULE__{guarantees: guarantees, mechanisms: mechanisms}
  end

  @doc """
  Normalizes a v0.7 flat capability map or a `%__MODULE__{}` into `%__MODULE__{}`.
  """
  @spec normalize(t() | map()) :: t()
  def normalize(%__MODULE__{} = caps), do: caps
  def normalize(%{} = flat), do: new(flat)

  @doc "Returns whether a guarantee is claimed."
  @spec guarantee?(t() | map(), atom()) :: boolean()
  def guarantee?(caps, key) when is_atom(key) do
    caps |> normalize() |> Map.fetch!(:guarantees) |> Map.get(key, false)
  end

  @doc "Returns whether a mechanism is claimed."
  @spec mechanism?(t() | map(), atom()) :: boolean()
  def mechanism?(caps, key) when is_atom(key) do
    caps |> normalize() |> Map.fetch!(:mechanisms) |> Map.get(key, false)
  end

  @doc """
  Flattens to the legacy map shape used by older conformance helpers.
  """
  @spec to_legacy_map(t() | map()) :: map()
  def to_legacy_map(caps) do
    %__MODULE__{guarantees: g, mechanisms: m} = normalize(caps)
    Map.merge(g, m)
  end

  defp split_legacy(opts) do
    Enum.reduce(opts, {%{}, %{}, %{}}, fn
      {k, v}, {g, m, rest} when k in @guarantee_keys ->
        {Map.put(g, k, v), m, rest}

      {k, v}, {g, m, rest} when k in @mechanism_keys ->
        {g, Map.put(m, k, v), rest}

      {k, v}, {g, m, rest} ->
        {g, m, Map.put(rest, k, v)}
    end)
  end
end
