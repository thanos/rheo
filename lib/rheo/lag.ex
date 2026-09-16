defmodule Rheo.Lag do
  @moduledoc """
  Per-partition and aggregate consumer lag (v0.5+).

  Lag for a partition is `max(high_watermark - frontier, 0)`. Aggregate `lag`
  is the **sum** of per-partition lags.
  """

  @enforce_keys [:stream, :group, :partitions, :lag]
  defstruct stream: nil, group: nil, partitions: %{}, lag: 0

  @type partition_lag :: %{
          frontier: non_neg_integer(),
          high_watermark: non_neg_integer(),
          lag: non_neg_integer()
        }

  @type t :: %__MODULE__{
          stream: String.t(),
          group: String.t(),
          partitions: %{optional(non_neg_integer()) => partition_lag()},
          lag: non_neg_integer()
        }

  @doc "Builds a lag struct from per-partition frontier/HW maps."
  @spec build(String.t(), String.t(), %{optional(non_neg_integer()) => partition_lag()}) :: t()
  def build(stream, group, partitions) when is_map(partitions) do
    total =
      partitions
      |> Map.values()
      |> Enum.reduce(0, fn %{lag: lag}, acc -> acc + lag end)

    %__MODULE__{stream: stream, group: group, partitions: partitions, lag: total}
  end

  @doc "Computes lag entries for each partition given frontier and high-watermark maps."
  @spec from_maps(String.t(), String.t(), map(), map(), [non_neg_integer()]) :: t()
  def from_maps(stream, group, frontiers, high_watermarks, partition_list) do
    parts =
      Map.new(partition_list, fn p ->
        frontier = Rheo.Partition.map_get(frontiers, p, 0)
        hw = Rheo.Partition.map_get(high_watermarks, p, 0)
        lag = max(hw - frontier, 0)
        {p, %{frontier: frontier, high_watermark: hw, lag: lag}}
      end)

    build(stream, group, parts)
  end
end
