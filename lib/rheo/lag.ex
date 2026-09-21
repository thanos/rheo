defmodule Rheo.Lag do
  @moduledoc """
  Per-partition and aggregate consumer lag.

  Lag for a partition is `max(high_watermark - frontier, 0)`. Aggregate `lag`
  is the **sum** of per-partition lags. Hosts normally call `Rheo.lag/3`;
  `build/3` and `from_maps/5` are for backends and tests assembling the struct.

  ```
  partition 0:  HW=10  frontier=7  → lag 3
  partition 1:  HW=5   frontier=5  → lag 0
                                 aggregate lag 3
  ```

  ## Fields

  | Field | Type | Meaning |
  |---|---|---|
  | `stream` | `String.t()` | Stream name |
  | `group` | `String.t()` | Consumer group name |
  | `partitions` | `%{partition => partition_lag()}` | Per-partition breakdown |
  | `lag` | `non_neg_integer()` | Sum of per-partition lags |

  ## `partition_lag` map

  | Key | Meaning |
  |---|---|
  | `:frontier` | Contiguous committed sequence for the group on that partition |
  | `:high_watermark` | Highest sequence written on that partition |
  | `:lag` | `max(high_watermark - frontier, 0)` |

  ## Example

      iex> lag = Rheo.Lag.build("market-events", "risk", %{
      ...>   0 => %{frontier: 7, high_watermark: 10, lag: 3},
      ...>   1 => %{frontier: 5, high_watermark: 5, lag: 0}
      ...> })
      iex> {lag.lag, lag.partitions[0].lag}
      {3, 3}
  """

  @enforce_keys [:stream, :group, :partitions, :lag]
  defstruct stream: nil, group: nil, partitions: %{}, lag: 0

  @typedoc """
  Lag numbers for one partition: frontier, high-watermark, and their difference.
  """
  @type partition_lag :: %{
          frontier: non_neg_integer(),
          high_watermark: non_neg_integer(),
          lag: non_neg_integer()
        }

  @typedoc """
  Aggregate lag for a `{stream, group}` with a per-partition map.

  See the module documentation for field meanings.
  """
  @type t :: %__MODULE__{
          stream: String.t(),
          group: String.t(),
          partitions: %{optional(non_neg_integer()) => partition_lag()},
          lag: non_neg_integer()
        }

  @doc """
  Builds a lag struct from per-partition frontier/HW maps.

  Sums each entry's `:lag` into the aggregate. Does not recompute lag from
  frontier/HW — pass already-calculated values (see `from_maps/5`).

  ## Examples

      iex> Rheo.Lag.build("s", "g", %{0 => %{frontier: 1, high_watermark: 4, lag: 3}}).lag
      3

  ## Returns

  A `%Rheo.Lag{}`. Does not raise when `partitions` is a map.
  """
  @spec build(String.t(), String.t(), %{optional(non_neg_integer()) => partition_lag()}) :: t()
  def build(stream, group, partitions) when is_map(partitions) do
    total =
      partitions
      |> Map.values()
      |> Enum.reduce(0, fn %{lag: lag}, acc -> acc + lag end)

    %__MODULE__{stream: stream, group: group, partitions: partitions, lag: total}
  end

  @doc """
  Computes lag entries for each partition given frontier and high-watermark maps.

  Missing keys default to `0` via `Rheo.Partition.map_get/3`.

  ## Examples

      iex> lag = Rheo.Lag.from_maps("s", "g", %{"0" => 2}, %{"0" => 5}, [0])
      iex> lag.partitions[0]
      %{frontier: 2, high_watermark: 5, lag: 3}

  ## Returns

  A `%Rheo.Lag{}` covering every id in `partition_list`.
  """
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
