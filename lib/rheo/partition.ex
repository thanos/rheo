defmodule Rheo.Partition do
  @moduledoc """
  Partition routing helpers for multi-partition streams (v0.5+).

  Routing uses `:erlang.phash2/2` (stable on the BEAM; not portable to other
  runtimes). Prefer an explicit `:partition` when you need a fixed assignment.
  """

  @doc """
  Resolves the target partition for an append.

  Precedence: explicit `:partition` in `opts`, else `:key` in opts or payload,
  else `0`. Returns `{:error, :invalid_partition}` when out of range.
  """
  @spec resolve(map(), keyword(), pos_integer()) ::
          {:ok, non_neg_integer()} | {:error, :invalid_partition}
  def resolve(payload, opts, partition_count)
      when is_map(payload) and is_list(opts) and is_integer(partition_count) and
             partition_count >= 1 do
    if Keyword.has_key?(opts, :partition) do
      validate(Keyword.fetch!(opts, :partition), partition_count)
    else
      key = Keyword.get(opts, :key) || Map.get(payload, :key) || Map.get(payload, "key")

      if is_nil(key) do
        {:ok, 0}
      else
        {:ok, :erlang.phash2(to_string(key), partition_count)}
      end
    end
  end

  @doc "Returns `{:ok, p}` when `p` is in `0..partition_count-1`."
  @spec validate(term(), pos_integer()) ::
          {:ok, non_neg_integer()} | {:error, :invalid_partition}
  def validate(partition, partition_count)
      when is_integer(partition) and partition >= 0 and partition < partition_count do
    {:ok, partition}
  end

  def validate(_, _), do: {:error, :invalid_partition}

  @doc "Normalizes `:partition` / `:partitions` / `:all` into a sorted unique list."
  @spec normalize_assignment(term(), pos_integer()) ::
          {:ok, [non_neg_integer()]} | {:error, :invalid_partition}
  def normalize_assignment(:all, partition_count) when partition_count >= 1 do
    {:ok, Enum.to_list(0..(partition_count - 1))}
  end

  def normalize_assignment(nil, partition_count), do: normalize_assignment(:all, partition_count)

  def normalize_assignment(partition, partition_count) when is_integer(partition) do
    with {:ok, p} <- validate(partition, partition_count), do: {:ok, [p]}
  end

  def normalize_assignment(partitions, partition_count) when is_list(partitions) do
    partitions
    |> Enum.reduce_while({:ok, []}, fn p, {:ok, acc} ->
      case validate(p, partition_count) do
        {:ok, ok} -> {:cont, {:ok, [ok | acc]}}
        {:error, _} = err -> {:halt, err}
      end
    end)
    |> case do
      {:ok, list} -> {:ok, list |> Enum.reverse() |> Enum.uniq() |> Enum.sort()}
      other -> other
    end
  end

  def normalize_assignment(_, _), do: {:error, :invalid_partition}

  @doc ~S|String key for maps stored in Mongo ("0", "1", …).|
  @spec key(non_neg_integer()) :: String.t()
  def key(partition) when is_integer(partition) and partition >= 0,
    do: Integer.to_string(partition)

  @doc "Reads an integer from a string-or-atom keyed map (Mongo/ETS)."
  @spec map_get(map(), non_neg_integer(), non_neg_integer()) :: non_neg_integer()
  def map_get(map, partition, default \\ 0) when is_map(map) do
    k = key(partition)

    cond do
      Map.has_key?(map, k) -> Map.get(map, k) || default
      Map.has_key?(map, partition) -> Map.get(map, partition) || default
      true -> default
    end
  end
end
