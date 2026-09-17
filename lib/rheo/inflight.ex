defmodule Rheo.Inflight do
  @moduledoc """
  Shared inflight-lease bookkeeping for `Rheo.Group` and `Rheo.Producer` (v0.8).

  Keys are opaque terms (task refs for Group, `lease_id` for Producer). Values
  are maps that at least contain `:lease`.
  """

  alias Rheo.Lease

  @type t :: %{optional(term()) => map()}

  @doc "Empty inflight set."
  @spec new() :: t()
  def new, do: %{}

  @doc "Number of unsettled leases."
  @spec size(t()) :: non_neg_integer()
  def size(inflight) when is_map(inflight), do: map_size(inflight)

  @doc "Capacity left given max demand."
  @spec capacity(t(), non_neg_integer()) :: non_neg_integer()
  def capacity(inflight, max_demand)
      when is_map(inflight) and is_integer(max_demand) and max_demand >= 0 do
    max(max_demand - map_size(inflight), 0)
  end

  @doc "Tracks a lease under `key`."
  @spec put(t(), term(), Lease.t(), map()) :: t()
  def put(inflight, key, %Lease{} = lease, meta \\ %{})
      when is_map(inflight) and is_map(meta) do
    Map.put(inflight, key, Map.put(meta, :lease, lease))
  end

  @doc "Drops one key."
  @spec delete(t(), term()) :: t()
  def delete(inflight, key) when is_map(inflight), do: Map.delete(inflight, key)

  @doc "Drops many keys."
  @spec drop(t(), [term()]) :: t()
  def drop(inflight, keys) when is_map(inflight) and is_list(keys),
    do: Map.drop(inflight, keys)

  @doc "Fetches the lease for `key`."
  @spec fetch_lease(t(), term()) :: {:ok, Lease.t()} | :error
  def fetch_lease(inflight, key) when is_map(inflight) do
    case Map.fetch(inflight, key) do
      {:ok, %{lease: %Lease{} = lease}} -> {:ok, lease}
      _ -> :error
    end
  end

  @doc "Replaces the lease for an existing key."
  @spec update_lease(t(), term(), Lease.t()) :: t()
  def update_lease(inflight, key, %Lease{} = lease) when is_map(inflight) do
    case Map.fetch(inflight, key) do
      {:ok, meta} -> Map.put(inflight, key, %{meta | lease: lease})
      :error -> inflight
    end
  end

  @doc "All tracked leases."
  @spec leases(t()) :: [Lease.t()]
  def leases(inflight) when is_map(inflight) do
    inflight
    |> Map.values()
    |> Enum.map(& &1.lease)
  end
end
