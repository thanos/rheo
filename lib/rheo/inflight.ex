defmodule Rheo.Inflight do
  @moduledoc """
  Inflight-lease bookkeeping shared by `Rheo.Group` and `Rheo.Producer`.

  An inflight set maps an opaque key (a task monitor reference for the Group,
  a `lease_id` for the Producer) to a metadata map that always contains
  `:lease`. All functions are pure; the owning process performs backend calls
  and telemetry around them.

  ## Examples

      iex> event = %Rheo.Event{id: "e1", stream: "s", partition: 0, sequence: 1,
      ...>   timestamp: ~U[2026-01-01 00:00:00.000Z], payload: %{}}
      iex> lease = %Rheo.Lease{lease_id: "l1", stream: "s", group: "g", event_id: "e1",
      ...>   event: event, consumer_id: "c", attempt: 1,
      ...>   leased_at: ~U[2026-01-01 00:00:00.000Z], expires_at: ~U[2026-01-01 00:00:30.000Z]}
      iex> inflight = Rheo.Inflight.put(Rheo.Inflight.new(), "l1", lease)
      iex> {Rheo.Inflight.size(inflight), Rheo.Inflight.capacity(inflight, 10)}
      {1, 9}
      iex> {:ok, %{lease: ^lease}, inflight} = Rheo.Inflight.pop(inflight, "l1")
      iex> Rheo.Inflight.size(inflight)
      0
  """

  alias Rheo.{Lease, Settle}

  @typedoc "Opaque key for an inflight entry (usually a monitor reference)."
  @type key :: term()

  @typedoc "Metadata stored with an inflight lease (must include `:lease`)."
  @type meta :: %{required(:lease) => Lease.t(), optional(atom()) => term()}

  @typedoc "Map of inflight keys to lease metadata."
  @type t :: %{optional(key()) => meta()}

  @typedoc "Outcome of renewing one tracked lease."
  @type renew_result :: {key(), Lease.t(), :ok | Settle.reason()}

  @doc "Empty inflight set."
  @spec new() :: t()
  def new, do: %{}

  @doc "Number of unsettled leases."
  @spec size(t()) :: non_neg_integer()
  def size(inflight) when is_map(inflight), do: map_size(inflight)

  @doc "Slots left before `max_demand` unsettled leases are held."
  @spec capacity(t(), non_neg_integer()) :: non_neg_integer()
  def capacity(inflight, max_demand)
      when is_map(inflight) and is_integer(max_demand) and max_demand >= 0 do
    max(max_demand - map_size(inflight), 0)
  end

  @doc "Tracks `lease` under `key`, merging `meta` into the stored metadata."
  @spec put(t(), key(), Lease.t(), map()) :: t()
  def put(inflight, key, %Lease{} = lease, meta \\ %{})
      when is_map(inflight) and is_map(meta) do
    Map.put(inflight, key, Map.put(meta, :lease, lease))
  end

  @doc "Removes `key`, returning its metadata when it was tracked."
  @spec pop(t(), key()) :: {:ok, meta(), t()} | :error
  def pop(inflight, key) when is_map(inflight) do
    case Map.pop(inflight, key) do
      {nil, _inflight} -> :error
      {meta, inflight} -> {:ok, meta, inflight}
    end
  end

  @doc "Drops one key."
  @spec delete(t(), key()) :: t()
  def delete(inflight, key) when is_map(inflight), do: Map.delete(inflight, key)

  @doc "Drops many keys."
  @spec drop(t(), [key()]) :: t()
  def drop(inflight, keys) when is_map(inflight) and is_list(keys),
    do: Map.drop(inflight, keys)

  @doc "Fetches the lease tracked under `key`."
  @spec fetch_lease(t(), key()) :: {:ok, Lease.t()} | :error
  def fetch_lease(inflight, key) when is_map(inflight) do
    case Map.fetch(inflight, key) do
      {:ok, %{lease: %Lease{} = lease}} -> {:ok, lease}
      _ -> :error
    end
  end

  @doc "Replaces the lease for an existing key; unknown keys are ignored."
  @spec update_lease(t(), key(), Lease.t()) :: t()
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

  @doc """
  Renews every tracked lease with `renew_fun`.

  Renewed leases replace the stored lease. Leases the holder has lost
  (`Rheo.Settle.lost?/1`) are dropped so the backend can redeliver them; other
  failures keep the lease tracked for the next renewal round. Returns the
  updated set and one `t:renew_result/0` per lease so the caller can emit
  telemetry.
  """
  @spec renew_all(t(), (Lease.t() -> {:ok, Lease.t()} | {:error, term()})) ::
          {t(), [renew_result()]}
  def renew_all(inflight, renew_fun) when is_map(inflight) and is_function(renew_fun, 1) do
    Enum.reduce(inflight, {inflight, []}, fn {key, %{lease: lease}}, {acc, results} ->
      case renew_fun.(lease) do
        {:ok, %Lease{} = renewed} ->
          {update_lease(acc, key, renewed), [{key, renewed, :ok} | results]}

        {:error, reason} ->
          classified = Settle.classify(reason)
          acc = if Settle.lost?(classified), do: Map.delete(acc, key), else: acc
          {acc, [{key, lease, classified} | results]}
      end
    end)
  end
end
