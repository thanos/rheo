defmodule Rheo.Clock.Frozen do
  @moduledoc """
  Controllable clock for tests and demos.

  Configure `config :rheo, clock: Rheo.Clock.Frozen` then call `set/1` or
  `advance/1` to simulate lease expiry without sleeping.
  """
  @behaviour Rheo.Clock

  @table :rheo_frozen_clock

  @doc """
  Returns the frozen clock's current time.

  When unset, falls back to the real wall clock.

  ## Examples

      iex> Rheo.Clock.Frozen.set(~U[2026-01-15 12:00:00.000Z])
      :ok
      iex> Rheo.Clock.Frozen.utc_now()
      ~U[2026-01-15 12:00:00.000Z]

  ## Returns

  A `DateTime.t()`.
  """
  @impl true
  @spec utc_now() :: DateTime.t()
  def utc_now do
    ensure_table()

    case :ets.lookup(@table, :now) do
      [{:now, %DateTime{} = dt}] -> dt
      [] -> DateTime.utc_now() |> DateTime.truncate(:millisecond)
    end
  end

  @doc """
  Sets the frozen clock to a specific `DateTime`.

  ## Examples

      iex> Rheo.Clock.Frozen.set(~U[2026-06-01 08:30:00.000Z])
      :ok
      iex> Rheo.Clock.Frozen.utc_now()
      ~U[2026-06-01 08:30:00.000Z]

  ## Arguments

    * `dt` — UTC `DateTime.t()`; truncated to milliseconds

  ## Returns

  `:ok`
  """
  @spec set(DateTime.t()) :: :ok
  def set(%DateTime{} = dt) do
    ensure_table()
    :ets.insert(@table, {:now, DateTime.truncate(dt, :millisecond)})
    :ok
  end

  @doc """
  Advances the frozen clock by `ms` milliseconds.

  ## Examples

      iex> Rheo.Clock.Frozen.set(~U[2026-01-15 12:00:00.000Z])
      :ok
      iex> Rheo.Clock.Frozen.advance(1_500)
      :ok
      iex> Rheo.Clock.Frozen.utc_now()
      ~U[2026-01-15 12:00:01.500Z]

  ## Arguments

    * `ms` — non-negative integer milliseconds

  ## Returns

  `:ok`

  ## Errors

  Raises `FunctionClauseError` when `ms` is negative or not an integer.
  """
  @spec advance(non_neg_integer()) :: :ok
  def advance(ms) when is_integer(ms) and ms >= 0 do
    now = utc_now()
    set(DateTime.add(now, ms, :millisecond))
  end

  @doc """
  Clears the frozen value so `utc_now/0` uses the wall clock again.

  ## Examples

      iex> Rheo.Clock.Frozen.set(~U[2026-01-15 12:00:00.000Z])
      :ok
      iex> Rheo.Clock.Frozen.reset()
      :ok
      iex> match?(%DateTime{}, Rheo.Clock.Frozen.utc_now())
      true

  ## Returns

  `:ok`
  """
  @spec reset() :: :ok
  def reset do
    ensure_table()
    :ets.delete(@table, :now)
    :ok
  end

  defp ensure_table do
    case :ets.whereis(@table) do
      :undefined ->
        try do
          :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])
        rescue
          ArgumentError -> :ok
        end

      _ ->
        :ok
    end
  end
end
