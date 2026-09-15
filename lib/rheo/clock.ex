defmodule Rheo.Clock do
  @moduledoc """
  Injectable clock used for lease expiry and event timestamps.

  Rheo reads time through this behaviour so tests can advance time without
  `Process.sleep/1` via `Rheo.Clock.Frozen`.

  ## Callback

  See `c:utc_now/0`.

  ## Example implementation

      defmodule MyApp.Clock do
        @behaviour Rheo.Clock

        @impl true
        def utc_now, do: DateTime.utc_now() |> DateTime.truncate(:millisecond)
      end

  Configure with:

      config :rheo, clock: MyApp.Clock
  """

  @doc """
  Returns the current UTC time.

  ## Examples

      defmodule Demo.Clock do
        @behaviour Rheo.Clock

        @impl true
        def utc_now, do: ~U[2026-01-15 12:00:00.000Z]
      end

  ## Returns

  A `DateTime.t()` in UTC, typically truncated to milliseconds.
  """
  @callback utc_now() :: DateTime.t()

  @doc """
  Returns the configured clock module.

  Defaults to `Rheo.Clock.System` when `:clock` is unset.

  ## Examples

      iex> is_atom(Rheo.Clock.impl())
      true

  ## Returns

  A module implementing `Rheo.Clock`.
  """
  @spec impl() :: module()
  def impl do
    Application.get_env(:rheo, :clock, Rheo.Clock.System)
  end

  @doc """
  Current UTC time according to the configured clock.

  ## Examples

      iex> match?(%DateTime{}, Rheo.Clock.utc_now())
      true

  ## Returns

  A `DateTime.t()`. Delegates to `c:utc_now/0` on `impl/0`.
  """
  @spec utc_now() :: DateTime.t()
  def utc_now, do: impl().utc_now()
end
