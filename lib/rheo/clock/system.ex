defmodule Rheo.Clock.System do
  @moduledoc """
  Wall-clock implementation of `Rheo.Clock`.

  Uses `DateTime.utc_now/0` truncated to milliseconds. This is the default clock
  in non-test environments.
  """
  @behaviour Rheo.Clock

  @doc """
  Returns the current UTC wall-clock time.

  ## Examples

      iex> match?(%DateTime{time_zone: "Etc/UTC"}, Rheo.Clock.System.utc_now())
      true

  ## Returns

  A `DateTime.t()` truncated to milliseconds.
  """
  @impl true
  @spec utc_now() :: DateTime.t()
  def utc_now, do: DateTime.utc_now() |> DateTime.truncate(:millisecond)
end
