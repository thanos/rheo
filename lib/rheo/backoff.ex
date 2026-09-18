defmodule Rheo.Backoff do
  @moduledoc """
  Exponential backoff schedule shared by `Rheo.Group` and `Rheo.Producer`.

  A runtime holds the current delay in its state (`0` when healthy). Each
  consecutive fetch failure doubles the delay from `min_ms/0` up to `max_ms/0`;
  a successful fetch resets it to `0`.

  ## Examples

      iex> Rheo.Backoff.next(0)
      100

      iex> Rheo.Backoff.next(100)
      200

      iex> Rheo.Backoff.next(4_000)
      5000
  """

  @min_ms 100
  @max_ms 5_000

  @doc "Smallest non-zero delay."
  @spec min_ms() :: pos_integer()
  def min_ms, do: @min_ms

  @doc "Largest delay."
  @spec max_ms() :: pos_integer()
  def max_ms, do: @max_ms

  @doc "Next delay after `current_ms` (`0` means no backoff yet)."
  @spec next(non_neg_integer()) :: pos_integer()
  def next(0), do: @min_ms

  def next(current_ms) when is_integer(current_ms) and current_ms > 0,
    do: min(current_ms * 2, @max_ms)
end
