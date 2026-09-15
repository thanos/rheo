defmodule Rheo.Id do
  @moduledoc """
  Opaque identifier generation for events, leases, and consumer ids.
  """

  @doc """
  Generates a URL-safe random identifier.

  Uses cryptographically strong randomness. Suitable for event ids, lease
  tokens, and consumer ids.

  ## Examples

      iex> id = Rheo.Id.generate()
      iex> is_binary(id) and byte_size(id) > 10
      true

      iex> Rheo.Id.generate() != Rheo.Id.generate()
      true

  ## Returns

  A `String.t()` identifier. Does not raise under normal conditions.
  """
  @spec generate() :: String.t()
  def generate do
    16
    |> :crypto.strong_rand_bytes()
    |> Base.url_encode64(padding: false)
  end
end
