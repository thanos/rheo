defmodule Rheo.Id do
  @moduledoc """
  Opaque identifier generation for events, leases, and consumer ids.

  Rheo treats these strings as opaque tokens: backends and the facade generate
  them when callers omit `:id` / `:consumer_id`, and fencing compares
  `lease_id` (and optional `receipt`) by term equality. Callers may supply
  their own ids on append when they need deterministic event keys; otherwise
  prefer `generate/0`.
  """

  @doc """
  Generates a URL-safe random identifier.

  Uses cryptographically strong randomness (`:crypto.strong_rand_bytes/1`) and
  Base64 URL encoding without padding. Suitable for event ids, lease tokens,
  and consumer ids.

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
