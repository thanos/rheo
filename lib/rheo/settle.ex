defmodule Rheo.Settle do
  @moduledoc """
  Portable settlement result vocabulary (v0.8).

  Backends and runtimes should map storage failures into these atoms rather
  than leaking driver exceptions as the public contract.
  """

  @type result :: :ok | {:error, reason()}
  @type reason ::
          :stale_lease
          | :receipt_mismatch
          | :backend_unavailable
          | :ambiguous
          | term()

  @doc "Classifies a settle/renew error into a portable reason."
  @spec classify(term()) :: reason()
  def classify(:stale_lease), do: :stale_lease
  def classify(:receipt_mismatch), do: :receipt_mismatch
  def classify(:backend_unavailable), do: :backend_unavailable
  def classify({:error, reason}), do: classify(reason)

  def classify(reason) when is_atom(reason), do: reason
  def classify(_other), do: :ambiguous

  @doc "True when the holder must treat the lease as no longer owned."
  @spec lost?(reason()) :: boolean()
  def lost?(reason), do: classify(reason) in [:stale_lease, :receipt_mismatch]
end
