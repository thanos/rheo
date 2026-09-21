defmodule Rheo.Settle do
  @moduledoc """
  Portable settlement result vocabulary and the runtime policy built on it.

  Backends map storage and driver failures into this vocabulary at their
  boundary; `Rheo.Group`, `Rheo.Producer`, and `Rheo.Broadway.Acknowledger`
  emit it in telemetry and decide what to do with an unsettled lease.

  ## Reasons

  | Reason | Meaning |
  |---|---|
  | `:stale_lease` | The fencing token no longer matches; another holder owns the delivery |
  | `:receipt_mismatch` | The backend-native receipt does not match the current claim |
  | `:backend_unavailable` | The backend could not be reached; the write may or may not have happened |
  | `{:ambiguous, cause}` | The backend reported that the outcome is unknown |
  | `{:failed, cause}` | The backend definitely did not apply the operation |
  | `{:invalid, cause}` | The request was malformed or referenced a missing stream / group |

  Portable domain atoms returned by backends (`:group_not_found`,
  `:stream_not_found`, …) pass through `classify/1` unchanged.

  ## Examples

      iex> Rheo.Settle.classify({:error, :stale_lease})
      :stale_lease

      iex> Rheo.Settle.classify(%RuntimeError{message: "boom"})
      {:failed, %RuntimeError{message: "boom"}}

      iex> Rheo.Settle.lost?(:receipt_mismatch)
      true

      iex> Rheo.Settle.nack_after_failed_ack?(:backend_unavailable)
      false
  """

  @typedoc """
  Portable settle / renew / fetch failure reason.

  See the module documentation for the full table. Domain atoms such as
  `:stream_not_found` pass through `classify/1` unchanged.
  """
  @type reason ::
          :stale_lease
          | :receipt_mismatch
          | :backend_unavailable
          | {:ambiguous, term()}
          | {:failed, term()}
          | {:invalid, term()}
          | atom()

  @doc """
  Classifies a settle, renew, or fetch error into a portable reason.

  Accepts a bare reason or an `{:error, reason}` tuple. Non-atom terms that
  carry no explicit category become `{:failed, term}`.
  """
  @spec classify(term()) :: reason()
  def classify({:error, reason}), do: classify(reason)
  def classify({:ambiguous, _cause} = reason), do: reason
  def classify({:failed, _cause} = reason), do: reason
  def classify({:invalid, _cause} = reason), do: reason
  def classify(reason) when is_atom(reason), do: reason
  def classify(other), do: {:failed, other}

  @doc "True when the holder no longer owns the lease and must stop renewing it."
  @spec lost?(term()) :: boolean()
  def lost?(reason), do: classify(reason) in [:stale_lease, :receipt_mismatch]

  @doc """
  Decides whether a lease should be handed back with `Rheo.nack/3` after an
  ACK failed.

  Returns `false` when the holder has lost the lease, when the backend is
  unavailable, or when the outcome is ambiguous: the ACK may already be durable,
  and a nack would either be fenced or be lost as well. Lease expiry redelivers
  the event in those cases. Returns `true` for definite failures, where the
  delivery is still leased and can be released immediately.
  """
  @spec nack_after_failed_ack?(term()) :: boolean()
  def nack_after_failed_ack?(reason) do
    case classify(reason) do
      :stale_lease -> false
      :receipt_mismatch -> false
      :backend_unavailable -> false
      {:ambiguous, _} -> false
      _definite -> true
    end
  end
end
