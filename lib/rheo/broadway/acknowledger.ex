defmodule Rheo.Broadway.Acknowledger do
  @moduledoc """
  `Broadway.Acknowledger` that settles Rheo leases.

  Attached by `Rheo.Broadway.transform/2`; you never build it by hand. Broadway
  calls `ack/3` once per producer with the batch of successful and failed
  messages, and each lease is settled with its own fencing token:

  | Outcome | Call |
  |---|---|
  | successful | `Rheo.ack/2` |
  | failed | `Rheo.nack/3` (default) |
  | failed, `on_failure: :reject` | `Rheo.reject/3` |

  `Rheo.nack/3` returns the delivery for retry, or dead-letters it once
  `attempt >= max_attempts` for the group. `Rheo.reject/3` dead-letters
  immediately.

  After settling, each lease is reported back to the producer with
  `Rheo.Producer.confirm/2`, which stops renewal and frees a `:max_demand` slot.
  Confirmation happens even when the settle call fails, because a lease that
  cannot be settled has already been fenced — the backend will redeliver it.
  Failed settles emit `[:rheo, :ack, :error]`, `[:rheo, :retry, :error]`, or
  `[:rheo, :reject, :error]` with the `event_id` and reason.

  ## Per-message override

      Broadway.Message.configure_ack(message, on_failure: :reject)

  See `Rheo.Broadway` and ADR 018.
  """

  @behaviour Broadway.Acknowledger

  require Logger

  alias Broadway.Message
  alias Rheo.{Producer, Telemetry}

  @impl true
  def ack(producer, successful, failed) do
    confirmed =
      Enum.map(successful, &settle(&1, :ack)) ++ Enum.map(failed, &settle(&1, :failed))

    case confirmed do
      [] -> :ok
      lease_ids -> Producer.confirm(producer, lease_ids)
    end
  end

  @impl true
  def configure(_producer, ack_data, options) do
    {:ok, Enum.reduce(options, ack_data, &put_option/2)}
  end

  defp put_option({:on_failure, on_failure}, ack_data) when on_failure in [:nack, :reject] do
    %{ack_data | on_failure: on_failure}
  end

  defp put_option({:rheo, rheo}, ack_data) when is_atom(rheo) do
    %{ack_data | rheo: rheo}
  end

  defp put_option({key, value}, _ack_data) do
    raise ArgumentError,
          "unsupported Rheo.Broadway.Acknowledger option: #{inspect(key)} => #{inspect(value)}"
  end

  defp settle(%Message{acknowledger: {_mod, _ref, ack_data}} = message, outcome) do
    %{lease: lease, rheo: rheo} = ack_data

    case settle_outcome(ack_data, outcome, message) do
      {op, :ok} -> emit_settled(rheo, op, lease)
      {op, {:error, reason}} -> emit_error(rheo, op, lease, reason)
    end

    lease.lease_id
  end

  defp settle_outcome(%{lease: lease, rheo: rheo}, :ack, _message) do
    {:ack, Rheo.ack(lease, rheo: rheo)}
  end

  defp settle_outcome(%{lease: lease, rheo: rheo, on_failure: :reject}, :failed, message) do
    {:reject, Rheo.reject(lease, failure_reason(message), rheo: rheo)}
  end

  defp settle_outcome(%{lease: lease, rheo: rheo}, :failed, message) do
    {:retry, Rheo.nack(lease, failure_reason(message), rheo: rheo)}
  end

  defp failure_reason(%Message{status: {:failed, reason}}), do: reason
  defp failure_reason(%Message{status: status}), do: status

  defp emit_settled(rheo, op, lease) do
    Telemetry.execute([:rheo, :broadway, op], %{count: 1}, %{
      rheo: rheo,
      stream: lease.stream,
      group: lease.group,
      event_id: lease.event_id
    })
  end

  defp emit_error(rheo, op, lease, reason) do
    Logger.warning(
      "Rheo.Broadway.Acknowledger #{op} failed stream=#{lease.stream} " <>
        "group=#{lease.group} event_id=#{lease.event_id} reason=#{inspect(reason)}"
    )

    Telemetry.execute([:rheo, op, :error], %{count: 1}, %{
      rheo: rheo,
      stream: lease.stream,
      group: lease.group,
      event_id: lease.event_id,
      reason: reason
    })
  end
end
