# Optional integration: compiled only when `:broadway` is present.
if Code.ensure_loaded?(Broadway) do
  defmodule Rheo.Broadway.Acknowledger do
    @moduledoc """
    `Broadway.Acknowledger` that settles Rheo leases.

    Attached by `Rheo.Broadway.transform/2`; you never build it by hand. Broadway
    calls `ack/3` once per producer with the batch of successful and failed
    messages, and each lease is settled with its own fencing token through
    `Rheo.Producer.ack/3`, `nack/4`, or `reject/4`:

    | Outcome | Call |
    |---|---|
    | successful | `Rheo.Producer.ack/3` |
    | failed | `Rheo.Producer.nack/4` (default) |
    | failed, `on_failure: :reject` | `Rheo.Producer.reject/4` |

    `nack` returns the delivery for retry, or dead-letters it once
    `attempt >= max_attempts` for the group. `reject` dead-letters immediately.
    Each helper also releases the producer's inflight entry, so renewal stops and
    a `:max_demand` slot is freed even when the durable settle fails — a lease
    that cannot be settled has been fenced or will expire, and the backend
    redelivers it. Failed settles emit `[:rheo, :ack, :error]`,
    `[:rheo, :retry, :error]`, or `[:rheo, :reject, :error]` with the `event_id`
    and a `Rheo.Settle` reason.

    ## Per-message override

        Broadway.Message.configure_ack(message, on_failure: :reject)

    See `Rheo.Broadway` and ADR 018.
    """

    @behaviour Broadway.Acknowledger

    require Logger

    alias Broadway.Message
    alias Rheo.{Producer, Settle, Telemetry}

    @impl true
    def ack(producer, successful, failed) do
      Enum.each(successful, &settle(producer, &1, :ack))
      Enum.each(failed, &settle(producer, &1, :failed))
      :ok
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

    defp settle(producer, %Message{acknowledger: {_mod, _ref, ack_data}} = message, outcome) do
      %{lease: lease, rheo: rheo} = ack_data

      case settle_outcome(producer, ack_data, outcome, message) do
        {op, :ok} -> emit_settled(rheo, op, lease)
        {op, {:error, reason}} -> emit_error(rheo, op, lease, Settle.classify(reason))
      end
    end

    defp settle_outcome(producer, %{lease: lease, rheo: rheo}, :ack, _message) do
      {:ack, Producer.ack(producer, lease, rheo: rheo)}
    end

    defp settle_outcome(producer, %{lease: lease, rheo: rheo, on_failure: :reject}, :failed, msg) do
      {:reject, Producer.reject(producer, lease, failure_reason(msg), rheo: rheo)}
    end

    defp settle_outcome(producer, %{lease: lease, rheo: rheo}, :failed, message) do
      {:retry, Producer.nack(producer, lease, failure_reason(message), rheo: rheo)}
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
end
