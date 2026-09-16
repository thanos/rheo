defmodule Rheo.Broadway do
  @moduledoc """
  Wiring between `Rheo.Producer` and [Broadway](https://hexdocs.pm/broadway).

  Broadway owns the topology — processors, batchers, concurrency, rate limiting.
  Rheo owns durable event-source semantics — leases, fencing, retries,
  dead-letters, partitions, and the contiguous ACK frontier. `transform/2` is the
  seam: it turns each `%Rheo.Lease{}` emitted by `Rheo.Producer` into a
  `%Broadway.Message{}` whose acknowledger settles the lease
  (`Rheo.Broadway.Acknowledger`).

  ## Pipeline

      defmodule MyApp.RiskBroadway do
        use Broadway

        def start_link(_opts) do
          Broadway.start_link(__MODULE__,
            name: __MODULE__,
            producer: [
              module:
                {Rheo.Producer,
                 rheo: MyRheo, stream: "market-events", group: "risk", max_demand: 50},
              transformer: {Rheo.Broadway, :transform, []},
              concurrency: 1
            ],
            processors: [default: [concurrency: 8]]
          )
        end

        @impl true
        def handle_message(_processor, message, _context) do
          case Risk.process(message.data) do
            :ok -> message
            {:error, reason} -> Broadway.Message.failed(message, reason)
          end
        end
      end

  `message.data` is the `%Rheo.Event{}`. Returning the message ACKs the lease;
  `Broadway.Message.failed/2` sends it back for retry (or dead-letters it, see
  "Failure mapping").

  ## Message shape

  | Field | Value |
  |---|---|
  | `data` | `%Rheo.Event{}` |
  | `metadata.lease` | the full `%Rheo.Lease{}`, including `lease_id` |
  | `metadata.stream` | stream name |
  | `metadata.group` | consumer group name |
  | `metadata.partition` | event partition |
  | `metadata.attempt` | delivery attempt, starting at `1` |

  `metadata.attempt` is how you tell a first delivery from a redelivery — Rheo is
  at-least-once, so handlers must stay idempotent.

  ## Failure mapping

  | Broadway outcome | Rheo settle |
  |---|---|
  | successful | `Rheo.ack/2` |
  | failed | `Rheo.nack/3` (default) |
  | failed, `on_failure: :reject` | `Rheo.reject/3` — dead-letters for this group |

  Set the default on the producer (`on_failure: :reject`), or per message with
  `Broadway.Message.configure_ack(message, on_failure: :reject)`.

  ## Options

  `transform/2` receives the transformer's argument list and reads `:rheo` and
  `:on_failure` from it. Both default to the values configured on the producer
  that emitted the lease, so `{Rheo.Broadway, :transform, []}` is normally
  enough. Pass them explicitly to override:

      transformer: {Rheo.Broadway, :transform, [rheo: MyRheo, on_failure: :reject]}

  ## Compared with `Rheo.Consumer`

  `Rheo.Consumer` is the OTP handler API: `Rheo.Group` fetches, runs handlers,
  renews, and settles. Broadway is an alternative consumption surface for the
  same durable group — pick one per `{rheo, stream, group}`. Batching, rate
  limiting, and fan-out into other Broadway stages are reasons to pick this one.
  See ADR 018.
  """

  alias Rheo.Broadway.Acknowledger
  alias Rheo.Lease

  @doc """
  Transforms a `%Rheo.Lease{}` emitted by `Rheo.Producer` into a Broadway message.

  Used as Broadway's `:transformer`. Broadway invokes it inside the producer
  process, so the producer's pid becomes the acknowledger's `ack_ref` and
  settled leases are reported back with `Rheo.Producer.confirm/2`.

  ## Arguments

    * `lease` — a `%Rheo.Lease{}`
    * `opts` — the transformer argument list; supports `:rheo` and `:on_failure`,
      both defaulting to the producer's configuration

  ## Examples

      iex> event = %Rheo.Event{
      ...>   id: "evt_1",
      ...>   stream: "market-events",
      ...>   partition: 2,
      ...>   sequence: 7,
      ...>   timestamp: ~U[2026-01-15 12:00:00.000Z],
      ...>   type: "curve_update",
      ...>   payload: %{"currency" => "EUR"}
      ...> }
      iex> lease = %Rheo.Lease{
      ...>   lease_id: "lease_1",
      ...>   stream: "market-events",
      ...>   group: "risk",
      ...>   event_id: event.id,
      ...>   event: event,
      ...>   consumer_id: "risk-1",
      ...>   attempt: 1,
      ...>   leased_at: ~U[2026-01-15 12:00:00.000Z],
      ...>   expires_at: ~U[2026-01-15 12:00:30.000Z]
      ...> }
      iex> message = Rheo.Broadway.transform(lease, rheo: MyRheo)
      iex> {message.data.type, message.metadata.partition, message.metadata.attempt}
      {"curve_update", 2, 1}
      iex> {mod, _ack_ref, ack_data} = message.acknowledger
      iex> {mod, ack_data.rheo, ack_data.on_failure}
      {Rheo.Broadway.Acknowledger, MyRheo, :nack}

  ## Returns

  A `%Broadway.Message{}`.
  """
  @spec transform(Lease.t(), keyword()) :: Broadway.Message.t()
  def transform(%Lease{} = lease, opts \\ []) when is_list(opts) do
    config = Rheo.Producer.config() || %{}

    ack_data = %{
      lease: lease,
      rheo: Keyword.get(opts, :rheo) || Map.get(config, :rheo, Rheo),
      on_failure: Keyword.get(opts, :on_failure) || Map.get(config, :on_failure, :nack)
    }

    %Broadway.Message{
      data: lease.event,
      metadata: %{
        lease: lease,
        stream: lease.stream,
        group: lease.group,
        partition: lease.event.partition,
        attempt: lease.attempt
      },
      acknowledger: {Acknowledger, self(), ack_data}
    }
  end
end
