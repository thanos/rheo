defmodule Rheo.Event do
  @moduledoc """
  An immutable event stored in a Rheo stream.

  Events are the durable system of record. Consumer progress (leases, ACKs,
  retries, dead-letters) lives in separate per-group delivery state — never as a
  `processed` flag on the event.

  Persistence decoding belongs in backend codecs (e.g.
  `Rheo.Backend.Mongo.Codec`), not on this struct.

  ## Fields

  | Field | Type | Meaning |
  |---|---|---|
  | `id` | `String.t()` | Stable unique id (use for idempotency) |
  | `stream` | `String.t()` | Stream name this event belongs to |
  | `partition` | `non_neg_integer()` | Partition within the stream |
  | `sequence` | `pos_integer()` | Monotonic sequence within `(stream, partition)` |
  | `timestamp` | `DateTime.t()` | Event time (defaults to append time) |
  | `key` | `String.t() \\| nil` | Optional routing key (`:erlang.phash2/2`) |
  | `type` | `String.t() \\| nil` | Optional event type for queries |
  | `metadata` | `map()` | Cross-cutting headers (correlation id, producer, …) |
  | `payload` | `map()` | Domain body (currency, curve, price, …) |

  ## Example

      iex> event = %Rheo.Event{
      ...>   id: "evt_01",
      ...>   stream: "market-events",
      ...>   partition: 0,
      ...>   sequence: 1,
      ...>   timestamp: ~U[2026-01-15 12:00:00.000Z],
      ...>   key: "EUR-EURIBOR-6M",
      ...>   type: "curve_update",
      ...>   metadata: %{"correlation_id" => "abc"},
      ...>   payload: %{"currency" => "EUR", "price" => 2.913}
      ...> }
      iex> {event.type, event.payload["currency"]}
      {"curve_update", "EUR"}
  """

  @enforce_keys [:id, :stream, :partition, :sequence, :timestamp, :payload]
  defstruct [
    :id,
    :stream,
    :partition,
    :sequence,
    :timestamp,
    :key,
    :type,
    metadata: %{},
    payload: %{}
  ]

  @typedoc """
  An immutable stream event.

  See the module documentation for field meanings and examples.
  """
  @type t :: %__MODULE__{
          id: String.t(),
          stream: String.t(),
          partition: non_neg_integer(),
          sequence: pos_integer(),
          timestamp: DateTime.t(),
          key: String.t() | nil,
          type: String.t() | nil,
          metadata: map(),
          payload: map()
        }
end
