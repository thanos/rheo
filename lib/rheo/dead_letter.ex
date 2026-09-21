defmodule Rheo.DeadLetter do
  @moduledoc """
  Portable dead-letter (DLQ) row for ops inspection.

  **DLQ** = dead-letter queue: a delivery that stopped being retried for a
  consumer group (after `Rheo.reject/3` or exhausted nack attempts). The
  underlying stream event is unchanged; only that group's delivery is
  dead-lettered.

  Backends may store rejects as delivery-row status flips (ETS/Mongo/Ecto) or as
  a separate DLQ stream (Redis). This struct is the normalized read shape for
  `Rheo.dead_letters/3`.

  ## Fields

  | Field | Type | Meaning |
  |---|---|---|
  | `stream` | `String.t()` | Stream name |
  | `group` | `String.t()` | Consumer group that dead-lettered the delivery |
  | `event_id` | `String.t()` | Stable `Rheo.Event.id` |
  | `partition` | `non_neg_integer()` | Partition of the event |
  | `sequence` | `non_neg_integer() \\| nil` | Sequence when known |
  | `reason` | `term()` | Reject/retry reason recorded by the backend |
  | `dead_lettered_at` | `DateTime.t() \\| nil` | When the delivery became dead-lettered |
  | `event` | `Rheo.Event.t() \\| nil` | Embedded event when the backend includes it |

  ## Example

      iex> dl = %Rheo.DeadLetter{
      ...>   stream: "market-events",
      ...>   group: "risk",
      ...>   event_id: "evt_01",
      ...>   partition: 0,
      ...>   sequence: 3,
      ...>   reason: :invalid_schema,
      ...>   dead_lettered_at: ~U[2026-01-15 12:00:00.000Z],
      ...>   event: nil
      ...> }
      iex> {dl.group, dl.reason}
      {"risk", :invalid_schema}
  """

  @enforce_keys [:stream, :group, :event_id]
  defstruct stream: nil,
            group: nil,
            event_id: nil,
            partition: 0,
            sequence: nil,
            reason: nil,
            dead_lettered_at: nil,
            event: nil

  @typedoc """
  One dead-lettered delivery for ops listing.

  See the module documentation for field meanings.
  """
  @type t :: %__MODULE__{
          stream: String.t(),
          group: String.t(),
          event_id: String.t(),
          partition: non_neg_integer(),
          sequence: non_neg_integer() | nil,
          reason: term(),
          dead_lettered_at: DateTime.t() | nil,
          event: Rheo.Event.t() | nil
        }
end
