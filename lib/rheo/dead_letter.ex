defmodule Rheo.DeadLetter do
  @moduledoc """
  Portable **dead-letter** (DLQ) row for ops inspection (v0.10+).

  **DLQ** = dead-letter queue: a delivery that stopped being retried for a
  consumer group (after `Rheo.reject/3` or exhausted nack attempts). The
  underlying stream event is unchanged; only that group's delivery is
  dead-lettered.

  Backends may store rejects as delivery-row status flips (ETS/Mongo/Ecto) or as
  a separate DLQ stream (Redis). This struct is the normalized read shape for
  `Rheo.dead_letters/3`.
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
