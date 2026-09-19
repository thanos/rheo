defmodule Rheo.DeadLetter do
  @moduledoc """
  Portable dead-letter row for ops inspection (v0.10+).

  Backends may store rejects as delivery-row status flips (ETS/Mongo/Ecto) or as
  a separate DLQ stream (Redis). This struct is the normalized read shape.
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
