defmodule Rheo.GroupInfo do
  @moduledoc """
  Group health snapshot for ops inspection (v0.10+).

  Combines contiguous-frontier `lag` with inflight and dead-letter counts so
  hosts can answer the NATS-style `consumer info` questions without a second
  settle path.
  """

  @enforce_keys [:stream, :group, :lag]
  defstruct stream: nil,
            group: nil,
            lag: nil,
            inflight_count: 0,
            dead_letter_count: 0

  @type t :: %__MODULE__{
          stream: String.t(),
          group: String.t(),
          lag: Rheo.Lag.t(),
          inflight_count: non_neg_integer(),
          dead_letter_count: non_neg_integer()
        }
end
