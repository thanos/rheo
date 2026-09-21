defmodule Rheo.GroupInfo do
  @moduledoc """
  Group health snapshot for ops inspection.

  Combines contiguous-frontier `lag` with inflight and dead-letter counts so
  hosts can answer consumer-info questions without a second settle path.
  Returned by `Rheo.group_info/3`.

  ## Fields

  | Field | Type | Meaning |
  |---|---|---|
  | `stream` | `String.t()` | Stream name |
  | `group` | `String.t()` | Consumer group name |
  | `lag` | `Rheo.Lag.t()` | Per-partition and aggregate lag |
  | `inflight_count` | `non_neg_integer()` | Active leases for this group |
  | `dead_letter_count` | `non_neg_integer()` | Dead-lettered deliveries for this group |

  ## Example

      iex> info = %Rheo.GroupInfo{
      ...>   stream: "market-events",
      ...>   group: "risk",
      ...>   lag: Rheo.Lag.build("market-events", "risk", %{
      ...>     0 => %{frontier: 1, high_watermark: 4, lag: 3}
      ...>   }),
      ...>   inflight_count: 2,
      ...>   dead_letter_count: 1
      ...> }
      iex> {info.lag.lag, info.inflight_count, info.dead_letter_count}
      {3, 2, 1}
  """

  @enforce_keys [:stream, :group, :lag]
  defstruct stream: nil,
            group: nil,
            lag: nil,
            inflight_count: 0,
            dead_letter_count: 0

  @typedoc """
  Ops health snapshot for one `{stream, group}`.

  See the module documentation for field meanings.
  """
  @type t :: %__MODULE__{
          stream: String.t(),
          group: String.t(),
          lag: Rheo.Lag.t(),
          inflight_count: non_neg_integer(),
          dead_letter_count: non_neg_integer()
        }
end
