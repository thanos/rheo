defmodule Rheo.Lease do
  @moduledoc """
  A temporary claim on an event for one consumer group.

  Fetching work creates a lease with a unique `lease_id` (fencing token). Only
  the holder of the **current** lease may `Rheo.ack/1`, `Rheo.nack/2`, or
  `Rheo.reject/2`. After expiry, another consumer may obtain a new lease
  (at-least-once redelivery).

  ## Fields

  | Field | Type | Meaning |
  |---|---|---|
  | `lease_id` | `String.t()` | Opaque fencing token for this claim |
  | `stream` | `String.t()` | Stream name |
  | `group` | `String.t()` | Consumer group name |
  | `event_id` | `String.t()` | Id of the leased event |
  | `event` | `Rheo.Event.t()` | Full immutable event payload |
  | `consumer_id` | `String.t()` | Worker that holds the lease |
  | `attempt` | `pos_integer()` | Delivery attempt count (starts at 1) |
  | `leased_at` | `DateTime.t()` | When the lease was granted |
  | `expires_at` | `DateTime.t()` | When the lease becomes reclaimable |
  | `receipt` | `term() \\| nil` | Opaque backend-native settle identity (ADR 021) |

  ## Example

      iex> event = %Rheo.Event{
      ...>   id: "evt_01",
      ...>   stream: "market-events",
      ...>   partition: 0,
      ...>   sequence: 1,
      ...>   timestamp: ~U[2026-01-15 12:00:00.000Z],
      ...>   type: "curve_update",
      ...>   payload: %{"currency" => "EUR"}
      ...> }
      iex> lease = %Rheo.Lease{
      ...>   lease_id: "lease_abc",
      ...>   stream: "market-events",
      ...>   group: "risk",
      ...>   event_id: event.id,
      ...>   event: event,
      ...>   consumer_id: "risk-worker-1",
      ...>   attempt: 1,
      ...>   leased_at: ~U[2026-01-15 12:00:00.000Z],
      ...>   expires_at: ~U[2026-01-15 12:00:30.000Z],
      ...>   receipt: "lease_abc"
      ...> }
      iex> {lease.group, lease.event.type, lease.attempt, lease.receipt}
      {"risk", "curve_update", 1, "lease_abc"}
  """

  @enforce_keys [
    :lease_id,
    :stream,
    :group,
    :event_id,
    :event,
    :consumer_id,
    :attempt,
    :leased_at,
    :expires_at
  ]
  defstruct [
    :lease_id,
    :stream,
    :group,
    :event_id,
    :event,
    :consumer_id,
    :attempt,
    :leased_at,
    :expires_at,
    receipt: nil
  ]

  @typedoc """
  A fenced lease on a single event for a consumer group.

  Pass this struct to `Rheo.ack/1`, `Rheo.nack/2`, or `Rheo.reject/2`.

  `receipt` is an opaque backend-native settle token (ADR 021). Database
  backends typically mirror `lease_id`; native-stream backends may store a
  Redis ID / PEL claim identity. Do not pattern-match on receipt contents in
  application code.
  """
  @type t :: %__MODULE__{
          lease_id: String.t(),
          stream: String.t(),
          group: String.t(),
          event_id: String.t(),
          event: Rheo.Event.t(),
          consumer_id: String.t(),
          attempt: pos_integer(),
          leased_at: DateTime.t(),
          expires_at: DateTime.t(),
          receipt: term() | nil
        }
end
