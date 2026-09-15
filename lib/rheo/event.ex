defmodule Rheo.Event do
  @moduledoc """
  An immutable event stored in a Rheo stream.

  Events are the durable system of record. Consumer progress (leases, ACKs,
  retries, dead-letters) lives in separate per-group delivery state — never as a
  `processed` flag on the event.

  ## Fields

  | Field | Type | Meaning |
  |---|---|---|
  | `id` | `String.t()` | Stable unique id (use for idempotency) |
  | `stream` | `String.t()` | Stream name this event belongs to |
  | `partition` | `non_neg_integer()` | Partition within the stream (MVP uses `0`) |
  | `sequence` | `pos_integer()` | Monotonic sequence within `(stream, partition)` |
  | `timestamp` | `DateTime.t()` | Event time (defaults to append time) |
  | `key` | `String.t() \\| nil` | Optional routing / partition key |
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

  @doc """
  Builds an `%Rheo.Event{}` from a Mongo (or map) document.

  Accepts string or atom keys for common fields. Missing `partition` defaults to
  `0`; missing `metadata` / `payload` default to `%{}`.

  ## Examples

      iex> Rheo.Event.from_doc(%{
      ...>   "_id" => "evt_01",
      ...>   "stream" => "market-events",
      ...>   "partition" => 0,
      ...>   "sequence" => 12,
      ...>   "timestamp" => ~U[2026-01-15 12:00:00.000Z],
      ...>   "type" => "curve_update",
      ...>   "payload" => %{"currency" => "EUR"}
      ...> })
      %Rheo.Event{
        id: "evt_01",
        stream: "market-events",
        partition: 0,
        sequence: 12,
        timestamp: ~U[2026-01-15 12:00:00.000Z],
        key: nil,
        type: "curve_update",
        metadata: %{},
        payload: %{"currency" => "EUR"}
      }

      iex> Rheo.Event.from_doc(%{
      ...>   id: "evt_02",
      ...>   stream: "trades",
      ...>   sequence: 1,
      ...>   timestamp: ~U[2026-01-15 12:00:00.000Z],
      ...>   payload: %{trade: "12345"}
      ...> }).partition
      0

  ## Returns

  Always returns `%Rheo.Event{}`. Does not raise for missing optional fields.
  Raises `FunctionClauseError` if `doc` is not a map.
  """
  @spec from_doc(map()) :: t()
  def from_doc(doc) when is_map(doc) do
    %__MODULE__{
      id: field(doc, ["_id", :_id, "id", :id]),
      stream: field(doc, ["stream", :stream]),
      partition: field(doc, ["partition", :partition], 0),
      sequence: field(doc, ["sequence", :sequence]),
      timestamp: cast_datetime(field(doc, ["timestamp", :timestamp])),
      key: field(doc, ["key", :key]),
      type: field(doc, ["type", :type]),
      metadata: field(doc, ["metadata", :metadata], %{}),
      payload: field(doc, ["payload", :payload], %{})
    }
  end

  defp field(doc, keys, default \\ nil) do
    Enum.reduce_while(keys, default, fn key, acc ->
      if Map.has_key?(doc, key), do: {:halt, Map.get(doc, key)}, else: {:cont, acc}
    end)
  end

  defp cast_datetime(%DateTime{} = dt), do: dt

  defp cast_datetime(other) when is_binary(other) do
    case DateTime.from_iso8601(other) do
      {:ok, dt, _} -> dt
      _ -> other
    end
  end

  defp cast_datetime(other), do: other
end
