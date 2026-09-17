# Optional integration: compiled only when `:mongodb_driver` is present.
if Code.ensure_loaded?(Mongo) do
  defmodule Rheo.Backend.Mongo.Codec do
    @moduledoc """
    Mongo document encode/decode for Rheo domain structs.

    Keeps persistence coupling out of `Rheo.Event`.
    """

    alias Rheo.Event

    @doc """
    Builds an `%Rheo.Event{}` from a Mongo (or map) document.

    Accepts string or atom keys. Missing `partition` defaults to `0`; missing
    `metadata` / `payload` default to `%{}`.

    ## Examples

        iex> Rheo.Backend.Mongo.Codec.event_from_doc(%{
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

    ## Returns

    `%Rheo.Event{}`
    """
    @spec event_from_doc(map()) :: Event.t()
    def event_from_doc(doc) when is_map(doc) do
      %Event{
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
end
