# Optional integration: compiled only when `:redix` is present.
if Code.ensure_loaded?(Redix) do
  defmodule Rheo.Backend.Redis.Codec do
    @moduledoc """
    Encode / decode Rheo events as Redis STREAM entry fields.

    An entry carries the portable event identity (`id`, `sequence`, `partition`,
    `timestamp`, `key`, `type`) as flat fields and the two open maps
    (`metadata`, `payload`) as JSON, so `Rheo.Query` filters can be applied in
    the adapter (`secondary_indexes: false`, ADR 026).

    Nil `key` / `type` fields are omitted rather than stored as empty strings.
    """

    alias Rheo.Event

    @doc """
    Builds the flat `XADD` field list for an event.

    ## Examples

        iex> event = %Rheo.Event{
        ...>   id: "evt_01",
        ...>   stream: "market",
        ...>   partition: 0,
        ...>   sequence: 7,
        ...>   timestamp: ~U[2026-01-15 12:00:00.000Z],
        ...>   type: "curve_update",
        ...>   payload: %{"currency" => "EUR"}
        ...> }
        iex> Rheo.Backend.Redis.Codec.to_fields(event)
        ["id", "evt_01", "sequence", "7", "partition", "0", "timestamp",
         "2026-01-15T12:00:00.000Z", "type", "curve_update", "metadata", "{}",
         "payload", "{\\"currency\\":\\"EUR\\"}"]

    ## Returns

    A list of binaries, field and value alternating.
    """
    @spec to_fields(Event.t()) :: [String.t()]
    def to_fields(%Event{} = event) do
      [
        "id",
        event.id,
        "sequence",
        Integer.to_string(event.sequence),
        "partition",
        Integer.to_string(event.partition)
      ] ++
        ["timestamp", encode_timestamp(event.timestamp)] ++
        optional("key", event.key) ++
        optional("type", event.type) ++
        ["metadata", encode_map(event.metadata), "payload", encode_map(event.payload)]
    end

    @doc """
    Rebuilds an `%Rheo.Event{}` from a stream entry's field list.

    ## Arguments

      * `stream` — Rheo stream name (not stored per entry)
      * `fields` — flat `[field, value, …]` list as returned by Redis

    ## Examples

        iex> fields = ["id", "evt_01", "sequence", "7", "partition", "0",
        ...>           "timestamp", "2026-01-15T12:00:00.000Z", "type", "curve_update",
        ...>           "metadata", "{}", "payload", "{\\"currency\\":\\"EUR\\"}"]
        iex> event = Rheo.Backend.Redis.Codec.event_from_fields("market", fields)
        iex> {event.id, event.sequence, event.type, event.payload}
        {"evt_01", 7, "curve_update", %{"currency" => "EUR"}}

    ## Returns

    `%Rheo.Event{}`.
    """
    @spec event_from_fields(String.t(), [String.t()]) :: Event.t()
    def event_from_fields(stream, fields) when is_list(fields) do
      map = to_map(fields)

      %Event{
        id: map["id"],
        stream: stream,
        partition: to_integer(map["partition"], 0),
        sequence: to_integer(map["sequence"], 0),
        timestamp: decode_timestamp(map["timestamp"]),
        key: map["key"],
        type: map["type"],
        metadata: decode_map(map["metadata"]),
        payload: decode_map(map["payload"])
      }
    end

    @doc """
    Converts a flat Redis field list into a map.

    ## Examples

        iex> Rheo.Backend.Redis.Codec.to_map(["a", "1", "b", "2"])
        %{"a" => "1", "b" => "2"}

        iex> Rheo.Backend.Redis.Codec.to_map(nil)
        %{}
    """
    @spec to_map([String.t()] | nil) :: %{optional(String.t()) => String.t()}
    def to_map(nil), do: %{}

    def to_map(fields) when is_list(fields) do
      fields
      |> Enum.chunk_every(2)
      |> Map.new(fn
        [field, value] -> {field, value}
        [field] -> {field, nil}
      end)
    end

    @doc """
    Parses an integer stored as a Redis string.

    ## Examples

        iex> Rheo.Backend.Redis.Codec.to_integer("12", 0)
        12

        iex> Rheo.Backend.Redis.Codec.to_integer(nil, 3)
        3
    """
    @spec to_integer(term(), integer()) :: integer()
    def to_integer(value, default) when is_binary(value) do
      case Integer.parse(value) do
        {int, _rest} -> int
        :error -> default
      end
    end

    def to_integer(value, _default) when is_integer(value), do: value
    def to_integer(_value, default), do: default

    @doc """
    Encodes an arbitrary settle reason as a short diagnostic string.

    Reasons are diagnostics, never payloads: handler terms are inspected and
    truncated instead of stored verbatim.

    ## Examples

        iex> Rheo.Backend.Redis.Codec.encode_reason(:boom)
        ":boom"

        iex> Rheo.Backend.Redis.Codec.encode_reason("too big")
        "too big"
    """
    @spec encode_reason(term()) :: String.t()
    def encode_reason(reason) when is_binary(reason), do: String.slice(reason, 0, 1_000)
    def encode_reason(reason), do: reason |> inspect(limit: 50) |> String.slice(0, 1_000)

    defp optional(_field, nil), do: []
    defp optional(field, value), do: [field, to_string(value)]

    defp encode_map(map) when is_map(map), do: Jason.encode!(map)
    defp encode_map(_), do: "{}"

    defp decode_map(nil), do: %{}

    defp decode_map(json) when is_binary(json) do
      case Jason.decode(json) do
        {:ok, map} when is_map(map) -> map
        _ -> %{}
      end
    end

    defp encode_timestamp(%DateTime{} = timestamp), do: DateTime.to_iso8601(timestamp)
    defp encode_timestamp(other), do: to_string(other)

    defp decode_timestamp(nil), do: nil

    defp decode_timestamp(value) when is_binary(value) do
      case DateTime.from_iso8601(value) do
        {:ok, timestamp, _offset} -> timestamp
        _ -> value
      end
    end
  end
end
