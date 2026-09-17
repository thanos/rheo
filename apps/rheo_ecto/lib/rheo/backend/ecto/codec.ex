defmodule Rheo.Backend.Ecto.Codec do
  @moduledoc """
  SQL row encode/decode for `Rheo.Backend.Ecto`.

  PostgreSQL stores JSON in `jsonb` and timestamps in `timestamptz`, so maps and
  `DateTime` structs round-trip natively through Postgrex. SQLite has neither
  type, so JSON is encoded with `Jason` and timestamps become fixed-width
  ISO 8601 strings that sort lexicographically.

  Decoding accepts either representation, which keeps the backend working
  against schemas created by an older Rheo version or by hand.
  """

  alias Rheo.Event

  @typedoc "SQL dialect a value is being encoded for."
  @type dialect :: :postgres | :sqlite

  @doc """
  Encodes a map for a JSON column.

  ## Arguments

    * `dialect` — `:postgres` (native `jsonb`) or `:sqlite` (JSON text)
    * `map` — the map to store

  ## Examples

      iex> Rheo.Backend.Ecto.Codec.encode_json(:sqlite, %{"currency" => "EUR"})
      ~s({"currency":"EUR"})

      iex> Rheo.Backend.Ecto.Codec.encode_json(:postgres, %{"currency" => "EUR"})
      %{"currency" => "EUR"}

  ## Returns

  A map (PostgreSQL) or a JSON string (SQLite).
  """
  @spec encode_json(dialect(), map()) :: map() | String.t()
  def encode_json(:postgres, map) when is_map(map), do: map
  def encode_json(:sqlite, map) when is_map(map), do: Jason.encode!(map)

  @doc """
  Decodes a JSON column value into a map.

  ## Arguments

    * `value` — a map, a JSON string, or `nil`

  ## Examples

      iex> Rheo.Backend.Ecto.Codec.decode_json(~s({"a":1}))
      %{"a" => 1}

      iex> Rheo.Backend.Ecto.Codec.decode_json(nil)
      %{}

  ## Returns

  A map. Unparseable strings decode to `%{}`.
  """
  @spec decode_json(term()) :: map()
  def decode_json(nil), do: %{}
  def decode_json(value) when is_map(value), do: value

  def decode_json(value) when is_binary(value) do
    case Jason.decode(value) do
      {:ok, map} when is_map(map) -> map
      _ -> %{}
    end
  end

  def decode_json(_), do: %{}

  @doc """
  Encodes a `DateTime` for a timestamp column.

  SQLite values are padded to microsecond precision so that string comparison
  matches chronological order.

  ## Arguments

    * `dialect` — `:postgres` or `:sqlite`
    * `datetime` — a `DateTime` or `nil`

  ## Examples

      iex> Rheo.Backend.Ecto.Codec.encode_datetime(:sqlite, ~U[2026-01-15 12:00:00Z])
      "2026-01-15T12:00:00.000000Z"

      iex> Rheo.Backend.Ecto.Codec.encode_datetime(:sqlite, nil)
      nil

  ## Returns

  A `DateTime` (PostgreSQL), an ISO 8601 string (SQLite), or `nil`.
  """
  @spec encode_datetime(dialect(), DateTime.t() | nil) :: DateTime.t() | String.t() | nil
  def encode_datetime(_dialect, nil), do: nil
  def encode_datetime(:postgres, %DateTime{} = datetime), do: datetime

  def encode_datetime(:sqlite, %DateTime{} = datetime) do
    truncated = DateTime.truncate(datetime, :microsecond)
    DateTime.to_iso8601(%{truncated | microsecond: {elem(truncated.microsecond, 0), 6}})
  end

  @doc """
  Decodes a timestamp column value into a `DateTime`.

  ## Arguments

    * `value` — a `DateTime`, `NaiveDateTime`, ISO 8601 string, or `nil`

  ## Examples

      iex> Rheo.Backend.Ecto.Codec.decode_datetime("2026-01-15T12:00:00.000000Z")
      ~U[2026-01-15 12:00:00.000000Z]

      iex> Rheo.Backend.Ecto.Codec.decode_datetime(nil)
      nil

  ## Returns

  A `DateTime` or `nil`.
  """
  @spec decode_datetime(term()) :: DateTime.t() | nil
  def decode_datetime(nil), do: nil
  def decode_datetime(%DateTime{} = datetime), do: datetime

  def decode_datetime(%NaiveDateTime{} = naive), do: DateTime.from_naive!(naive, "Etc/UTC")

  def decode_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> datetime
      _ -> nil
    end
  end

  @doc """
  Builds an `%Rheo.Event{}` from a `rheo_events` row.

  ## Arguments

    * `row` — a map of column name to value (see `rows/2`)

  ## Examples

      iex> Rheo.Backend.Ecto.Codec.event_from_row(%{
      ...>   "id" => "evt_01",
      ...>   "stream" => "market-events",
      ...>   "partition" => 0,
      ...>   "sequence" => 12,
      ...>   "timestamp" => "2026-01-15T12:00:00.000000Z",
      ...>   "type" => "curve_update",
      ...>   "metadata" => nil,
      ...>   "payload" => ~s({"currency":"EUR"})
      ...> })
      %Rheo.Event{
        id: "evt_01",
        stream: "market-events",
        partition: 0,
        sequence: 12,
        timestamp: ~U[2026-01-15 12:00:00.000000Z],
        key: nil,
        type: "curve_update",
        metadata: %{},
        payload: %{"currency" => "EUR"}
      }

  ## Returns

  `%Rheo.Event{}`
  """
  @spec event_from_row(map()) :: Event.t()
  def event_from_row(row) when is_map(row) do
    %Event{
      id: row["id"],
      stream: row["stream"],
      partition: row["partition"] || 0,
      sequence: row["sequence"],
      timestamp: decode_datetime(row["timestamp"]),
      key: row["key"],
      type: row["type"],
      metadata: decode_json(row["metadata"]),
      payload: decode_json(row["payload"])
    }
  end

  @doc """
  Zips an `Ecto.Adapters.SQL` result into a list of column-keyed maps.

  ## Arguments

    * `columns` — column names from the query result
    * `rows` — row value lists from the query result

  ## Examples

      iex> Rheo.Backend.Ecto.Codec.rows(["a", "b"], [[1, 2], [3, 4]])
      [%{"a" => 1, "b" => 2}, %{"a" => 3, "b" => 4}]

  ## Returns

  A list of maps, one per row.
  """
  @spec rows([String.t()] | nil, [[term()]] | nil) :: [map()]
  def rows(nil, _rows), do: []
  def rows(_columns, nil), do: []

  def rows(columns, rows) when is_list(columns) and is_list(rows) do
    Enum.map(rows, fn values -> columns |> Enum.zip(values) |> Map.new() end)
  end
end
