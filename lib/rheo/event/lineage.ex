defmodule Rheo.Event.Lineage do
  @moduledoc """
  Standard metadata keys for event lineage and provenance.

  These live in `Rheo.Event.metadata` (string keys after persistence), not as
  top-level Event fields.

  | Key | Meaning |
  |---|---|
  | `correlation_id` | Shared id across a saga / request |
  | `causation_id` | Id of the event or command that caused this one |
  | `producer` | Producing service or component |
  | `schema` | Payload schema name |
  | `schema_version` | Schema version (string or number) |

  ## Example

      iex> meta = Rheo.Event.Lineage.put(%{},
      ...>   correlation_id: "req-1",
      ...>   producer: "pricing",
      ...>   schema: "curve_update",
      ...>   schema_version: 1
      ...> )
      iex> Rheo.Event.Lineage.get(meta, :correlation_id)
      "req-1"
  """

  @keys [:correlation_id, :causation_id, :producer, :schema, :schema_version]

  @doc """
  Returns the list of standardized lineage metadata keys.

  ## Examples

      iex> :correlation_id in Rheo.Event.Lineage.keys()
      true
  """
  @spec keys() :: [atom()]
  def keys, do: @keys

  @doc """
  Builds a metadata map from lineage options.

  Unknown keys are ignored. Values are kept as-is; backends stringify atom keys
  on append.

  ## Arguments

    * `metadata` — existing map to merge into (default `%{}`)
    * `opts` — keyword list; only keys in `keys/0` are copied

  ## Examples

      iex> Rheo.Event.Lineage.put(correlation_id: "abc", ignored: true)
      %{correlation_id: "abc"}

      iex> Rheo.Event.Lineage.put(%{"x" => 1}, producer: "risk")
      %{"x" => 1, producer: "risk"}
  """
  @spec put(map(), keyword()) :: map()
  def put(metadata \\ %{}, opts) when is_map(metadata) and is_list(opts) do
    Enum.reduce(@keys, metadata, fn key, acc ->
      case Keyword.fetch(opts, key) do
        {:ok, value} -> Map.put(acc, key, value)
        :error -> acc
      end
    end)
  end

  @doc """
  Reads a lineage field from an event or metadata map.

  Accepts atom or string keys on the source map.

  ## Examples

      iex> Rheo.Event.Lineage.get(%{"correlation_id" => "c1"}, :correlation_id)
      "c1"

      iex> event = %Rheo.Event{
      ...>   id: "e1",
      ...>   stream: "s",
      ...>   partition: 0,
      ...>   sequence: 1,
      ...>   timestamp: ~U[2026-01-15 12:00:00.000Z],
      ...>   payload: %{},
      ...>   metadata: %{producer: "api"}
      ...> }
      iex> Rheo.Event.Lineage.get(event, :producer)
      "api"

      iex> Rheo.Event.Lineage.get(%{}, :schema)
      nil
  """
  @spec get(Rheo.Event.t() | map(), atom()) :: term() | nil
  def get(%Rheo.Event{metadata: metadata}, key) when is_atom(key), do: get(metadata, key)

  def get(metadata, key) when is_map(metadata) and is_atom(key) do
    Map.get(metadata, key) || Map.get(metadata, Atom.to_string(key))
  end
end
