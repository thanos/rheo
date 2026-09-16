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
  """

  @keys [:correlation_id, :causation_id, :producer, :schema, :schema_version]

  @doc "Returns the list of standardized lineage metadata keys."
  @spec keys() :: [atom()]
  def keys, do: @keys

  @doc """
  Builds a metadata map from lineage options.

  Unknown keys are ignored. Values are kept as-is; backends stringify atom keys
  on append.
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
  """
  @spec get(Rheo.Event.t() | map(), atom()) :: term() | nil
  def get(%Rheo.Event{metadata: metadata}, key) when is_atom(key), do: get(metadata, key)

  def get(metadata, key) when is_map(metadata) and is_atom(key) do
    Map.get(metadata, key) || Map.get(metadata, Atom.to_string(key))
  end
end
