defmodule Rheo.Backend.Table.Store do
  @moduledoc false

  @type table :: :ets.tid() | atom()
  @type key :: term()
  @type value :: term()
  @type match_spec :: :ets.match_spec()

  @typedoc "Next key in term order, or `:\"$end_of_table\"` past the last one."
  @type next_key :: key() | :"$end_of_table"

  @callback lookup(table(), key()) :: [{key(), value()}]
  @callback insert(table(), key(), value()) :: :ok
  @callback delete(table(), key()) :: :ok
  @callback select(table(), match_spec()) :: [term()]

  @doc """
  Returns the smallest key greater than `key` on an `ordered_set` table.

  `key` need not exist. Used to walk a key range in order without materializing
  the whole table, which keeps range reads O(range) instead of O(table).
  """
  @callback next_key(table(), key()) :: next_key()
end

defmodule Rheo.Backend.Table.ETS do
  @moduledoc false
  @behaviour Rheo.Backend.Table.Store

  @impl true
  def lookup(table, key), do: :ets.lookup(table, key)

  @impl true
  def insert(table, key, value) do
    true = :ets.insert(table, {key, value})
    :ok
  end

  @impl true
  def delete(table, key) do
    :ets.delete(table, key)
    :ok
  end

  @impl true
  def select(table, spec), do: :ets.select(table, spec)

  @impl true
  def next_key(table, key), do: :ets.next(table, key)
end

defmodule Rheo.Backend.Table.Mnesia do
  @moduledoc false
  @behaviour Rheo.Backend.Table.Store

  @impl true
  def lookup(table, key) do
    case Rheo.Backend.Mnesia.Store.current().dirty_read(table, key) do
      [{^table, ^key, value}] -> [{key, value}]
      [] -> []
    end
  end

  @impl true
  def insert(table, key, value) do
    :ok = Rheo.Backend.Mnesia.Store.current().dirty_write({table, key, value})
  end

  @impl true
  def delete(table, key) do
    :ok = Rheo.Backend.Mnesia.Store.current().dirty_delete(table, key)
  end

  @impl true
  def select(table, spec) do
    converted =
      Enum.map(spec, fn {head, guards, body} ->
        {mnesia_head(table, head), guards, body}
      end)

    Rheo.Backend.Mnesia.Store.current().dirty_select(table, converted)
  end

  @impl true
  def next_key(table, key) do
    Rheo.Backend.Mnesia.Store.current().dirty_next(table, key)
  end

  defp mnesia_head(table, {key, value}), do: {table, key, value}
end
