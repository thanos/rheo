defmodule Rheo.Backend.Table.Store do
  @moduledoc false

  @type table :: :ets.tid() | atom()
  @type key :: term()
  @type value :: term()
  @type match_spec :: :ets.match_spec()

  @callback lookup(table(), key()) :: [{key(), value()}]
  @callback insert(table(), key(), value()) :: :ok
  @callback delete(table(), key()) :: :ok
  @callback select(table(), match_spec()) :: [term()]
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

  defp mnesia_head(table, {key, value}), do: {table, key, value}
end
