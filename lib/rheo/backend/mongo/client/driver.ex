# Optional integration: compiled only when `:mongodb_driver` is present.
if Code.ensure_loaded?(Mongo) do
  defmodule Rheo.Backend.Mongo.Client.Driver do
    @moduledoc false
    @behaviour Rheo.Backend.Mongo.Client

    @impl true
    def command(handle, cmd), do: wrap(fn -> Mongo.command(handle, cmd) end)

    @impl true
    def insert_one(handle, coll, doc), do: wrap(fn -> Mongo.insert_one(handle, coll, doc) end)

    @impl true
    def insert_many(handle, coll, docs, opts),
      do: wrap(fn -> Mongo.insert_many(handle, coll, docs, opts) end)

    @impl true
    def find(handle, coll, filter, opts) do
      wrap(fn -> Mongo.find(handle, coll, filter, opts) end)
    end

    @impl true
    def find_one(handle, coll, filter), do: wrap(fn -> Mongo.find_one(handle, coll, filter) end)

    @impl true
    def find_one_and_update(handle, coll, filter, update, opts),
      do: wrap(fn -> Mongo.find_one_and_update(handle, coll, filter, update, opts) end)

    @impl true
    def update_many(handle, coll, filter, update, opts),
      do: wrap(fn -> Mongo.update_many(handle, coll, filter, update, opts) end)

    @impl true
    def delete_many(handle, coll, filter),
      do: wrap(fn -> Mongo.delete_many(handle, coll, filter) end)

    @impl true
    def create_indexes(handle, coll, indexes),
      do: wrap(fn -> Mongo.create_indexes(handle, coll, indexes) end)

    defp wrap(fun) do
      fun.()
    catch
      :exit, {:timeout, _} -> {:error, {:ambiguous, :timeout}}
      :exit, _reason -> {:error, :backend_unavailable}
    end
  end
end
