defmodule Rheo.Backend.Mongo.Client.Driver do
  @moduledoc false
  @behaviour Rheo.Backend.Mongo.Client

  @impl true
  def command(handle, cmd), do: Mongo.command(handle, cmd)

  @impl true
  def insert_one(handle, coll, doc), do: Mongo.insert_one(handle, coll, doc)

  @impl true
  def insert_many(handle, coll, docs, opts), do: Mongo.insert_many(handle, coll, docs, opts)

  @impl true
  def find(handle, coll, filter, opts), do: Mongo.find(handle, coll, filter, opts)

  @impl true
  def find_one(handle, coll, filter), do: Mongo.find_one(handle, coll, filter)

  @impl true
  def find_one_and_update(handle, coll, filter, update, opts),
    do: Mongo.find_one_and_update(handle, coll, filter, update, opts)

  @impl true
  def create_indexes(handle, coll, indexes), do: Mongo.create_indexes(handle, coll, indexes)
end
