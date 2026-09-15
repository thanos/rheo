defmodule Rheo.Case do
  @moduledoc false
  use ExUnit.CaseTemplate

  using do
    quote do
      import Rheo.Case
    end
  end

  setup tags do
    if tags[:mongo] do
      :ok = ensure_rheo_started()
      :ok = drop_rheo_collections()
      :ok = Rheo.ensure_indexes()
      Rheo.Clock.Frozen.reset()
      :ok
    else
      :ok
    end
  end

  def mongo_url do
    System.get_env("RHEO_MONGO_URL", "mongodb://localhost:27017/rheo_test")
  end

  def ensure_rheo_started do
    case Process.whereis(Rheo.Mongo) do
      pid when is_pid(pid) ->
        :ok

      nil ->
        {:ok, _} = start_supervised({Rheo, url: mongo_url(), name: Rheo.Mongo})
        :ok
    end
  end

  def drop_rheo_collections do
    topo = Rheo.Backend.Mongo.topology_name()

    Enum.each(["streams", "events", "groups", "deliveries"], fn coll ->
      _ = Mongo.delete_many(topo, coll, %{})
    end)

    :ok
  end

  def unique_stream(prefix \\ "stream") do
    "#{prefix}-#{System.unique_integer([:positive])}"
  end
end
