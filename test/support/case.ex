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
      :ok = stop_all_groups(Rheo)
      :ok = drop_rheo_collections()
      :ok = Rheo.ensure_indexes()
      Rheo.Clock.Frozen.reset()

      on_exit(fn -> stop_all_groups(Rheo) end)
      :ok
    else
      :ok
    end
  end

  def mongo_url do
    System.get_env("RHEO_MONGO_URL", "mongodb://localhost:27017/rheo_test")
  end

  def ensure_rheo_started do
    case Process.whereis(Rheo) do
      pid when is_pid(pid) ->
        :ok

      nil ->
        {:ok, _} = start_supervised({Rheo, url: mongo_url()})
        :ok
    end
  end

  def stop_all_groups(rheo) do
    case Process.whereis(Rheo.Names.group_supervisor(rheo)) do
      pid when is_pid(pid) ->
        pid
        |> DynamicSupervisor.which_children()
        |> Enum.each(fn
          {_, child, _, _} when is_pid(child) ->
            _ = DynamicSupervisor.terminate_child(pid, child)

          _ ->
            :ok
        end)

        :ok

      _ ->
        :ok
    end
  end

  def drop_rheo_collections do
    handle = Rheo.Backend.Mongo.default_handle()

    Enum.each(["streams", "events", "groups", "deliveries"], fn coll ->
      _ = Mongo.delete_many(handle, coll, %{})
    end)

    :ok
  end

  def unique_stream(prefix \\ "stream") do
    "#{prefix}-#{System.unique_integer([:positive])}"
  end
end
