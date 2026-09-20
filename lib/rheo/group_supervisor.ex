defmodule Rheo.GroupSupervisor do
  @moduledoc """
  Dynamic supervisor for local `Rheo.Group` processes under one Rheo instance.
  """
  use DynamicSupervisor

  def start_link(opts) do
    rheo = Keyword.fetch!(opts, :rheo)
    DynamicSupervisor.start_link(__MODULE__, opts, name: Rheo.Names.group_supervisor(rheo))
  end

  @impl true
  def init(_opts), do: DynamicSupervisor.init(strategy: :one_for_one)

  @doc """
  Starts a `Rheo.Group` under this instance's dynamic supervisor.

  This is the dynamic-start path for a local consumer group. Host applications
  normally supervise a `Rheo.Consumer` child spec instead.
  """
  @spec start_group(atom(), keyword()) :: DynamicSupervisor.on_start_child()
  def start_group(rheo, group_opts) do
    spec = {Rheo.Group, Keyword.put(group_opts, :rheo, rheo)}
    DynamicSupervisor.start_child(Rheo.Names.group_supervisor(rheo), spec)
  end
end
