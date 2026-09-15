defmodule Rheo.Names do
  @moduledoc false

  @spec registry(atom()) :: atom()
  def registry(rheo), do: Module.concat(rheo, Registry)

  @spec instance(atom()) :: atom()
  def instance(rheo), do: Module.concat(rheo, Instance)

  @spec group_supervisor(atom()) :: atom()
  def group_supervisor(rheo), do: Module.concat(rheo, GroupSupervisor)

  @spec task_supervisor(atom()) :: atom()
  def task_supervisor(rheo), do: Module.concat(rheo, TaskSupervisor)

  @spec backend_handle(atom()) :: atom()
  def backend_handle(rheo), do: Module.concat(rheo, Mongo)

  @spec group(atom(), String.t(), String.t()) :: {:via, module(), term()}
  def group(rheo, stream, group) do
    {:via, Registry, {registry(rheo), {:group, stream, group}}}
  end
end
