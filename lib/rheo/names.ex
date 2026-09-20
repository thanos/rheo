defmodule Rheo.Names do
  @moduledoc false

  @spec instance(atom()) :: atom()
  def instance(rheo), do: Module.concat(rheo, Instance)

  @spec group_supervisor(atom()) :: atom()
  def group_supervisor(rheo), do: Module.concat(rheo, GroupSupervisor)

  @spec task_supervisor(atom()) :: atom()
  def task_supervisor(rheo), do: Module.concat(rheo, TaskSupervisor)

  # Via-tuple registration for `{instance, stream, group}`. The Registry is
  # started by `Rheo.Application` (the `:rheo` app must be running) so Groups
  # started by a host supervisor are not linked to an instance process.
  @spec group(atom(), String.t(), String.t()) :: {:via, module(), {atom(), term()}}
  def group(rheo, stream, group) when is_atom(rheo) and is_binary(stream) and is_binary(group) do
    {:via, Registry, {Rheo.Registry, {rheo, stream, group}}}
  end
end
