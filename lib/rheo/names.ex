defmodule Rheo.Names do
  @moduledoc false

  @spec instance(atom()) :: atom()
  def instance(rheo), do: Module.concat(rheo, Instance)

  @spec group_supervisor(atom()) :: atom()
  def group_supervisor(rheo), do: Module.concat(rheo, GroupSupervisor)

  @spec task_supervisor(atom()) :: atom()
  def task_supervisor(rheo), do: Module.concat(rheo, TaskSupervisor)

  # A local name per {instance, stream, group}. Groups are static
  # configuration, so the atom table stays bounded; a plain name also keeps a
  # Group independent of any instance process (a Registry links registrants).
  @spec group(atom(), String.t(), String.t()) :: atom()
  def group(rheo, stream, group) when is_atom(rheo) and is_binary(stream) and is_binary(group) do
    String.to_atom("#{rheo}.Group.#{stream}.#{group}")
  end
end
