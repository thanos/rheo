defmodule Rheo.GroupSupervisor do
  @moduledoc """
  Dynamic supervisor for local `Rheo.Group` processes under one Rheo instance.

  Started automatically as a child of `{Rheo, ...}`. Hosts usually supervise a
  `Rheo.Consumer` child spec in their own tree; use `start_group/2` only when a
  group must be started dynamically at runtime.

  ```
  Application Supervisor
        |
        +-- Rheo (named instance)
        |     +-- Backend handle
        |     +-- Rheo.Instance
        |     +-- Task.Supervisor
        |     +-- Rheo.GroupSupervisor   <-- this process
        |              +-- (optional) dynamically started Rheo.Group
        |
        +-- RiskConsumer = Rheo.Group    <-- preferred: host-owned child
  ```
  """
  use DynamicSupervisor

  @doc """
  Starts the instance's group dynamic supervisor.

  ## Options

    * `:rheo` — required instance name (`atom()`), used to register via
      `Rheo.Names.group_supervisor/1`

  ## Returns

  `{:ok, pid}` | `{:error, reason}` as for `DynamicSupervisor.start_link/3`.

  ## Examples

  Hosts do not call this directly — `{Rheo, name: MyRheo, ...}` starts it.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    rheo = Keyword.fetch!(opts, :rheo)
    DynamicSupervisor.start_link(__MODULE__, opts, name: Rheo.Names.group_supervisor(rheo))
  end

  @impl true
  def init(_opts), do: DynamicSupervisor.init(strategy: :one_for_one)

  @doc """
  Starts a `Rheo.Group` under this instance's dynamic supervisor.

  Prefer supervising `use Rheo.Consumer` in the host tree. Use this when a
  group must be created after boot (admin tooling, on-demand workers).

  ## Arguments

    * `rheo` — instance name (`atom()`)
    * `group_opts` — same options as `Rheo.Group` / `Rheo.Consumer`
      (`:stream`, `:group`, `:module`, `:max_demand`, `:concurrency`, …).
      `:rheo` is injected automatically.

  ## Returns

    * `{:ok, pid}` on success
    * `{:error, {:already_started, pid}}` if that `{rheo, stream, group}`
      already runs on this node
    * other `DynamicSupervisor` start errors

  ## Examples

      # After {Rheo, name: MyRheo, backend: {Rheo.Backend.ETS, []}} is running:
      Rheo.GroupSupervisor.start_group(MyRheo,
        stream: "market-events",
        group: "risk",
        module: MyApp.RiskConsumer,
        max_demand: 10,
        concurrency: 4
      )
  """
  @spec start_group(atom(), keyword()) :: DynamicSupervisor.on_start_child()
  def start_group(rheo, group_opts) do
    spec = {Rheo.Group, Keyword.put(group_opts, :rheo, rheo)}
    DynamicSupervisor.start_child(Rheo.Names.group_supervisor(rheo), spec)
  end
end
