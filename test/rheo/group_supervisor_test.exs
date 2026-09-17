defmodule Rheo.GroupSupervisorTest do
  use ExUnit.Case, async: false

  @moduletag :mongo

  defmodule NoopConsumer do
    @behaviour Rheo.Consumer

    @impl true
    def handle_event(_event, state), do: :ack
  end

  setup do
    :ok = Rheo.Case.ensure_rheo_started()
    :ok = Rheo.Case.stop_all_groups(Rheo)
    :ok
  end

  test "start_group starts a Rheo.Group under the instance supervisor" do
    stream = Rheo.Case.unique_stream("gs")
    :ok = Rheo.create_stream(stream)
    :ok = Rheo.create_group(stream, "risk")

    assert {:ok, pid} =
             Rheo.GroupSupervisor.start_group(Rheo,
               stream: stream,
               group: "risk",
               module: NoopConsumer,
               poll_ms: 200
             )

    assert is_pid(pid)
    assert Process.alive?(pid)

    assert {:error, {:already_started, ^pid}} =
             Rheo.GroupSupervisor.start_group(Rheo,
               stream: stream,
               group: "risk",
               module: NoopConsumer
             )

    :ok = Rheo.Case.stop_all_groups(Rheo)
  end

  test "start_link registers the named DynamicSupervisor" do
    assert is_pid(Process.whereis(Rheo.Names.group_supervisor(Rheo)))
  end
end
