defmodule Rheo.ConsumerBridgeTest do
  use Rheo.Case, async: false

  @moduletag :mongo

  defmodule LiveConsumer do
    use Rheo.Consumer, stream: "unused", group: "unused"

    @impl true
    def handle_event(_event, state), do: {:ack, state}
  end

  setup do
    :ok = ensure_rheo_started()
    stream = unique_stream("bridge")
    :ok = Rheo.create_stream(stream)
    :ok = Rheo.create_group(stream, "risk")
    %{stream: stream}
  end

  test "default terminate stops the supervised group child" do
    stream = unique_stream("bridge-term")
    :ok = Rheo.create_stream(stream)
    :ok = Rheo.create_group(stream, "risk")

    assert {:ok, bridge} =
             Rheo.Consumer.start_link(LiveConsumer,
               stream: stream,
               group: "risk",
               poll_ms: 200,
               name: :"bridge-term-#{System.unique_integer()}"
             )

    group_pid = GenServer.whereis(Rheo.Names.group(Rheo, stream, "risk"))
    assert is_pid(group_pid)
    assert Process.alive?(group_pid)

    assert :ok = GenServer.stop(bridge)
    refute Process.alive?(group_pid)
  end

  test "bridge dies when monitored group is killed", %{stream: stream} do
    Process.flag(:trap_exit, true)

    assert {:ok, bridge} =
             Rheo.Consumer.start_link(LiveConsumer,
               stream: stream,
               group: "risk",
               poll_ms: 200,
               name: :"bridge-kill-#{System.unique_integer()}"
             )

    group_pid = GenServer.whereis(Rheo.Names.group(Rheo, stream, "risk"))
    ref = Process.monitor(bridge)
    Process.exit(group_pid, :kill)

    assert_receive {:DOWN, ^ref, :process, ^bridge, :killed}, 1_000
  end

  test "Rheo.Consumer.child_spec/2 builds expected spec", %{stream: stream} do
    spec =
      Rheo.Consumer.child_spec(LiveConsumer,
        stream: stream,
        group: "risk",
        rheo: Rheo,
        id: :bridge_spec
      )

    assert spec.id == :bridge_spec

    assert spec.start ==
             {Rheo.Consumer, :start_link,
              [LiveConsumer, [stream: stream, group: "risk", rheo: Rheo, id: :bridge_spec]]}
  end
end
