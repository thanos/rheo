defmodule Rheo.ConsumerUnitTest do
  use ExUnit.Case, async: false

  defmodule TinyConsumer do
    use Rheo.Consumer, stream: "unit-stream", group: "unit-group"

    @impl true
    def handle_event(_event, _ctx), do: :ack
  end

  defmodule BadSetupConsumer do
    use Rheo.Consumer, stream: "unit-stream", group: "bad-setup"

    @impl true
    def setup(_opts), do: {:ok, :not_a_map}

    @impl true
    def handle_event(_event, _ctx), do: :ack
  end

  defmodule RefusingConsumer do
    use Rheo.Consumer, stream: "unit-stream", group: "refusing"

    @impl true
    def setup(_opts), do: {:stop, :setup_refused}

    @impl true
    def handle_event(_event, _ctx), do: :ack
  end

  setup do
    rheo = :"consumer_unit_#{System.unique_integer([:positive])}"
    start_supervised!({Rheo, name: rheo, backend: Rheo.Backend.ETS}, id: rheo)
    :ok = Rheo.create_stream("unit-stream", rheo: rheo)
    :ok = Rheo.create_group("unit-stream", "unit-group", rheo: rheo)
    %{rheo: rheo}
  end

  test "child_spec targets Rheo.Group with the consumer as id" do
    spec = TinyConsumer.child_spec([])
    assert spec.id == {TinyConsumer, Rheo, "unit-stream", "unit-group"}
    assert spec.restart == :permanent
    assert spec.shutdown == Rheo.Group.child_spec(rheo: Rheo, stream: "s", group: "g").shutdown

    {Rheo.Consumer, :start_link, [TinyConsumer, opts]} = spec.start
    assert opts[:module] == TinyConsumer
    refute Keyword.has_key?(opts, :id)

    assert TinyConsumer.child_spec(id: :custom, rheo: :other).id == :custom
    assert TinyConsumer.child_spec(:not_a_list).id == spec.id
  end

  test "start_link starts the registered group", %{rheo: rheo} do
    assert {:ok, pid} = TinyConsumer.start_link(rheo: rheo, poll_ms: 500)
    assert GenServer.whereis(Rheo.Names.group(rheo, "unit-stream", "unit-group")) == pid
    assert :ok = GenServer.stop(pid)
  end

  test "a second consumer for the same local group is refused", %{rheo: rheo} do
    assert {:ok, pid} = TinyConsumer.start_link(rheo: rheo, poll_ms: 500)
    assert {:error, {:already_started, ^pid}} = TinyConsumer.start_link(rheo: rheo)
    assert :ok = GenServer.stop(pid)
  end

  test "setup must return a map", %{rheo: rheo} do
    Process.flag(:trap_exit, true)
    :ok = Rheo.create_group("unit-stream", "bad-setup", rheo: rheo)
    :ok = Rheo.create_group("unit-stream", "refusing", rheo: rheo)

    assert {:error, {:invalid_context, :not_a_map}} = BadSetupConsumer.start_link(rheo: rheo)
    assert {:error, :setup_refused} = RefusingConsumer.start_link(rheo: rheo)
  end

  test "the host supervisor restarts a crashed group as its single owner", %{rheo: rheo} do
    Process.flag(:trap_exit, true)

    {:ok, host} =
      Supervisor.start_link([{TinyConsumer, rheo: rheo, poll_ms: 500}],
        strategy: :one_for_one,
        max_restarts: 3,
        max_seconds: 5
      )

    name = Rheo.Names.group(rheo, "unit-stream", "unit-group")
    group = GenServer.whereis(name)
    ref = Process.monitor(group)
    Process.exit(group, :kill)
    assert_receive {:DOWN, ^ref, :process, ^group, :killed}, 1_000

    restarted =
      Enum.find_value(1..50, fn _ ->
        case GenServer.whereis(name) do
          pid when is_pid(pid) and pid != group -> pid
          _ -> Process.sleep(10) && nil
        end
      end)

    assert is_pid(restarted)
    assert Process.alive?(host)
    assert [{_, ^restarted, :worker, _}] = Supervisor.which_children(host)
    assert DynamicSupervisor.which_children(Rheo.Names.group_supervisor(rheo)) == []

    :ok = Supervisor.stop(host)
  end
end
