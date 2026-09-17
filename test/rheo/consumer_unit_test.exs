defmodule Rheo.ConsumerUnitTest do
  use ExUnit.Case, async: false

  defmodule TinyConsumer do
    use Rheo.Consumer, stream: "unit-stream", group: "unit-group"

    @impl true
    def handle_event(_event, state), do: :ack
  end

  test "child_spec defaults and overrides" do
    spec = TinyConsumer.child_spec([])
    assert spec.id == {TinyConsumer, Rheo, "unit-stream", "unit-group"}
    assert spec.shutdown == 6_000

    spec2 = TinyConsumer.child_spec(id: :custom, rheo: :Other)
    assert spec2.id == :custom

    assert match?(%{id: _}, TinyConsumer.child_spec(:not_a_list))
  end

  test "start_link without name starts bridge via group starter" do
    parent = self()

    starter = fn rheo, opts ->
      send(parent, {:started, rheo, opts[:module]})
      pid = spawn(fn -> Process.sleep(:infinity) end)
      {:ok, pid}
    end

    assert {:ok, bridge} =
             Rheo.Consumer.start_link(TinyConsumer,
               stream: "s",
               group: "g",
               __group_starter__: starter,
               __group_terminator__: fn _rheo, pid -> Process.exit(pid, :kill) end
             )

    assert_receive {:started, Rheo, TinyConsumer}
    assert Process.alive?(bridge)
    GenServer.stop(bridge)
  end

  test "bridge refuses to share an already-started local group" do
    Process.flag(:trap_exit, true)
    group = spawn(fn -> Process.sleep(:infinity) end)
    starter = fn _rheo, _opts -> {:error, {:already_started, group}} end

    assert {:error, {:group_already_started, ^group}} =
             Rheo.Consumer.start_link(TinyConsumer,
               stream: "s",
               group: "g",
               name: :"shared-#{System.unique_integer()}",
               __group_starter__: starter
             )

    Process.exit(group, :kill)
  end

  test "bridge stops when group starter fails" do
    Process.flag(:trap_exit, true)
    starter = fn _rheo, _opts -> {:error, :no_sup} end

    assert {:error, :no_sup} =
             Rheo.Consumer.start_link(TinyConsumer,
               stream: "s",
               group: "g",
               __group_starter__: starter
             )
  end

  test "bridge stops when monitored group dies" do
    Process.flag(:trap_exit, true)
    group = spawn(fn -> Process.sleep(:infinity) end)

    starter = fn _rheo, _opts -> {:ok, group} end

    assert {:ok, bridge} =
             Rheo.Consumer.start_link(TinyConsumer,
               stream: "s",
               group: "g",
               __group_starter__: starter,
               __group_terminator__: fn _, _ -> :ok end
             )

    ref = Process.monitor(bridge)
    Process.exit(group, :kill)
    assert_receive {:DOWN, ^ref, :process, ^bridge, :killed}, 1_000
  end

  test "bridge ignores unrelated info messages" do
    group = spawn(fn -> Process.sleep(:infinity) end)

    assert {:ok, bridge} =
             Rheo.Consumer.start_link(TinyConsumer,
               stream: "s",
               group: "g",
               __group_starter__: fn _, _ -> {:ok, group} end,
               __group_terminator__: fn _, _ -> :ok end
             )

    send(bridge, :noise)
    assert Process.alive?(bridge)
    GenServer.stop(bridge)
    Process.exit(group, :kill)
  end

  test "terminator error path kills group when terminate_child fails" do
    group = spawn(fn -> Process.sleep(:infinity) end)

    terminator = fn _rheo, pid ->
      # Simulate DynamicSupervisor.terminate_child/2 failure branch
      Process.exit(pid, :kill)
      :ok
    end

    assert {:ok, bridge} =
             Rheo.Consumer.start_link(TinyConsumer,
               stream: "s",
               group: "g",
               __group_starter__: fn _, _ -> {:ok, group} end,
               __group_terminator__: terminator
             )

    GenServer.stop(bridge)
    refute Process.alive?(group)
  end

  test "terminator catching exit still returns ok" do
    group = spawn(fn -> Process.sleep(:infinity) end)

    terminator = fn _rheo, _pid ->
      exit(:bye)
    end

    assert {:ok, bridge} =
             Rheo.Consumer.start_link(TinyConsumer,
               stream: "s",
               group: "g",
               __group_starter__: fn _, _ -> {:ok, group} end,
               __group_terminator__: terminator
             )

    # stop should not raise even if terminator exits
    assert :ok = GenServer.stop(bridge)
    Process.exit(group, :kill)
  end
end
