defmodule Rheo.GroupUnitTest do
  use Rheo.Case, async: false

  alias Rheo.Clock.Frozen

  @moduletag :mongo

  defmodule NoSetupConsumer do
    @behaviour Rheo.Consumer

    @impl true
    def handle_event(_event, _ctx), do: :ack
  end

  defmodule StopSetupConsumer do
    use Rheo.Consumer, stream: "unused", group: "unused"

    @impl true
    def setup(_opts), do: {:stop, :setup_refused}

    @impl true
    def handle_event(_event, _ctx), do: :ack
  end

  defmodule InvalidReturnConsumer do
    use Rheo.Consumer, stream: "unused", group: "unused"

    @impl true
    def setup(opts), do: {:ok, %{agent: Keyword.fetch!(opts, :agent)}}

    @impl true
    def handle_event(_event, %{agent: agent}) do
      Agent.update(agent, &(&1 + 1))
      :not_an_outcome
    end
  end

  defmodule ThrowConsumer do
    use Rheo.Consumer, stream: "unused", group: "unused"

    @impl true
    def setup(opts), do: {:ok, %{agent: Keyword.fetch!(opts, :agent)}}

    @impl true
    def handle_event(_event, %{agent: agent}) do
      Agent.update(agent, &(&1 + 1))
      throw(:handler_throw)
    end
  end

  defmodule RejectConsumer do
    use Rheo.Consumer, stream: "unused", group: "unused"

    @impl true
    def setup(opts), do: {:ok, %{agent: Keyword.fetch!(opts, :agent)}}

    @impl true
    def handle_event(event, %{agent: agent}) do
      Agent.update(agent, fn xs -> [{:reject, event.id} | xs] end)
      {:reject, :bad_payload}
    end
  end

  defmodule SlowAckConsumer do
    use Rheo.Consumer, stream: "unused", group: "unused"

    @impl true
    def handle_event(_event, _ctx) do
      Process.sleep(400)
      :ack
    end
  end

  defmodule SlowRetryConsumer do
    use Rheo.Consumer, stream: "unused", group: "unused"

    @impl true
    def handle_event(_event, _ctx) do
      Process.sleep(400)
      {:retry, :boom}
    end
  end

  defmodule SlowRejectConsumer do
    use Rheo.Consumer, stream: "unused", group: "unused"

    @impl true
    def handle_event(_event, _ctx) do
      Process.sleep(400)
      {:reject, :bad}
    end
  end

  defmodule SlowSlotConsumer do
    use Rheo.Consumer, stream: "unused", group: "unused"

    @impl true
    def setup(opts), do: {:ok, %{agent: Keyword.fetch!(opts, :agent)}}

    @impl true
    def handle_event(event, %{agent: agent}) do
      Process.sleep(400)
      Agent.update(agent, fn xs -> [event.id | xs] end)
      :ack
    end
  end

  defmodule RenewFailConsumer do
    use Rheo.Consumer, stream: "unused", group: "unused"

    @impl true
    def handle_event(_event, _ctx) do
      Process.sleep(800)
      :ack
    end
  end

  defmodule KillHangConsumer do
    use Rheo.Consumer, stream: "unused", group: "unused"

    @impl true
    def setup(opts), do: {:ok, %{agent: Keyword.fetch!(opts, :agent)}}

    @impl true
    def handle_event(event, %{agent: agent}) do
      worker = self()
      Agent.update(agent, fn _ -> %{pid: worker, event_id: event.id} end)
      Process.sleep(:infinity)
      :ack
    end
  end

  setup do
    Frozen.reset()
    stream = unique_stream("group-unit")
    :ok = Rheo.create_stream(stream)
    :ok = Rheo.create_group(stream, "risk")
    :ok = Rheo.create_group(stream, "invalid")
    %{stream: stream}
  end

  test "child_spec and direct start_link register a local name", %{stream: stream} do
    spec =
      Rheo.Group.child_spec(
        rheo: Rheo,
        stream: stream,
        group: "direct",
        module: NoSetupConsumer,
        poll_ms: 500
      )

    assert spec.id == {Rheo.Group, Rheo, stream, "direct"}
    assert spec.shutdown == 6_000

    {:ok, pid} =
      Rheo.Group.start_link(
        rheo: Rheo,
        stream: stream,
        group: "direct",
        module: NoSetupConsumer,
        poll_ms: 500
      )

    assert Process.alive?(pid)
    assert GenServer.whereis(Rheo.Names.group(Rheo, stream, "direct")) == pid
    assert :ok = GenServer.stop(pid)
  end

  test "init aborts when setup returns stop", %{stream: stream} do
    assert {:error, :setup_refused} =
             Rheo.GroupSupervisor.start_group(Rheo,
               stream: stream,
               group: "stop-setup",
               module: StopSetupConsumer
             )
  end

  test "init coerces concurrency below 1 to 1", %{stream: stream} do
    {:ok, pid} =
      Rheo.GroupSupervisor.start_group(Rheo,
        stream: stream,
        group: "conc-zero",
        module: NoSetupConsumer,
        concurrency: 0,
        poll_ms: 500
      )

    assert Process.alive?(pid)
    :ok = DynamicSupervisor.terminate_child(Rheo.Names.group_supervisor(Rheo), pid)
  end

  test "invalid handler return is nacked", %{stream: stream} do
    {:ok, _event} = Rheo.append(stream, %{type: "bad_return"})
    {:ok, agent} = Agent.start_link(fn -> 0 end)
    id = :"inv-#{System.unique_integer()}"

    {:ok, _} =
      start_supervised(
        {InvalidReturnConsumer,
         stream: stream, group: "invalid", agent: agent, poll_ms: 30, id: id}
      )

    wait_until(fn -> Agent.get(agent, & &1) >= 2 end)
    assert :ok = stop_supervised(id)
  end

  test "reject outcome dead-letters for the group", %{stream: stream} do
    {:ok, _} = Rheo.append(stream, %{type: "poison"})
    {:ok, agent} = Agent.start_link(fn -> [] end)

    {:ok, _} =
      start_supervised(
        {RejectConsumer,
         stream: stream,
         group: "risk",
         agent: agent,
         poll_ms: 30,
         id: :"rej-#{System.unique_integer()}"}
      )

    wait_until(fn -> Agent.get(agent, & &1) != [] end)
    assert {:reject, _} = hd(Agent.get(agent, & &1))
    assert {:ok, []} = Rheo.fetch(stream, "risk", limit: 1, consumer_id: "after-reject")
  end

  test "handler throw is treated as handler_error", %{stream: stream} do
    {:ok, _event} = Rheo.append(stream, %{type: "throw_me"})
    {:ok, agent} = Agent.start_link(fn -> 0 end)
    id = :"throw-#{System.unique_integer()}"

    {:ok, _} =
      start_supervised(
        {ThrowConsumer, stream: stream, group: "invalid", agent: agent, poll_ms: 30, id: id}
      )

    wait_until(fn -> Agent.get(agent, & &1) >= 2 end)
    assert :ok = stop_supervised(id)
  end

  test "failed ack on a stolen lease emits telemetry and does not nack", %{stream: stream} do
    Frozen.set(DateTime.utc_now())

    {:ok, event} = Rheo.append(stream, %{type: "stale_ack"})
    parent = self()
    handler_id = "ack-fail-#{System.unique_integer()}"
    lease_handler = "lease-#{System.unique_integer()}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:rheo, :ack, :error],
        fn _e, _m, meta, _ ->
          if meta[:event_id] == event.id, do: send(parent, {:ack_error, meta[:reason]})
        end,
        nil
      )

    :ok =
      :telemetry.attach(
        lease_handler,
        [:rheo, :lease],
        fn _e, %{count: c}, meta, _ ->
          if c > 0 and meta[:stream] == stream, do: send(parent, :group_leased)
        end,
        nil
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
      :telemetry.detach(lease_handler)
    end)

    # Long lease_ms keeps renew from firing during the steal window; Frozen expires it.
    lease_ms = 30_000

    {:ok, group_pid} =
      Rheo.GroupSupervisor.start_group(Rheo,
        stream: stream,
        group: "risk",
        module: SlowAckConsumer,
        poll_ms: 20,
        lease_ms: lease_ms,
        max_demand: 1,
        concurrency: 1
      )

    assert_receive :group_leased, 2_000
    Frozen.advance(lease_ms + 1)

    assert {:ok, [_]} =
             Rheo.fetch(stream, "risk", limit: 1, consumer_id: "stealer", lease_ms: 500)

    assert_receive {:ack_error, :stale_lease}, 2_000
    :ok = DynamicSupervisor.terminate_child(Rheo.Names.group_supervisor(Rheo), group_pid)
  end

  test "drain waits for inflight work", %{stream: stream} do
    {:ok, event} = Rheo.append(stream, %{type: "drain_api"})
    {:ok, agent} = Agent.start_link(fn -> [] end)

    {:ok, group_pid} =
      Rheo.GroupSupervisor.start_group(Rheo,
        stream: stream,
        group: "risk",
        module: SlowSlotConsumer,
        agent: agent,
        poll_ms: 20,
        concurrency: 1,
        max_demand: 1
      )

    wait_until(fn -> event.id in Agent.get(agent, & &1) end)
    assert :ok = Rheo.Group.drain(group_pid, 3_000)
    :ok = DynamicSupervisor.terminate_child(Rheo.Names.group_supervisor(Rheo), group_pid)
  end

  test "drain returns timeout when work exceeds budget", %{stream: stream} do
    {:ok, _} = Rheo.append(stream, %{type: "drain_timeout"})

    {:ok, group_pid} =
      Rheo.GroupSupervisor.start_group(Rheo,
        stream: stream,
        group: "risk",
        module: SlowAckConsumer,
        poll_ms: 20,
        max_demand: 1
      )

    Process.sleep(80)
    assert :timeout = Rheo.Group.drain(group_pid, 50)
    :ok = DynamicSupervisor.terminate_child(Rheo.Names.group_supervisor(Rheo), group_pid)
  end

  test "renew failure is logged while inflight", %{stream: stream} do
    Frozen.set(DateTime.utc_now())

    {:ok, event} = Rheo.append(stream, %{type: "renew_fail"})
    parent = self()
    handler_id = "renew-#{System.unique_integer()}"
    lease_handler = "lease-renew-#{System.unique_integer()}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:rheo, :lease, :renew],
        fn _e, _m, meta, _ ->
          if meta[:event_id] == event.id, do: send(parent, {:renew, meta[:result]})
        end,
        nil
      )

    :ok =
      :telemetry.attach(
        lease_handler,
        [:rheo, :lease],
        fn _e, %{count: c}, meta, _ ->
          if c > 0 and meta[:stream] == stream, do: send(parent, :group_leased)
        end,
        nil
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
      :telemetry.detach(lease_handler)
    end)

    {:ok, group_pid} =
      Rheo.GroupSupervisor.start_group(Rheo,
        stream: stream,
        group: "risk",
        module: RenewFailConsumer,
        poll_ms: 20,
        lease_ms: 200,
        max_demand: 1
      )

    assert_receive :group_leased, 2_000
    Frozen.advance(250)

    wait_until(fn ->
      case Rheo.fetch(stream, "risk", limit: 1, consumer_id: "stealer", lease_ms: 500) do
        {:ok, [_]} ->
          true

        {:ok, []} ->
          Frozen.advance(250)
          false
      end
    end)

    assert_receive {:renew, :stale_lease}, 2_000
    :ok = DynamicSupervisor.terminate_child(Rheo.Names.group_supervisor(Rheo), group_pid)
  end

  test "group ignores unknown messages", %{stream: stream} do
    {:ok, pid} =
      Rheo.GroupSupervisor.start_group(Rheo,
        stream: stream,
        group: "noise",
        module: NoSetupConsumer,
        poll_ms: 500
      )

    send(pid, :random_noise)
    assert Process.alive?(pid)
    :ok = DynamicSupervisor.terminate_child(Rheo.Names.group_supervisor(Rheo), pid)
  end

  test "terminate drains and emits stop telemetry", %{stream: stream} do
    {:ok, _} = Rheo.append(stream, %{type: "term"})
    {:ok, agent} = Agent.start_link(fn -> [] end)

    {:ok, pid} =
      Rheo.GroupSupervisor.start_group(Rheo,
        stream: stream,
        group: "risk",
        module: SlowSlotConsumer,
        agent: agent,
        poll_ms: 20
      )

    Process.sleep(100)
    ref = Process.monitor(pid)
    Process.exit(pid, :shutdown)
    assert_receive {:DOWN, ^ref, :process, ^pid, :shutdown}, 3_000
  end

  test "concurrency caps inflight slots", %{stream: stream} do
    {:ok, _} = Rheo.append_batch(stream, for(_ <- 1..3, do: %{type: "slot"}))
    {:ok, agent} = Agent.start_link(fn -> [] end)

    {:ok, _} =
      start_supervised(
        {SlowSlotConsumer,
         stream: stream,
         group: "risk",
         agent: agent,
         concurrency: 1,
         max_demand: 3,
         poll_ms: 30,
         id: :"slot-#{System.unique_integer()}"}
      )

    wait_until(fn -> length(Agent.get(agent, & &1)) == 3 end)
  end

  test "failed nack emits retry error telemetry", %{stream: stream} do
    Frozen.set(DateTime.utc_now())

    {:ok, event} = Rheo.append(stream, %{type: "nack_fail"})
    parent = self()
    handler_id = "retry-err-#{System.unique_integer()}"
    lease_handler = "lease-nack-#{System.unique_integer()}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:rheo, :retry, :error],
        fn _e, _m, meta, _ ->
          if meta[:event_id] == event.id, do: send(parent, {:retry_error, meta[:reason]})
        end,
        nil
      )

    :ok =
      :telemetry.attach(
        lease_handler,
        [:rheo, :lease],
        fn _e, %{count: c}, meta, _ ->
          if c > 0 and meta[:stream] == stream, do: send(parent, :group_leased)
        end,
        nil
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
      :telemetry.detach(lease_handler)
    end)

    lease_ms = 30_000

    {:ok, group_pid} =
      Rheo.GroupSupervisor.start_group(Rheo,
        stream: stream,
        group: "risk",
        module: SlowRetryConsumer,
        poll_ms: 20,
        lease_ms: lease_ms,
        max_demand: 1
      )

    assert_receive :group_leased, 2_000
    Frozen.advance(lease_ms + 1)

    assert {:ok, [_]} =
             Rheo.fetch(stream, "risk", limit: 1, consumer_id: "stealer", lease_ms: 500)

    assert_receive {:retry_error, :stale_lease}, 2_000
    :ok = DynamicSupervisor.terminate_child(Rheo.Names.group_supervisor(Rheo), group_pid)
  end

  test "failed reject emits reject error telemetry", %{stream: stream} do
    Frozen.set(DateTime.utc_now())

    {:ok, event} = Rheo.append(stream, %{type: "reject_fail"})
    parent = self()
    handler_id = "reject-err-#{System.unique_integer()}"
    lease_handler = "lease-rej-#{System.unique_integer()}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:rheo, :reject, :error],
        fn _e, _m, meta, _ ->
          if meta[:event_id] == event.id, do: send(parent, {:reject_error, meta[:reason]})
        end,
        nil
      )

    :ok =
      :telemetry.attach(
        lease_handler,
        [:rheo, :lease],
        fn _e, %{count: c}, meta, _ ->
          if c > 0 and meta[:stream] == stream, do: send(parent, :group_leased)
        end,
        nil
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
      :telemetry.detach(lease_handler)
    end)

    lease_ms = 30_000

    {:ok, group_pid} =
      Rheo.GroupSupervisor.start_group(Rheo,
        stream: stream,
        group: "risk",
        module: SlowRejectConsumer,
        poll_ms: 20,
        lease_ms: lease_ms,
        max_demand: 1
      )

    assert_receive :group_leased, 2_000
    Frozen.advance(lease_ms + 1)

    assert {:ok, [_]} =
             Rheo.fetch(stream, "risk", limit: 1, consumer_id: "stealer", lease_ms: 500)

    assert_receive {:reject_error, :stale_lease}, 2_000
    :ok = DynamicSupervisor.terminate_child(Rheo.Names.group_supervisor(Rheo), group_pid)
  end

  test "draining group stops scheduling new fetches", %{stream: stream} do
    {:ok, _} = Rheo.append(stream, %{type: "drain_only"})

    {:ok, group_pid} =
      Rheo.GroupSupervisor.start_group(Rheo,
        stream: stream,
        group: "risk",
        module: SlowAckConsumer,
        poll_ms: 20,
        max_demand: 1
      )

    Process.sleep(50)
    assert :ok = Rheo.Group.drain(group_pid, 2_000)

    count =
      receive_fetches_while(group_pid, 300)

    assert count == 0
    :ok = DynamicSupervisor.terminate_child(Rheo.Names.group_supervisor(Rheo), group_pid)
  end

  test "handle_worker_down on kill emits crash telemetry and leaves lease", %{stream: stream} do
    Frozen.set(DateTime.utc_now())
    {:ok, event} = Rheo.append(stream, %{type: "kill_me"})
    {:ok, agent} = Agent.start_link(fn -> nil end)
    parent = self()
    crash_handler = "worker-crash-#{System.unique_integer()}"

    :ok =
      :telemetry.attach(
        crash_handler,
        [:rheo, :worker, :crash],
        fn _e, %{count: 1}, meta, _ ->
          if meta[:event_id] == event.id do
            send(parent, {:worker_crash, meta[:reason]})
          end
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(crash_handler) end)

    lease_ms = 30_000

    {:ok, group_pid} =
      Rheo.GroupSupervisor.start_group(Rheo,
        stream: stream,
        group: "risk",
        module: KillHangConsumer,
        agent: agent,
        poll_ms: 20,
        lease_ms: lease_ms,
        max_demand: 1,
        concurrency: 1
      )

    wait_until(fn -> match?(%{pid: pid} when is_pid(pid), Agent.get(agent, & &1)) end)
    %{pid: worker} = Agent.get(agent, & &1)

    Process.exit(worker, :kill)

    assert_receive {:worker_crash, :killed}, 2_000
    assert Process.alive?(group_pid)

    :ok = DynamicSupervisor.terminate_child(Rheo.Names.group_supervisor(Rheo), group_pid)

    Frozen.advance(lease_ms + 1)

    assert {:ok, [again]} =
             Rheo.fetch(stream, "risk", limit: 1, consumer_id: "after-kill", lease_ms: 1_000)

    assert again.event_id == event.id
  end

  test "handle_worker_down ignores unknown monitor refs", %{stream: stream} do
    {:ok, group_pid} =
      Rheo.GroupSupervisor.start_group(Rheo,
        stream: stream,
        group: "risk",
        module: NoSetupConsumer,
        poll_ms: 500
      )

    send(group_pid, {:DOWN, make_ref(), :process, self(), :noproc})
    Process.sleep(50)
    assert Process.alive?(group_pid)

    :ok = DynamicSupervisor.terminate_child(Rheo.Names.group_supervisor(Rheo), group_pid)
  end

  test "handle_worker_down during drain does not ACK", %{stream: stream} do
    Frozen.set(DateTime.utc_now())
    {:ok, event} = Rheo.append(stream, %{type: "drain_kill"})
    {:ok, agent} = Agent.start_link(fn -> nil end)
    parent = self()
    crash_handler = "drain-crash-#{System.unique_integer()}"

    :ok =
      :telemetry.attach(
        crash_handler,
        [:rheo, :worker, :crash],
        fn _e, _m, meta, _ ->
          if meta[:event_id] == event.id, do: send(parent, :drain_worker_crash)
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(crash_handler) end)

    lease_ms = 5_000

    {:ok, group_pid} =
      Rheo.GroupSupervisor.start_group(Rheo,
        stream: stream,
        group: "risk",
        module: KillHangConsumer,
        agent: agent,
        poll_ms: 20,
        lease_ms: lease_ms,
        max_demand: 1
      )

    wait_until(fn -> match?(%{pid: pid} when is_pid(pid), Agent.get(agent, & &1)) end)
    %{pid: worker} = Agent.get(agent, & &1)

    drain_task =
      Task.async(fn -> Rheo.Group.drain(group_pid, 2_000) end)

    Process.sleep(30)
    Process.exit(worker, :kill)

    assert_receive :drain_worker_crash, 2_000
    assert :ok = Task.await(drain_task, 3_000)

    Frozen.advance(lease_ms + 1)

    assert {:ok, [again]} =
             Rheo.fetch(stream, "risk", limit: 1, consumer_id: "after-drain-kill")

    assert again.event_id == event.id

    :ok = DynamicSupervisor.terminate_child(Rheo.Names.group_supervisor(Rheo), group_pid)
  end

  defp receive_fetches_while(group_pid, window_ms) do
    deadline = System.monotonic_time(:millisecond) + window_ms

    receive do
      _ -> receive_fetches_while(group_pid, deadline - System.monotonic_time(:millisecond))
    after
      max(deadline - System.monotonic_time(:millisecond), 1) -> 0
    end
  end

  defp wait_until(fun, attempts \\ 100) do
    cond do
      fun.() -> true
      attempts <= 0 -> flunk("condition not met")
      true -> Process.sleep(50) && wait_until(fun, attempts - 1)
    end
  end
end
