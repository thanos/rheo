defmodule Rheo.GroupTest do
  use Rheo.Case, async: false

  @moduletag :mongo

  defmodule GateConsumer do
    use Rheo.Consumer, stream: "unused", group: "unused"

    @impl true
    def setup(opts), do: {:ok, %{agent: Keyword.fetch!(opts, :agent)}}

    @impl true
    def handle_event(event, %{agent: agent}) do
      Agent.update(agent, fn xs -> [event.id | xs] end)
      :ack
    end
  end

  defmodule CrashConsumer do
    use Rheo.Consumer, stream: "unused", group: "unused"

    @impl true
    def setup(opts), do: {:ok, %{agent: Keyword.fetch!(opts, :agent)}}

    @impl true
    def handle_event(event, %{agent: agent}) do
      crash? =
        Agent.get_and_update(agent, fn
          %{crash?: true} = s -> {true, %{s | crash?: false, seen: [event.id | s.seen]}}
          s -> {false, %{s | seen: [event.id | s.seen]}}
        end)

      if crash? do
        raise "boom before ack"
      else
        :ack
      end
    end
  end

  defmodule DrainConsumer do
    use Rheo.Consumer, stream: "unused", group: "unused"

    @impl true
    def setup(opts), do: {:ok, %{agent: Keyword.fetch!(opts, :agent)}}

    @impl true
    def handle_event(event, %{agent: agent}) do
      Process.sleep(200)
      Agent.update(agent, fn xs -> [event.id | xs] end)
      :ack
    end
  end

  setup do
    stream = unique_stream("group")
    :ok = Rheo.create_stream(stream)
    :ok = Rheo.create_group(stream, "risk")
    :ok = Rheo.create_group(stream, "surv")
    {:ok, agent} = Agent.start_link(fn -> [] end)
    %{stream: stream, agent: agent}
  end

  test "concurrency and demand process all events", %{stream: stream, agent: agent} do
    {:ok, _} = Rheo.append_batch(stream, for(_ <- 1..6, do: %{type: "e"}))

    {:ok, _} =
      start_supervised(
        {GateConsumer,
         stream: stream,
         group: "risk",
         agent: agent,
         concurrency: 2,
         max_demand: 3,
         poll_ms: 20,
         id: :"conc-#{System.unique_integer()}"}
      )

    wait_until(fn -> length(Agent.get(agent, & &1)) == 6 end)
  end

  test "two local groups consume independently", %{stream: stream} do
    {:ok, agent_a} = Agent.start_link(fn -> [] end)
    {:ok, agent_b} = Agent.start_link(fn -> [] end)
    {:ok, _} = Rheo.append_batch(stream, [%{type: "a"}, %{type: "b"}])

    {:ok, _} =
      start_supervised(
        {GateConsumer,
         stream: stream,
         group: "risk",
         agent: agent_a,
         poll_ms: 20,
         id: :"ga-#{System.unique_integer()}"}
      )

    {:ok, _} =
      start_supervised(
        {GateConsumer,
         stream: stream,
         group: "surv",
         agent: agent_b,
         poll_ms: 20,
         id: :"gb-#{System.unique_integer()}"}
      )

    wait_until(fn -> length(Agent.get(agent_a, & &1)) == 2 end)
    wait_until(fn -> length(Agent.get(agent_b, & &1)) == 2 end)
  end

  test "worker crash before ACK redelivers", %{stream: stream} do
    {:ok, event} = Rheo.append(stream, %{type: "fragile"})
    {:ok, agent} = Agent.start_link(fn -> %{crash?: true, seen: []} end)

    {:ok, _} =
      start_supervised(
        {CrashConsumer,
         stream: stream,
         group: "risk",
         agent: agent,
         lease_ms: 5_000,
         poll_ms: 30,
         max_demand: 1,
         id: :"crash-#{System.unique_integer()}"}
      )

    wait_until(fn ->
      %{seen: seen} = Agent.get(agent, & &1)
      length(Enum.filter(seen, &(&1 == event.id))) >= 2
    end)
  end

  test "no hot polling on empty stream", %{stream: stream, agent: agent} do
    handler_id = "poll-#{System.unique_integer()}"
    parent = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:rheo, :fetch, :stop],
        fn _e, _m, meta, _ -> send(parent, {:fetch, meta[:stream]}) end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    {:ok, _} =
      start_supervised(
        {GateConsumer,
         stream: stream,
         group: "risk",
         agent: agent,
         poll_ms: 150,
         id: :"poll-#{System.unique_integer()}"}
      )

    _ = receive_fetches(stream, 200)
    count = receive_fetches(stream, 400)
    assert count <= 6
  end

  test "graceful shutdown drains inflight", %{stream: stream} do
    {:ok, agent} = Agent.start_link(fn -> [] end)
    {:ok, event} = Rheo.append(stream, %{type: "drain_me"})
    id = :"drain-#{System.unique_integer()}"

    {:ok, pid} =
      start_supervised(
        {DrainConsumer, stream: stream, group: "risk", agent: agent, poll_ms: 20, id: id}
      )

    Process.sleep(50)
    assert :ok = stop_supervised(id)
    refute Process.alive?(pid)
    wait_until(fn -> event.id in Agent.get(agent, & &1) end)
  end

  test "group restart recovers via durable leases", %{stream: stream} do
    {:ok, agent} = Agent.start_link(fn -> [] end)
    {:ok, event} = Rheo.append(stream, %{type: "restart"})
    id = :"rst-#{System.unique_integer()}"

    {:ok, _} =
      start_supervised(
        {GateConsumer, stream: stream, group: "risk", agent: agent, poll_ms: 20, id: id}
      )

    wait_until(fn -> event.id in Agent.get(agent, & &1) end)
    assert :ok = stop_supervised(id)
    :ok = stop_all_groups(Rheo)

    {:ok, agent2} = Agent.start_link(fn -> [] end)
    {:ok, later} = Rheo.append(stream, %{type: "after"})

    {:ok, _} =
      start_supervised(
        {GateConsumer,
         stream: stream,
         group: "risk",
         agent: agent2,
         poll_ms: 20,
         id: :"rst2-#{System.unique_integer()}"}
      )

    wait_until(fn -> later.id in Agent.get(agent2, & &1) end)
  end

  test "fetch errors emit telemetry and backoff", %{stream: stream, agent: agent} do
    handler_id = "err-#{System.unique_integer()}"
    parent = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:rheo, :fetch, :error],
        fn _e, _m, meta, _ -> send(parent, {:fetch_error, meta[:reason]}) end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    # Point consumer at a group that does not exist
    {:ok, _} =
      start_supervised(
        {GateConsumer,
         stream: stream,
         group: "missing-group",
         agent: agent,
         poll_ms: 20,
         id: :"err-#{System.unique_integer()}"}
      )

    assert_receive {:fetch_error, :group_not_found}, 1_000
  end

  defp receive_fetches(stream, window_ms) do
    deadline = System.monotonic_time(:millisecond) + window_ms
    count_fetches(stream, deadline, 0)
  end

  defp count_fetches(stream, deadline, n) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      n
    else
      receive do
        {:fetch, ^stream} -> count_fetches(stream, deadline, n + 1)
        {:fetch, _} -> count_fetches(stream, deadline, n)
      after
        max(remaining, 1) -> n
      end
    end
  end

  defp wait_until(fun, attempts \\ 80) do
    cond do
      fun.() -> true
      attempts <= 0 -> flunk("condition not met")
      true -> Process.sleep(50) && wait_until(fun, attempts - 1)
    end
  end
end
