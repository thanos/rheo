defmodule Rheo.GroupPollTest do
  use ExUnit.Case, async: false

  defmodule IdleConsumer do
    use Rheo.Consumer, stream: "unused", group: "unused"

    @impl true
    def setup(opts), do: {:ok, %{agent: Keyword.fetch!(opts, :agent)}}

    @impl true
    def handle_event(event, %{agent: agent}) do
      Agent.update(agent, fn xs -> [event.id | xs] end)
      :ack
    end
  end

  test "idle reductions stay flat after settling events" do
    rheo = :"group_poll_#{System.unique_integer([:positive])}"
    stream = "poll-#{System.unique_integer([:positive])}"
    start_supervised!({Rheo, name: rheo, backend: Rheo.Backend.ETS}, id: rheo)
    :ok = Rheo.create_stream(stream, rheo: rheo)
    :ok = Rheo.create_group(stream, "g", rheo: rheo)
    {:ok, agent} = Agent.start_link(fn -> [] end)

    {:ok, pid} =
      start_supervised(
        {IdleConsumer,
         rheo: rheo,
         stream: stream,
         group: "g",
         agent: agent,
         poll_ms: 100,
         lease_ms: 60_000,
         concurrency: 4,
         max_demand: 4,
         id: :"idle-#{System.unique_integer([:positive])}"}
      )

    # Let the empty-stream poll settle before sampling.
    Process.sleep(250)
    before = idle_reductions(pid)

    events = for n <- 1..200, do: %{type: "e", n: n}
    assert {:ok, _} = Rheo.append_batch(stream, events, rheo: rheo)
    wait_until(fn -> length(Agent.get(agent, & &1)) == 200 end)

    Process.sleep(250)
    after_idle = idle_reductions(pid)

    assert after_idle / max(before, 1) < 5,
           "idle reductions grew from #{before}/s to #{after_idle}/s after 200 events"
  end

  defp idle_reductions(pid, window_ms \\ 1_000) do
    {_, start} = Process.info(pid, :reductions)
    Process.sleep(window_ms)
    {_, stop} = Process.info(pid, :reductions)
    stop - start
  end

  defp wait_until(fun, attempts \\ 80) do
    cond do
      fun.() -> true
      attempts <= 0 -> flunk("condition not met")
      true -> Process.sleep(50) && wait_until(fun, attempts - 1)
    end
  end
end
