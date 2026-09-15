defmodule Rheo.ConsumerTest do
  use Rheo.Case, async: false

  @moduletag :mongo

  defmodule RiskConsumer do
    use Rheo.Consumer, stream: "unused", group: "unused"

    @impl true
    def setup(opts) do
      {:ok, %{agent: Keyword.fetch!(opts, :agent)}}
    end

    @impl true
    def handle_event(event, %{agent: agent} = state) do
      Agent.update(agent, fn events -> [event.id | events] end)
      {:ack, state}
    end
  end

  defmodule RetryConsumer do
    use Rheo.Consumer, stream: "unused", group: "unused"

    @impl true
    def setup(opts), do: {:ok, %{agent: Keyword.fetch!(opts, :agent), fail_once: true}}

    @impl true
    def handle_event(event, %{agent: agent, fail_once: true} = state) do
      Agent.update(agent, fn xs -> [{:retry, event.id} | xs] end)
      {:retry, :boom, %{state | fail_once: false}}
    end

    def handle_event(event, %{agent: agent} = state) do
      Agent.update(agent, fn xs -> [{:ack, event.id} | xs] end)
      {:ack, state}
    end
  end

  setup do
    stream = unique_stream("consumer")
    :ok = Rheo.create_stream(stream)
    :ok = Rheo.create_group(stream, "risk")
    {:ok, agent} = Agent.start_link(fn -> [] end)
    %{stream: stream, agent: agent}
  end

  test "Rheo.Consumer processes and acks events", %{stream: stream, agent: agent} do
    {:ok, [e1, e2]} = Rheo.append_batch(stream, [%{type: "a"}, %{type: "b"}])

    {:ok, _pid} =
      start_supervised(
        {RiskConsumer,
         stream: stream,
         group: "risk",
         agent: agent,
         name: :"risk-#{System.unique_integer()}",
         poll_ms: 50}
      )

    wait_until(fn -> length(Agent.get(agent, & &1)) == 2 end)

    ids = Agent.get(agent, & &1) |> Enum.reverse()
    assert ids == [e1.id, e2.id]
    assert {:ok, []} = Rheo.fetch(stream, "risk", limit: 10, consumer_id: "check")
  end

  test "Rheo.Consumer retries then acks", %{stream: stream, agent: agent} do
    {:ok, [e]} = Rheo.append_batch(stream, [%{type: "retry_me"}])

    {:ok, _pid} =
      start_supervised(
        {RetryConsumer,
         stream: stream,
         group: "risk",
         agent: agent,
         name: :"retry-#{System.unique_integer()}",
         poll_ms: 50,
         max_demand: 1}
      )

    wait_until(fn ->
      actions = Agent.get(agent, & &1)
      {:ack, e.id} in actions and {:retry, e.id} in actions
    end)
  end

  defp wait_until(fun, attempts \\ 50) do
    cond do
      fun.() ->
        true

      attempts <= 0 ->
        flunk("condition not met in time")

      true ->
        Process.sleep(50)
        wait_until(fun, attempts - 1)
    end
  end
end
