defmodule Rheo.RenewTest do
  use Rheo.Case, async: false

  @moduletag :mongo

  defmodule SlowConsumer do
    use Rheo.Consumer, stream: "unused", group: "unused"

    @impl true
    def setup(opts), do: {:ok, %{agent: Keyword.fetch!(opts, :agent)}}

    @impl true
    def handle_event(event, %{agent: agent} = state) do
      Process.sleep(300)
      Agent.update(agent, fn ids -> [event.id | ids] end)
      :ack
    end
  end

  setup do
    stream = unique_stream("renew")
    :ok = Rheo.create_stream(stream)
    :ok = Rheo.create_group(stream, "risk")
    %{stream: stream}
  end

  test "renew extends expires_at", %{stream: stream} do
    {:ok, _} = Rheo.append(stream, %{type: "long"})
    {:ok, [lease]} = Rheo.fetch(stream, "risk", limit: 1, consumer_id: "c1", lease_ms: 1_000)
    original = lease.expires_at

    Process.sleep(50)
    assert {:ok, renewed} = Rheo.renew(lease, lease_ms: 5_000)
    assert DateTime.compare(renewed.expires_at, original) == :gt
    assert renewed.lease_id == lease.lease_id
  end

  test "stale renew fails", %{stream: stream} do
    {:ok, _} = Rheo.append(stream, %{type: "x"})
    {:ok, [lease]} = Rheo.fetch(stream, "risk", limit: 1, consumer_id: "c1")
    :ok = Rheo.ack(lease)
    assert {:error, :stale_lease} = Rheo.renew(lease)
  end

  test "handler longer than original lease succeeds with renewals", %{stream: stream} do
    {:ok, event} = Rheo.append(stream, %{type: "slow"})
    {:ok, agent} = Agent.start_link(fn -> [] end)

    {:ok, _} =
      start_supervised(
        {SlowConsumer,
         stream: stream,
         group: "risk",
         agent: agent,
         lease_ms: 150,
         poll_ms: 30,
         concurrency: 1,
         id: :"slow-#{System.unique_integer()}"}
      )

    wait_until(fn -> event.id in Agent.get(agent, & &1) end)
    assert {:ok, []} = Rheo.fetch(stream, "risk", limit: 1, consumer_id: "check")
  end

  defp wait_until(fun, attempts \\ 80) do
    cond do
      fun.() -> true
      attempts <= 0 -> flunk("condition not met")
      true -> Process.sleep(50) && wait_until(fun, attempts - 1)
    end
  end
end
