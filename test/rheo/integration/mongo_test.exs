defmodule Rheo.Integration.MongoTest do
  @moduledoc """
  Heavier MongoDB integration scenarios.

  Excluded by default. Enable with:

      RHEO_INTEGRATION=1 mix test
      mix test --include integration
  """
  use Rheo.Case, async: false

  @moduletag :mongo
  @moduletag :integration

  defmodule IntegrationConsumer do
    use Rheo.Consumer, stream: "unused", group: "unused"

    @impl true
    def setup(opts), do: {:ok, %{agent: Keyword.fetch!(opts, :agent)}}

    @impl true
    def handle_event(event, %{agent: agent} = state) do
      Agent.update(agent, fn ids -> [event.id | ids] end)
      {:ack, state}
    end
  end

  test "end-to-end: multi-instance append, query, consume, renew" do
    url = mongo_url()
    a = String.to_atom("int_a_#{System.unique_integer([:positive])}")
    b = String.to_atom("int_b_#{System.unique_integer([:positive])}")

    {:ok, _} =
      start_supervised(
        {Rheo, name: a, backend: {Rheo.Backend.Mongo, url: url, name: Module.concat(a, Mongo)}}
      )

    {:ok, _} =
      start_supervised(
        {Rheo, name: b, backend: {Rheo.Backend.Mongo, url: url, name: Module.concat(b, Mongo)}}
      )

    stream = unique_stream("integration")
    :ok = Rheo.create_stream(stream, rheo: a)
    :ok = Rheo.create_group(stream, "risk", rheo: a)

    {:ok, batch} =
      Rheo.append_batch(
        stream,
        for(i <- 1..20, do: %{type: "tick", n: i, currency: "EUR"}),
        rheo: a
      )

    assert length(batch) == 20

    q = Rheo.Query.new(stream, where: [type: "tick", currency: "EUR"], limit: 100)
    assert {:ok, found} = Rheo.query(q, rheo: b)
    assert length(found) == 20

    {:ok, [lease]} =
      Rheo.fetch(stream, "risk", limit: 1, consumer_id: "c1", rheo: a, lease_ms: 500)

    assert {:ok, renewed} = Rheo.renew(lease, rheo: a, lease_ms: 5_000)
    assert DateTime.compare(renewed.expires_at, lease.expires_at) == :gt
    assert :ok = Rheo.ack(renewed, rheo: a)

    {:ok, agent} = Agent.start_link(fn -> [] end)

    {:ok, _} =
      start_supervised(
        {IntegrationConsumer,
         rheo: a,
         stream: stream,
         group: "risk",
         agent: agent,
         concurrency: 4,
         max_demand: 10,
         poll_ms: 30,
         id: :"int-#{System.unique_integer()}"}
      )

    wait_until(fn -> length(Agent.get(agent, & &1)) >= 19 end)
  end

  test "reject and nack round-trip against live Mongo" do
    stream = unique_stream("int-settle")
    :ok = Rheo.create_stream(stream)
    :ok = Rheo.create_group(stream, "risk", max_attempts: 2)

    {:ok, _} = Rheo.append(stream, %{type: "poison"})
    {:ok, [lease]} = Rheo.fetch(stream, "risk", limit: 1, consumer_id: "c1")
    assert :ok = Rheo.nack(lease, :tmp)
    {:ok, [again]} = Rheo.fetch(stream, "risk", limit: 1, consumer_id: "c2")
    assert again.attempt == 2
    assert :ok = Rheo.reject(again, :bad)
    assert {:ok, []} = Rheo.fetch(stream, "risk", limit: 1, consumer_id: "c3")
  end

  defp wait_until(fun, attempts \\ 100) do
    cond do
      fun.() -> true
      attempts <= 0 -> flunk("integration condition not met")
      true -> Process.sleep(50) && wait_until(fun, attempts - 1)
    end
  end
end
