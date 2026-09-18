defmodule Rheo.Backend.RedisTest do
  use ExUnit.Case, async: false

  @moduletag :redis

  alias Rheo.Backend.Redis
  alias Rheo.Backend.Redis.Keys
  alias Rheo.Clock.Frozen

  doctest Rheo.Backend.Redis
  doctest Rheo.Backend.Redis.Keys
  doctest Rheo.Backend.Redis.Codec

  defmodule EchoConsumer do
    use Rheo.Consumer, stream: "unused", group: "unused"

    @impl true
    def setup(opts), do: {:ok, %{agent: Keyword.fetch!(opts, :agent)}}

    @impl true
    def handle_event(event, %{agent: agent}) do
      Agent.update(agent, fn ids -> [event.id | ids] end)
      :ack
    end
  end

  setup do
    Frozen.reset()
    name = :"redis_smoke_#{System.unique_integer([:positive])}"

    start_supervised!({Rheo, name: name, backend: {Redis, url: redis_url()}}, id: name)

    handle = Rheo.Instance.fetch!(name).handle
    on_exit(fn -> flush_keys(handle) end)

    %{rheo: name, handle: handle}
  end

  test "ping and ensure_indexes", %{rheo: rheo} do
    assert :ok = Rheo.ping(rheo: rheo)
    assert :ok = Rheo.ensure_indexes(rheo: rheo)
  end

  test "create, append, fetch, ack, and lag", %{rheo: rheo} do
    stream = unique_stream()

    assert :ok = Rheo.create_stream(stream, rheo: rheo)
    assert {:error, :already_exists} = Rheo.create_stream(stream, rheo: rheo)

    assert :ok = Rheo.create_group(stream, "g", rheo: rheo)
    assert {:error, :already_exists} = Rheo.create_group(stream, "g", rheo: rheo)

    assert {:ok, written} =
             Rheo.append_batch(
               stream,
               [%{type: "a", n: 1}, %{type: "b", n: 2}],
               rheo: rheo
             )

    assert Enum.map(written, & &1.sequence) == [1, 2]

    assert {:ok, [first, second]} = Rheo.fetch(stream, "g", limit: 5, rheo: rheo)
    assert first.event.sequence == 1
    assert first.receipt not in [nil, ""]
    assert first.attempt == 1

    assert :ok = Rheo.ack(first, rheo: rheo)
    assert {:error, :stale_lease} = Rheo.ack(first, rheo: rheo)

    assert {:ok, partial} = Rheo.lag(stream, "g", rheo: rheo)
    assert partial.partitions[0].frontier == 1
    assert partial.lag == 1

    assert :ok = Rheo.ack(second, rheo: rheo)
    assert {:ok, drained} = Rheo.lag(stream, "g", rheo: rheo)
    assert drained.partitions[0] == %{frontier: 2, high_watermark: 2, lag: 0}

    assert {:ok, history} = Rheo.read(stream, [after: 0, limit: 10] ++ [rheo: rheo])
    assert Enum.map(history, & &1.id) == Enum.map(written, & &1.id)
    assert Enum.map(history, & &1.payload) == Enum.map(written, & &1.payload)
  end

  test "an expired lease is reclaimed and the stale holder is fenced out", %{rheo: rheo} do
    Frozen.set(DateTime.utc_now())
    stream = unique_stream()

    assert :ok = Rheo.create_stream(stream, rheo: rheo)
    assert :ok = Rheo.create_group(stream, "g", rheo: rheo)
    assert {:ok, event} = Rheo.append(stream, %{type: "e"}, rheo: rheo)

    assert {:ok, [old]} =
             Rheo.fetch(stream, "g", limit: 1, lease_ms: 50, consumer_id: "old", rheo: rheo)

    Frozen.advance(100)

    assert {:ok, [new]} =
             Rheo.fetch(stream, "g", limit: 1, lease_ms: 5_000, consumer_id: "new", rheo: rheo)

    assert new.event_id == event.id
    assert new.receipt == old.receipt
    assert new.attempt == old.attempt + 1
    assert new.lease_id != old.lease_id

    assert {:error, :stale_lease} = Rheo.ack(old, rheo: rheo)
    assert {:error, :receipt_mismatch} = Rheo.ack(%{new | receipt: "1-1"}, rheo: rheo)
    assert :ok = Rheo.ack(new, rheo: rheo)
  end

  test "wait/2 returns a hint without blocking forever", %{rheo: rheo, handle: handle} do
    stream = unique_stream()
    assert :ok = Rheo.create_stream(stream, rheo: rheo)
    assert :ok = Rheo.create_group(stream, "g", rheo: rheo)
    assert {:ok, _} = Rheo.append(stream, %{type: "e"}, rheo: rheo)

    assert :ok =
             Rheo.Backend.Wakeup.wait(Redis, handle,
               timeout: 200,
               stream: stream,
               group: "g"
             )
  end

  test "a consumer drains the stream with the wakeup reader running", %{rheo: rheo} do
    Frozen.reset()
    stream = unique_stream()

    assert :ok = Rheo.create_stream(stream, rheo: rheo)
    assert :ok = Rheo.create_group(stream, "risk", rheo: rheo)
    {:ok, agent} = Agent.start_link(fn -> [] end)

    {:ok, _} =
      start_supervised(
        {EchoConsumer,
         rheo: rheo,
         stream: stream,
         group: "risk",
         agent: agent,
         poll_ms: 20,
         id: :"redis-consumer-#{System.unique_integer([:positive])}"}
      )

    assert {:ok, _} = Rheo.append_batch(stream, for(_ <- 1..5, do: %{type: "e"}), rheo: rheo)

    wait_until(fn -> length(Agent.get(agent, & &1)) == 5 end)

    wait_until(fn ->
      match?({:ok, %{lag: 0}}, Rheo.lag(stream, "risk", rheo: rheo))
    end)

    assert {:ok, %{lag: 0}} = Rheo.lag(stream, "risk", rheo: rheo)
  end

  defp wait_until(fun, timeout \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_until(fun, deadline)
  end

  defp do_wait_until(fun, deadline) do
    if fun.() do
      :ok
    else
      if System.monotonic_time(:millisecond) > deadline do
        flunk("condition not met within timeout")
      else
        Process.sleep(20)
        do_wait_until(fun, deadline)
      end
    end
  end

  defp unique_stream, do: "redis-#{System.unique_integer([:positive])}"

  defp redis_url, do: Rheo.Test.Redis.url()

  defp flush_keys(handle), do: Rheo.Test.Redis.flush(Keys.prefix(handle) <> "*")
end
