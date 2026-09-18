defmodule Rheo.Backend.RedisBroadwayTest do
  # Broadway / Producer smoke against Redis Streams — receipts are entry ids,
  # settle still goes through Rheo.Producer + Rheo.Broadway.Acknowledger.
  use ExUnit.Case, async: false

  @moduletag :redis

  alias Rheo.Backend.Redis
  alias Rheo.Backend.Redis.Keys
  alias Rheo.{Lease, Producer}

  defmodule Pipeline do
    @moduledoc false
    use Broadway

    def start_link(opts) do
      Broadway.start_link(__MODULE__,
        name: Keyword.fetch!(opts, :name),
        context: %{
          owner: Keyword.fetch!(opts, :owner),
          verdict: Keyword.fetch!(opts, :verdict)
        },
        producer: [
          module: {Rheo.Producer, Keyword.fetch!(opts, :producer)},
          transformer: {Rheo.Broadway, :transform, []},
          concurrency: 1
        ],
        processors: [default: [concurrency: 1]]
      )
    end

    @impl true
    def handle_message(_processor, message, %{owner: owner, verdict: verdict}) do
      send(owner, {:handled, message.data, message.metadata})
      verdict.(message)
    end
  end

  defmodule Sink do
    @moduledoc false
    use GenStage

    def start_link(opts), do: GenStage.start_link(__MODULE__, opts)

    @impl true
    def init(opts) do
      {:consumer, Keyword.fetch!(opts, :owner),
       subscribe_to: [{Keyword.fetch!(opts, :producer), max_demand: 10}]}
    end

    @impl true
    def handle_events(leases, _from, owner) do
      Enum.each(leases, &send(owner, {:lease, &1}))
      {:noreply, [], owner}
    end
  end

  setup do
    rheo = :"redis_bw_#{System.unique_integer([:positive])}"
    start_supervised!({Rheo, name: rheo, backend: {Redis, url: Rheo.Test.Redis.url()}}, id: rheo)
    handle = Rheo.Instance.fetch!(rheo).handle
    on_exit(fn -> Rheo.Test.Redis.flush(Keys.prefix(handle) <> "*") end)

    stream = "redis-bw-#{System.unique_integer([:positive])}"
    :ok = Rheo.create_stream(stream, rheo: rheo)
    :ok = Rheo.create_group(stream, "risk", rheo: rheo, max_attempts: 5)

    %{rheo: rheo, stream: stream}
  end

  test "Broadway ACKs and advances the frontier with Redis receipts", ctx do
    {:ok, written} =
      Rheo.append_batch(ctx.stream, [%{type: "a"}, %{type: "b"}, %{type: "c"}], rheo: ctx.rheo)

    start_pipeline!(ctx, fn message -> message end)

    assert_receive {:handled, event, metadata}, 3_000
    assert %Rheo.Event{} = event
    assert metadata.lease.receipt not in [nil, ""]
    assert metadata.lease.receipt != metadata.lease.lease_id
    assert event.id in Enum.map(written, & &1.id)

    assert_receive {:handled, _, _}, 3_000
    assert_receive {:handled, _, _}, 3_000

    wait_until(fn ->
      match?({:ok, %{lag: 0}}, Rheo.lag(ctx.stream, "risk", rheo: ctx.rheo))
    end)
  end

  test "Broadway NACK redelivers on Redis", ctx do
    {:ok, _} = Rheo.append(ctx.stream, %{type: "flaky"}, rheo: ctx.rheo)

    verdict = fn message ->
      if message.metadata.attempt == 1 do
        Broadway.Message.failed(message, :transient)
      else
        message
      end
    end

    start_pipeline!(ctx, verdict, poll_ms: 20)

    assert_receive {:handled, _, %{attempt: 1, lease: %Lease{receipt: receipt}}}, 3_000
    assert is_binary(receipt)
    assert_receive {:handled, _, %{attempt: 2}}, 3_000

    wait_until(fn ->
      match?({:ok, %{lag: 0}}, Rheo.lag(ctx.stream, "risk", rheo: ctx.rheo))
    end)
  end

  test "Producer emits Redis leases and settles through Producer.ack", ctx do
    {:ok, _} =
      Rheo.append_batch(ctx.stream, [%{type: "p1"}, %{type: "p2"}], rheo: ctx.rheo)

    {:ok, producer} =
      Producer.start_link(
        rheo: ctx.rheo,
        stream: ctx.stream,
        group: "risk",
        poll_ms: 20,
        max_demand: 10
      )

    {:ok, _} = Sink.start_link(producer: producer, owner: self())

    assert_receive {:lease, %Lease{receipt: r1} = a}, 3_000
    assert_receive {:lease, %Lease{receipt: r2} = b}, 3_000
    assert is_binary(r1) and is_binary(r2)
    assert r1 != a.lease_id and r2 != b.lease_id

    assert :ok = Producer.ack(producer, a, rheo: ctx.rheo)
    assert :ok = Producer.ack(producer, b, rheo: ctx.rheo)

    wait_until(fn -> Producer.inflight_count(producer) == 0 end)
    wait_until(fn -> match?({:ok, %{lag: 0}}, Rheo.lag(ctx.stream, "risk", rheo: ctx.rheo)) end)

    assert :ok = GenServer.stop(producer)
  end

  defp start_pipeline!(ctx, verdict, opts \\ []) do
    {producer_opts, opts} = Keyword.split(opts, [:poll_ms, :max_demand, :lease_ms, :on_failure])

    producer =
      [rheo: ctx.rheo, stream: ctx.stream, group: "risk", max_demand: 10, poll_ms: 30]
      |> Keyword.merge(producer_opts)

    name = :"redis_bw_pipe_#{System.unique_integer([:positive])}"

    start_supervised!(
      {Pipeline, [name: name, owner: self(), verdict: verdict, producer: producer] ++ opts},
      id: name
    )
  end

  defp wait_until(fun, attempts \\ 150) do
    cond do
      fun.() -> true
      attempts <= 0 -> flunk("condition not met")
      true -> Process.sleep(20) && wait_until(fun, attempts - 1)
    end
  end
end
