defmodule Rheo.ProducerTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Rheo.{Lease, Producer}

  defmodule Sink do
    @moduledoc false
    use GenStage

    def start_link(opts), do: GenStage.start_link(__MODULE__, opts)

    @impl true
    def init(opts) do
      subscription = [
        {Keyword.fetch!(opts, :producer), max_demand: Keyword.get(opts, :max_demand, 10)}
      ]

      {:consumer, Keyword.fetch!(opts, :owner), subscribe_to: subscription}
    end

    @impl true
    def handle_events(leases, _from, owner) do
      Enum.each(leases, &send(owner, {:lease, &1}))
      {:noreply, [], owner}
    end
  end

  setup do
    rheo = :"producer_rheo_#{System.unique_integer([:positive])}"
    start_supervised!({Rheo, name: rheo, backend: Rheo.Backend.ETS}, id: rheo)

    stream = "producer-#{System.unique_integer([:positive])}"
    :ok = Rheo.create_stream(stream, rheo: rheo, partition_count: 2)
    :ok = Rheo.create_group(stream, "risk", rheo: rheo)

    %{rheo: rheo, stream: stream}
  end

  test "emits leases in response to demand", ctx do
    append(ctx, [%{type: "a"}, %{type: "b"}, %{type: "c"}])
    producer = start_producer!(ctx, max_demand: 10)
    start_sink!(producer)

    leases = collect_leases(3)

    assert Enum.all?(leases, &match?(%Lease{group: "risk", attempt: 1}, &1))
    assert Enum.map(leases, & &1.event.type) |> Enum.sort() == ["a", "b", "c"]
    assert Producer.inflight_count(producer) == 3
  end

  test "max_demand bounds unsettled leases until confirmed", ctx do
    append(ctx, for(i <- 1..5, do: %{type: "e", i: i}))
    producer = start_producer!(ctx, max_demand: 2)
    start_sink!(producer)

    [first | _] = collect_leases(2)
    refute_receive {:lease, _}, 200
    assert Producer.inflight_count(producer) == 2

    :ok = Rheo.ack(first, rheo: ctx.rheo)
    :ok = Producer.confirm(producer, first.lease_id)

    assert_receive {:lease, %Lease{}}, 2_000
  end

  test "polls an empty group and emits once events arrive", ctx do
    producer = start_producer!(ctx, poll_ms: 20)
    start_sink!(producer)

    refute_receive {:lease, _}, 100
    append(ctx, [%{type: "late"}])

    assert_receive {:lease, %Lease{event: %{type: "late"}}}, 2_000
  end

  test "renews inflight leases on a timer", ctx do
    append(ctx, [%{type: "long_running"}])
    attach_telemetry([:rheo, :lease, :renew], ctx.stream, :renew)

    producer = start_producer!(ctx, lease_ms: 200)
    start_sink!(producer)

    collect_leases(1)

    assert_receive {:renew, :ok}, 2_000
    assert Producer.inflight_count(producer) == 1
  end

  test "renew drops a lease settled behind the producer's back", ctx do
    append(ctx, [%{type: "settled_elsewhere"}])
    producer = start_producer!(ctx, lease_ms: 60_000, max_demand: 1)
    start_sink!(producer)

    [lease] = collect_leases(1)
    :ok = Rheo.ack(lease, rheo: ctx.rheo)

    log =
      capture_log(fn ->
        send(producer, :renew)
        wait_until(fn -> Producer.inflight_count(producer) == 0 end)
      end)

    assert log =~ "dropping stale lease"
  end

  test "fetch errors back off and emit telemetry", ctx do
    attach_telemetry([:rheo, :fetch, :error], ctx.stream, :fetch_error)

    capture_log(fn ->
      producer = start_producer!(ctx, group: "no-such-group", poll_ms: 20)
      start_sink!(producer)

      assert_receive {:fetch_error, :group_not_found}, 2_000
    end)
  end

  test "restricts fetch to the assigned partitions", ctx do
    {:ok, _} = Rheo.append(ctx.stream, %{type: "p0"}, rheo: ctx.rheo, partition: 0)
    {:ok, _} = Rheo.append(ctx.stream, %{type: "p1"}, rheo: ctx.rheo, partition: 1)

    producer = start_producer!(ctx, partitions: [1], poll_ms: 20)
    start_sink!(producer)

    assert_receive {:lease, lease}, 2_000
    assert lease.event.partition == 1
    refute_receive {:lease, _}, 200
  end

  test "drain stops fetching", ctx do
    producer = start_producer!(ctx, poll_ms: 20)
    start_sink!(producer)

    assert :ok = Producer.drain(producer, 500)

    append(ctx, [%{type: "after_drain"}])
    refute_receive {:lease, _}, 300
  end

  test "drain times out while a lease is unsettled", ctx do
    append(ctx, [%{type: "unsettled"}])
    producer = start_producer!(ctx, max_demand: 1)
    start_sink!(producer)

    collect_leases(1)

    assert :timeout = Producer.drain(producer, 100)
  end

  test "drain completes when the lease is confirmed while waiting", ctx do
    append(ctx, [%{type: "confirm_during_drain"}])
    producer = start_producer!(ctx, max_demand: 1)
    start_sink!(producer)

    [lease] = collect_leases(1)
    drain = Task.async(fn -> Producer.drain(producer, 2_000) end)

    Process.sleep(50)
    :ok = Rheo.ack(lease, rheo: ctx.rheo)
    :ok = Producer.confirm(producer, lease.lease_id)

    assert :ok = Task.await(drain, 3_000)
  end

  test "config/0 exposes the producer's settle configuration", ctx do
    assert Producer.config() == nil

    producer = start_producer!(ctx, on_failure: :reject)

    assert %{rheo: rheo, stream: stream, group: "risk", on_failure: :reject} =
             producer_config(producer)

    assert rheo == ctx.rheo
    assert stream == ctx.stream
  end

  test "init rejects an unknown :on_failure" do
    assert_raise ArgumentError, ~r/:on_failure must be :nack or :reject/, fn ->
      Producer.init(stream: "s", group: "g", on_failure: :explode)
    end
  end

  test "init ignores the :broadway option Broadway injects", ctx do
    assert {:producer, state} =
             Producer.init(
               rheo: ctx.rheo,
               stream: ctx.stream,
               group: "risk",
               broadway: [name: FakePipeline, index: 0]
             )

    assert state.stream == ctx.stream
    assert state.group == "risk"
  end

  test "named start_link and unknown messages are tolerated", ctx do
    name = :"named_producer_#{System.unique_integer([:positive])}"

    producer =
      start_producer!(ctx, name: name, poll_ms: 5_000)

    assert GenServer.whereis(name) == producer

    send(producer, :unexpected)
    GenStage.cast(producer, :unexpected)

    assert Producer.inflight_count(producer) == 0
  end

  defp start_producer!(ctx, opts) do
    opts =
      [rheo: ctx.rheo, stream: ctx.stream, group: "risk"]
      |> Keyword.merge(opts)

    start_supervised!({Producer, opts}, id: {Producer, System.unique_integer([:positive])})
  end

  defp start_sink!(producer, opts \\ []) do
    opts = Keyword.merge([producer: producer, owner: self()], opts)
    start_supervised!({Sink, opts}, id: {Sink, System.unique_integer([:positive])})
  end

  defp producer_config(producer) do
    # `Rheo.Producer.config/0` reads the producer's process dictionary, which is
    # exactly what Broadway's transformer does from inside that process.
    {:dictionary, dictionary} = Process.info(producer, :dictionary)
    {_key, config} = List.keyfind(dictionary, {Producer, :config}, 0)
    config
  end

  defp append(ctx, payloads) do
    {:ok, _} = Rheo.append_batch(ctx.stream, payloads, rheo: ctx.rheo, partition: 0)
    :ok
  end

  defp collect_leases(count) do
    Enum.map(1..count, fn _ ->
      assert_receive {:lease, %Lease{} = lease}, 2_000
      lease
    end)
  end

  defp attach_telemetry(event, stream, tag) do
    parent = self()
    handler_id = "#{tag}-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(
        handler_id,
        event,
        fn _event, _measurements, metadata, _config ->
          if metadata[:stream] == stream do
            send(parent, {tag, metadata[:result] || metadata[:reason]})
          end
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  defp wait_until(fun, attempts \\ 100) do
    cond do
      fun.() -> true
      attempts <= 0 -> flunk("condition not met")
      true -> Process.sleep(20) && wait_until(fun, attempts - 1)
    end
  end
end
