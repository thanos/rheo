defmodule Rheo.ProducerMoxTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import Mox

  alias Rheo.Backend.Mock
  alias Rheo.{Lease, Producer}
  alias Rheo.Test.MoxBackend

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

  setup :set_mox_global
  setup :verify_on_exit!

  setup do
    opts = MoxBackend.start_opts!("producer_mox")
    start_supervised!(opts.child, id: opts.rheo)
    opts
  end

  test "emits mocked leases and settles via producer helpers", ctx do
    lease = MoxBackend.sample_lease(stream: "s", group: "g")

    expect(Mock, :fetch, fn handle, "s", "g", opts ->
      assert handle == ctx.handle
      assert opts[:limit] >= 1
      {:ok, [lease]}
    end)

    stub(Mock, :fetch, fn _h, "s", "g", _opts -> {:ok, []} end)
    expect(Mock, :ack, fn _h, ^lease -> :ok end)

    producer = start_producer!(ctx)
    start_sink!(producer)

    assert_receive {:lease, %Lease{event_id: id}}, 2_000
    assert id == lease.event_id
    assert Producer.inflight_count(producer) == 1

    assert :ok = Producer.ack(producer, lease, rheo: ctx.rheo)
    wait_until(fn -> Producer.inflight_count(producer) == 0 end)
  end

  test "nack and reject go through the mock backend", ctx do
    a = MoxBackend.sample_lease(stream: "s", group: "g", id: "a")
    b = MoxBackend.sample_lease(stream: "s", group: "g", id: "b")

    expect(Mock, :fetch, fn _h, "s", "g", _opts -> {:ok, [a, b]} end)
    stub(Mock, :fetch, fn _h, "s", "g", _opts -> {:ok, []} end)
    expect(Mock, :retry, fn _h, ^a, :later -> :ok end)
    expect(Mock, :reject, fn _h, ^b, :poison -> :ok end)

    producer = start_producer!(ctx, max_demand: 2)
    start_sink!(producer)

    leases = collect_leases(2)
    [first, second] = Enum.sort_by(leases, & &1.event_id)

    assert :ok = Producer.nack(producer, first, :later, rheo: ctx.rheo)
    assert :ok = Producer.reject(producer, second, :poison, rheo: ctx.rheo)
    wait_until(fn -> Producer.inflight_count(producer) == 0 end)
  end

  test "fetch errors back off with telemetry", ctx do
    parent = self()

    :telemetry.attach(
      "producer-mox-fetch-#{System.unique_integer([:positive])}",
      [:rheo, :fetch, :error],
      fn _e, _m, meta, _ -> send(parent, {:fetch_error, meta.reason}) end,
      nil
    )

    expect(Mock, :fetch, fn _h, "s", "g", _opts -> {:error, :backend_unavailable} end)
    stub(Mock, :fetch, fn _h, "s", "g", _opts -> {:ok, []} end)

    capture_log(fn ->
      producer = start_producer!(ctx, poll_ms: 20)
      start_sink!(producer)
      assert_receive {:fetch_error, :backend_unavailable}, 2_000
      assert :ok = GenServer.stop(producer)
    end)
  end

  test "renew failure drops the inflight lease", ctx do
    lease = MoxBackend.sample_lease(stream: "s", group: "g")

    expect(Mock, :fetch, fn _h, "s", "g", _opts -> {:ok, [lease]} end)
    stub(Mock, :fetch, fn _h, "s", "g", _opts -> {:ok, []} end)
    expect(Mock, :renew, fn _h, ^lease, _opts -> {:error, :stale_lease} end)

    producer = start_producer!(ctx, lease_ms: 80, max_demand: 1)
    start_sink!(producer)

    assert_receive {:lease, %Lease{}}, 2_000

    capture_log(fn ->
      wait_until(fn -> Producer.inflight_count(producer) == 0 end)
    end)
  end

  test "rejects invalid on_failure at start", ctx do
    Process.flag(:trap_exit, true)

    assert {:error, {%ArgumentError{message: message}, _}} =
             Producer.start_link(
               rheo: ctx.rheo,
               stream: "s",
               group: "g",
               on_failure: :explode
             )

    assert message =~ "on_failure"
  end

  test "drain completes when nothing is inflight", ctx do
    stub(Mock, :fetch, fn _h, "s", "g", _opts -> {:ok, []} end)
    producer = start_producer!(ctx)
    assert :ok = Producer.drain(producer, 200)
    assert :ok = GenServer.stop(producer)
  end

  defp start_producer!(ctx, opts \\ []) do
    opts =
      Keyword.merge(
        [rheo: ctx.rheo, stream: "s", group: "g", poll_ms: 30, max_demand: 10],
        opts
      )

    {:ok, pid} = Producer.start_link(opts)
    pid
  end

  defp start_sink!(producer) do
    {:ok, _} = Sink.start_link(producer: producer, owner: self(), max_demand: 10)
  end

  defp collect_leases(n, acc \\ [])

  defp collect_leases(0, acc), do: Enum.reverse(acc)

  defp collect_leases(n, acc) do
    receive do
      {:lease, lease} -> collect_leases(n - 1, [lease | acc])
    after
      2_000 -> flunk("expected #{n} more leases, have #{inspect(acc)}")
    end
  end

  defp wait_until(fun, attempts \\ 50) do
    if fun.() do
      :ok
    else
      if attempts <= 0,
        do: flunk("condition not met"),
        else: Process.sleep(20) && wait_until(fun, attempts - 1)
    end
  end
end
