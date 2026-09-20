defmodule Rheo.FailureInjectionTest do
  # Settlement failures, backend and instance restarts, and the at-least-once
  # promise under each, on ETS so they run everywhere.
  use ExUnit.Case, async: false

  alias Rheo.Backend.Flaky
  alias Rheo.Clock.Frozen

  defmodule GateConsumer do
    use Rheo.Consumer, stream: "unused", group: "unused"

    @impl true
    def setup(opts), do: {:ok, %{owner: Keyword.fetch!(opts, :owner)}}

    @impl true
    def handle_event(event, %{owner: owner}) do
      send(owner, {:handling, event.id, self()})

      receive do
        {:outcome, outcome} -> outcome
      end
    end
  end

  setup do
    Flaky.clear()
    Frozen.set(DateTime.utc_now())
    on_exit(fn -> Frozen.reset() end)

    rheo = :"flaky_#{System.unique_integer([:positive])}"
    start_supervised!({Rheo, name: rheo, backend: Flaky}, id: rheo)
    stream = "s-#{System.unique_integer([:positive])}"
    :ok = Rheo.create_stream(stream, rheo: rheo)
    :ok = Rheo.create_group(stream, "g", rheo: rheo, max_attempts: 5)
    %{rheo: rheo, stream: stream}
  end

  defp attach(event, tag) do
    parent = self()
    id = "#{tag}-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(id, event, fn _e, _m, meta, _ -> send(parent, {tag, meta}) end, nil)

    on_exit(fn -> :telemetry.detach(id) end)
  end

  defp start_consumer!(ctx, opts) do
    opts =
      Keyword.merge(
        [
          rheo: ctx.rheo,
          stream: ctx.stream,
          group: "g",
          owner: self(),
          poll_ms: 20,
          max_demand: 1
        ],
        opts
      )

    start_supervised!({GateConsumer, opts}, id: {GateConsumer, System.unique_integer()})
  end

  describe "handler succeeded, settle failed" do
    test "unavailable backend on ACK: no nack, lease expires, redelivered", ctx do
      attach([:rheo, :ack, :error], :ack_error)
      attach([:rheo, :retry, :error], :retry_error)
      {:ok, event} = Rheo.append(ctx.stream, %{type: "e"}, rheo: ctx.rheo)
      group = start_consumer!(ctx, lease_ms: 1_000)

      assert_receive {:handling, id, worker}, 2_000
      assert id == event.id
      Flaky.fail(:ack, :backend_unavailable)
      send(worker, {:outcome, :ack})

      assert_receive {:ack_error, %{event_id: ^id, reason: :backend_unavailable}}, 2_000
      refute_receive {:retry_error, _}, 100

      # Stop the group from re-fetching so the redelivery is observable here.
      assert :ok = Rheo.Group.drain(group, 1_000)

      # Still leased: nothing to fetch until the lease expires.
      Flaky.clear()
      assert {:ok, []} = Rheo.fetch(ctx.stream, "g", limit: 1, consumer_id: "x", rheo: ctx.rheo)
      Frozen.advance(1_100)

      assert {:ok, [again]} =
               Rheo.fetch(ctx.stream, "g", limit: 1, consumer_id: "x", rheo: ctx.rheo)

      assert again.event_id == event.id
      assert again.attempt == 2
    end

    test "definite failure on ACK: lease is nacked for immediate redelivery", ctx do
      attach([:rheo, :ack, :error], :ack_error)
      {:ok, event} = Rheo.append(ctx.stream, %{type: "e"}, rheo: ctx.rheo)
      start_consumer!(ctx, lease_ms: 60_000)

      assert_receive {:handling, _id, worker}, 2_000
      Flaky.fail(:ack, {:failed, :write_conflict})
      send(worker, {:outcome, :ack})

      assert_receive {:ack_error, %{reason: {:failed, :write_conflict}}}, 2_000

      # The group nacked, and the same group redelivers without waiting for expiry.
      assert_receive {:handling, id, worker2}, 2_000
      assert id == event.id
      Flaky.clear()
      send(worker2, {:outcome, :ack})
      assert {:ok, %{lag: 0}} = wait_lag(ctx, 0)
    end

    test "retry persistence failure is reported and the lease expires", ctx do
      attach([:rheo, :retry, :error], :retry_error)
      {:ok, event} = Rheo.append(ctx.stream, %{type: "e"}, rheo: ctx.rheo)
      start_consumer!(ctx, lease_ms: 1_000)

      assert_receive {:handling, _id, worker}, 2_000
      Flaky.fail(:retry, :backend_unavailable)
      send(worker, {:outcome, {:retry, :later}})

      assert_receive {:retry_error, %{reason: :backend_unavailable}}, 2_000
      Flaky.clear()
      Frozen.advance(1_100)
      assert_receive {:handling, id, worker2}, 2_000
      assert id == event.id
      send(worker2, {:outcome, :ack})
    end

    test "reject persistence failure is reported", ctx do
      attach([:rheo, :reject, :error], :reject_error)
      {:ok, _} = Rheo.append(ctx.stream, %{type: "e"}, rheo: ctx.rheo)
      start_consumer!(ctx, lease_ms: 60_000)

      assert_receive {:handling, _id, worker}, 2_000
      Flaky.fail(:reject, {:failed, :disk_full})
      send(worker, {:outcome, {:reject, :poison}})

      assert_receive {:reject_error, %{reason: {:failed, :disk_full}}}, 2_000
    end

    test "renew failure keeps the lease while it is not lost", ctx do
      attach([:rheo, :lease, :renew], :renew)
      {:ok, _} = Rheo.append(ctx.stream, %{type: "e"}, rheo: ctx.rheo)
      pid = start_consumer!(ctx, lease_ms: 200)

      assert_receive {:handling, _id, worker}, 2_000
      Flaky.fail(:renew, :backend_unavailable)
      send(pid, :renew)
      assert_receive {:renew, %{result: :backend_unavailable}}, 2_000

      Flaky.clear()
      send(pid, :renew)
      assert_receive {:renew, %{result: :ok}}, 2_000
      send(worker, {:outcome, :ack})
    end
  end

  describe "process failures" do
    test "backend process restart: instance stays usable", ctx do
      %Rheo.Instance{handle: handle} = Rheo.Instance.fetch!(ctx.rheo)
      backend = Process.whereis(handle)
      ref = Process.monitor(backend)
      Process.exit(backend, :kill)
      assert_receive {:DOWN, ^ref, :process, ^backend, :killed}, 1_000

      assert :ok = wait_until(fn -> Rheo.ping(rheo: ctx.rheo) == :ok end)
      # ETS is declared not durable: the stream is gone, the API is not.
      assert {:error, :stream_not_found} = Rheo.append(ctx.stream, %{type: "e"}, rheo: ctx.rheo)
      assert :ok = Rheo.create_stream(ctx.stream, rheo: ctx.rheo)
    end

    test "backend crash with a live group: fetch backs off and recovers" do
      attach([:rheo, :fetch, :error], :fetch_error)
      rheo = :"restart_#{System.unique_integer([:positive])}"
      start_supervised!({Rheo, name: rheo, backend: Rheo.Backend.ETS}, id: rheo)
      :ok = Rheo.create_stream("s", rheo: rheo)
      :ok = Rheo.create_group("s", "g", rheo: rheo)

      group =
        start_supervised!(
          {GateConsumer, rheo: rheo, stream: "s", group: "g", owner: self(), poll_ms: 20},
          id: :restart_consumer
        )

      %Rheo.Instance{handle: handle} = Rheo.Instance.fetch!(rheo)
      backend = Process.whereis(handle)
      Process.exit(backend, :kill)

      # Depending on how fast the supervisor restarts the backend, fetch sees
      # either the dead process or the empty store that replaced it.
      assert_receive {:fetch_error, %{reason: reason}}, 2_000
      assert reason in [:backend_unavailable, :group_not_found, :stream_not_found]
      assert Process.alive?(group)
      assert :ok = wait_until(fn -> Rheo.ping(rheo: rheo) == :ok end)

      # ETS is not durable: recreate the stream, then the same group consumes again.
      :ok = Rheo.create_stream("s", rheo: rheo)
      :ok = Rheo.create_group("s", "g", rheo: rheo)
      {:ok, event} = Rheo.append("s", %{type: "e"}, rheo: rheo)
      assert_receive {:handling, id, worker}, 5_000
      assert id == event.id
      send(worker, {:outcome, :ack})
    end
  end

  defp wait_lag(ctx, expected) do
    wait_until(fn ->
      match?({:ok, %{lag: ^expected}}, Rheo.lag(ctx.stream, "g", rheo: ctx.rheo))
    end)

    Rheo.lag(ctx.stream, "g", rheo: ctx.rheo)
  end

  defp wait_until(fun, attempts \\ 100) do
    cond do
      fun.() -> :ok
      attempts <= 0 -> flunk("condition not met")
      true -> Process.sleep(20) && wait_until(fun, attempts - 1)
    end
  end
end
