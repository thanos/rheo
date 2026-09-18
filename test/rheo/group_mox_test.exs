defmodule Rheo.GroupMoxTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import Mox

  alias Rheo.Backend.Mock
  alias Rheo.Test.MoxBackend

  defmodule AckConsumer do
    use Rheo.Consumer, stream: "unused", group: "unused"

    @impl true
    def setup(opts), do: {:ok, %{agent: Keyword.fetch!(opts, :agent)}}

    @impl true
    def handle_event(event, %{agent: agent}) do
      Agent.update(agent, fn xs -> [{:ack, event.id} | xs] end)
      :ack
    end
  end

  defmodule RetryConsumer do
    use Rheo.Consumer, stream: "unused", group: "unused"

    @impl true
    def handle_event(_event, _ctx), do: {:retry, :later}
  end

  defmodule RejectConsumer do
    use Rheo.Consumer, stream: "unused", group: "unused"

    @impl true
    def handle_event(_event, _ctx), do: {:reject, :bad}
  end

  defmodule RaiseConsumer do
    use Rheo.Consumer, stream: "unused", group: "unused"

    @impl true
    def handle_event(_event, _ctx), do: raise("boom")
  end

  defmodule InvalidConsumer do
    use Rheo.Consumer, stream: "unused", group: "unused"

    @impl true
    def handle_event(_event, _ctx), do: :not_valid
  end

  defmodule SlowAckConsumer do
    use Rheo.Consumer, stream: "unused", group: "unused"

    @impl true
    def handle_event(_event, _ctx) do
      Process.sleep(400)
      :ack
    end
  end

  defmodule HangAckConsumer do
    use Rheo.Consumer, stream: "unused", group: "unused"

    @impl true
    def handle_event(_event, _ctx) do
      Process.sleep(5_000)
      :ack
    end
  end

  setup :set_mox_global
  setup :verify_on_exit!

  setup do
    opts = MoxBackend.start_opts!("group_mox")
    start_supervised!(opts.child, id: opts.rheo)
    {:ok, agent} = Agent.start_link(fn -> [] end)
    Map.merge(opts, %{agent: agent})
  end

  test "fetches a lease, acks it, and records the handler call", ctx do
    lease = MoxBackend.sample_lease(stream: "s", group: "g")

    expect(Mock, :fetch, fn handle, "s", "g", opts ->
      assert handle == ctx.handle
      assert opts[:limit] >= 1
      {:ok, [lease]}
    end)

    expect(Mock, :ack, fn handle, ^lease ->
      assert handle == ctx.handle
      :ok
    end)

    stub(Mock, :fetch, fn _h, "s", "g", _opts -> {:ok, []} end)

    assert {:ok, pid} =
             Rheo.Group.start_link(
               rheo: ctx.rheo,
               stream: "s",
               group: "g",
               module: AckConsumer,
               agent: ctx.agent,
               poll_ms: 50,
               max_demand: 1,
               concurrency: 1
             )

    wait_until(fn -> Agent.get(ctx.agent, & &1) != [] end)
    assert [{:ack, id}] = Agent.get(ctx.agent, & &1)
    assert id == lease.event_id

    assert :ok = Rheo.Group.drain(pid, 500)
    assert :ok = GenServer.stop(pid)
  end

  test "fetch errors emit telemetry and back off", ctx do
    parent = self()

    :telemetry.attach(
      "group-mox-fetch-#{System.unique_integer([:positive])}",
      [:rheo, :fetch, :error],
      fn _e, _m, meta, _ -> send(parent, {:fetch_error, meta.reason}) end,
      nil
    )

    expect(Mock, :fetch, fn _h, "s", "g", _opts -> {:error, :group_not_found} end)
    stub(Mock, :fetch, fn _h, "s", "g", _opts -> {:ok, []} end)

    log =
      capture_log(fn ->
        assert {:ok, pid} =
                 Rheo.Group.start_link(
                   rheo: ctx.rheo,
                   stream: "s",
                   group: "g",
                   module: AckConsumer,
                   agent: ctx.agent,
                   poll_ms: 20
                 )

        assert_receive {:fetch_error, :group_not_found}, 2_000
        assert :ok = GenServer.stop(pid)
      end)

    assert log =~ "fetch failed"
  end

  test "retry and reject settle through the mock backend", ctx do
    retry_lease = MoxBackend.sample_lease(stream: "s", group: "retry", id: "e_retry")
    reject_lease = MoxBackend.sample_lease(stream: "s", group: "reject", id: "e_reject")

    expect(Mock, :fetch, fn _h, "s", "retry", _opts -> {:ok, [retry_lease]} end)
    expect(Mock, :retry, fn _h, ^retry_lease, :later -> :ok end)
    stub(Mock, :fetch, fn _h, "s", "retry", _opts -> {:ok, []} end)

    assert {:ok, retry_pid} =
             Rheo.Group.start_link(
               rheo: ctx.rheo,
               stream: "s",
               group: "retry",
               module: RetryConsumer,
               poll_ms: 50
             )

    Process.sleep(150)
    assert :ok = GenServer.stop(retry_pid)

    expect(Mock, :fetch, fn _h, "s", "reject", _opts -> {:ok, [reject_lease]} end)
    expect(Mock, :reject, fn _h, ^reject_lease, :bad -> :ok end)
    stub(Mock, :fetch, fn _h, "s", "reject", _opts -> {:ok, []} end)

    assert {:ok, reject_pid} =
             Rheo.Group.start_link(
               rheo: ctx.rheo,
               stream: "s",
               group: "reject",
               module: RejectConsumer,
               poll_ms: 50
             )

    Process.sleep(150)
    assert :ok = GenServer.stop(reject_pid)
  end

  test "handler raise and invalid outcome nack the lease", ctx do
    raised = MoxBackend.sample_lease(stream: "s", group: "raise", id: "e_raise")
    invalid = MoxBackend.sample_lease(stream: "s", group: "invalid", id: "e_invalid")

    expect(Mock, :fetch, fn _h, "s", "raise", _opts -> {:ok, [raised]} end)
    expect(Mock, :retry, fn _h, ^raised, {:handler_error, _} -> :ok end)
    stub(Mock, :fetch, fn _h, "s", "raise", _opts -> {:ok, []} end)

    capture_log(fn ->
      assert {:ok, pid} =
               Rheo.Group.start_link(
                 rheo: ctx.rheo,
                 stream: "s",
                 group: "raise",
                 module: RaiseConsumer,
                 poll_ms: 50
               )

      Process.sleep(150)
      assert :ok = GenServer.stop(pid)
    end)

    expect(Mock, :fetch, fn _h, "s", "invalid", _opts -> {:ok, [invalid]} end)
    expect(Mock, :retry, fn _h, ^invalid, {:invalid_outcome, :not_valid} -> :ok end)
    stub(Mock, :fetch, fn _h, "s", "invalid", _opts -> {:ok, []} end)

    capture_log(fn ->
      assert {:ok, pid} =
               Rheo.Group.start_link(
                 rheo: ctx.rheo,
                 stream: "s",
                 group: "invalid",
                 module: InvalidConsumer,
                 poll_ms: 50
               )

      Process.sleep(150)
      assert :ok = GenServer.stop(pid)
    end)
  end

  test "failed ack may nack when Settle says so", ctx do
    lease = MoxBackend.sample_lease(stream: "s", group: "ackfail")

    expect(Mock, :fetch, fn _h, "s", "ackfail", _opts -> {:ok, [lease]} end)
    expect(Mock, :ack, fn _h, ^lease -> {:error, {:failed, :write}} end)
    stub(Mock, :retry, fn _h, ^lease, {:ack_failed, _} -> :ok end)
    stub(Mock, :fetch, fn _h, "s", "ackfail", _opts -> {:ok, []} end)

    capture_log(fn ->
      assert {:ok, pid} =
               Rheo.Group.start_link(
                 rheo: ctx.rheo,
                 stream: "s",
                 group: "ackfail",
                 module: AckConsumer,
                 agent: ctx.agent,
                 poll_ms: 50
               )

      Process.sleep(150)
      assert :ok = GenServer.stop(pid)
    end)
  end

  test "renew failure is logged while work is still inflight", ctx do
    lease = MoxBackend.sample_lease(stream: "s", group: "renew")

    expect(Mock, :fetch, fn _h, "s", "renew", _opts -> {:ok, [lease]} end)
    stub(Mock, :renew, fn _h, ^lease, _opts -> {:error, :stale_lease} end)
    stub(Mock, :ack, fn _h, ^lease -> :ok end)
    stub(Mock, :fetch, fn _h, "s", "renew", _opts -> {:ok, []} end)

    capture_log(fn ->
      assert {:ok, pid} =
               Rheo.Group.start_link(
                 rheo: ctx.rheo,
                 stream: "s",
                 group: "renew",
                 module: SlowAckConsumer,
                 poll_ms: 50,
                 lease_ms: 80
               )

      Process.sleep(250)
      assert :ok = GenServer.stop(pid)
    end)
  end

  test "drain times out while work is inflight", ctx do
    lease = MoxBackend.sample_lease(stream: "s", group: "drain")

    expect(Mock, :fetch, fn _h, "s", "drain", _opts -> {:ok, [lease]} end)
    stub(Mock, :ack, fn _h, ^lease -> Process.sleep(5_000) && :ok end)
    stub(Mock, :fetch, fn _h, "s", "drain", _opts -> {:ok, []} end)
    stub(Mock, :renew, fn _h, ^lease, _opts -> {:ok, lease} end)

    assert {:ok, pid} =
             Rheo.Group.start_link(
               rheo: ctx.rheo,
               stream: "s",
               group: "drain",
               module: HangAckConsumer,
               poll_ms: 50,
               lease_ms: 60_000
             )

    Process.sleep(50)
    assert :timeout = Rheo.Group.drain(pid, 50)
    Process.exit(pid, :kill)
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
