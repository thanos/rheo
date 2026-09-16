defmodule Rheo.BroadwayTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Rheo.Broadway.Acknowledger

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
          transformer: {Rheo.Broadway, :transform, Keyword.get(opts, :transformer, [])},
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

  setup do
    rheo = :"broadway_rheo_#{System.unique_integer([:positive])}"
    start_supervised!({Rheo, name: rheo, backend: Rheo.Backend.ETS}, id: rheo)

    stream = "broadway-#{System.unique_integer([:positive])}"
    :ok = Rheo.create_stream(stream, rheo: rheo)
    :ok = Rheo.create_group(stream, "risk", rheo: rheo, max_attempts: 5)

    %{rheo: rheo, stream: stream}
  end

  test "successful messages ACK and advance the frontier", ctx do
    append(ctx, [%{type: "a"}, %{type: "b"}, %{type: "c"}])
    start_pipeline!(ctx, fn message -> message end)

    assert_receive {:handled, event, metadata}, 2_000
    assert %Rheo.Event{} = event
    assert metadata.stream == ctx.stream
    assert metadata.group == "risk"
    assert metadata.partition == 0
    assert metadata.attempt == 1
    assert metadata.lease.event_id == event.id

    assert_receive {:handled, _, _}, 2_000
    assert_receive {:handled, _, _}, 2_000

    wait_until(fn -> frontier(ctx) == 3 end)
    assert {:ok, %Rheo.Lag{lag: 0}} = Rheo.lag(ctx.stream, "risk", rheo: ctx.rheo)
  end

  test "failed messages NACK and are redelivered", ctx do
    append(ctx, [%{type: "flaky"}])

    verdict = fn message ->
      if message.metadata.attempt == 1 do
        Broadway.Message.failed(message, :transient)
      else
        message
      end
    end

    start_pipeline!(ctx, verdict, poll_ms: 20)

    assert_receive {:handled, _, %{attempt: 1}}, 2_000
    assert_receive {:handled, _, %{attempt: 2}}, 2_000

    wait_until(fn -> frontier(ctx) == 1 end)
  end

  test "on_failure: :reject dead-letters instead of retrying", ctx do
    append(ctx, [%{type: "poison"}])
    verdict = fn message -> Broadway.Message.failed(message, :bad_schema) end

    start_pipeline!(ctx, verdict, poll_ms: 20, on_failure: :reject)

    assert_receive {:handled, _, %{attempt: 1}}, 2_000
    refute_receive {:handled, _, %{attempt: 2}}, 500

    assert {:ok, []} = Rheo.fetch(ctx.stream, "risk", limit: 1, rheo: ctx.rheo)
    wait_until(fn -> frontier(ctx) == 1 end)
  end

  test "configure_ack overrides :on_failure for one message", ctx do
    append(ctx, [%{type: "one_off"}])

    verdict = fn message ->
      message
      |> Broadway.Message.configure_ack(on_failure: :reject)
      |> Broadway.Message.failed(:unrecoverable)
    end

    start_pipeline!(ctx, verdict, poll_ms: 20)

    assert_receive {:handled, _, %{attempt: 1}}, 2_000
    refute_receive {:handled, _, %{attempt: 2}}, 500
    assert {:ok, []} = Rheo.fetch(ctx.stream, "risk", limit: 1, rheo: ctx.rheo)
  end

  test "transformer options override the producer defaults", ctx do
    append(ctx, [%{type: "explicit"}])
    verdict = fn message -> Broadway.Message.failed(message, :nope) end

    start_pipeline!(ctx, verdict,
      poll_ms: 20,
      transformer: [rheo: ctx.rheo, on_failure: :reject]
    )

    assert_receive {:handled, _, %{attempt: 1}}, 2_000
    refute_receive {:handled, _, %{attempt: 2}}, 500
    assert {:ok, []} = Rheo.fetch(ctx.stream, "risk", limit: 1, rheo: ctx.rheo)
  end

  describe "acknowledger" do
    test "emits ack error telemetry for a lease that is already settled", ctx do
      append(ctx, [%{type: "stale"}])
      {:ok, [lease]} = Rheo.fetch(ctx.stream, "risk", limit: 1, rheo: ctx.rheo)
      :ok = Rheo.ack(lease, rheo: ctx.rheo)

      message = Rheo.Broadway.transform(lease, rheo: ctx.rheo)
      parent = self()
      handler_id = "ack-error-#{System.unique_integer([:positive])}"

      :ok =
        :telemetry.attach(
          handler_id,
          [:rheo, :ack, :error],
          fn _event, _measurements, metadata, _config ->
            send(parent, {:ack_error, metadata[:reason]})
          end,
          nil
        )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      log = capture_log(fn -> assert :ok = Acknowledger.ack(self(), [message], []) end)

      assert log =~ "ack failed"
      assert_receive {:ack_error, :stale_lease}
      assert_receive {:"$gen_cast", {:confirm, [lease_id]}}
      assert lease_id == lease.lease_id
    end

    test "acking nothing does not message the producer" do
      assert :ok = Acknowledger.ack(self(), [], [])
      refute_receive {:"$gen_cast", {:confirm, _}}, 100
    end

    test "configure/3 updates ack data and rejects unknown options", ctx do
      ack_data = %{lease: :placeholder, rheo: ctx.rheo, on_failure: :nack}

      assert {:ok, %{on_failure: :reject, rheo: Other}} =
               Acknowledger.configure(self(), ack_data, on_failure: :reject, rheo: Other)

      assert_raise ArgumentError, ~r/unsupported/, fn ->
        Acknowledger.configure(self(), ack_data, nonsense: true)
      end
    end
  end

  defp start_pipeline!(ctx, verdict, opts \\ []) do
    {producer_opts, opts} = Keyword.split(opts, [:poll_ms, :max_demand, :lease_ms, :on_failure])

    producer =
      [rheo: ctx.rheo, stream: ctx.stream, group: "risk", max_demand: 10]
      |> Keyword.merge(producer_opts)

    name = :"broadway_pipeline_#{System.unique_integer([:positive])}"

    start_supervised!(
      {Pipeline, [name: name, owner: self(), verdict: verdict, producer: producer] ++ opts},
      id: name
    )
  end

  defp append(ctx, payloads) do
    {:ok, _} = Rheo.append_batch(ctx.stream, payloads, rheo: ctx.rheo)
    :ok
  end

  defp frontier(ctx) do
    {:ok, lag} = Rheo.lag(ctx.stream, "risk", rheo: ctx.rheo)
    lag.partitions[0].frontier
  end

  defp wait_until(fun, attempts \\ 100) do
    cond do
      fun.() -> true
      attempts <= 0 -> flunk("condition not met")
      true -> Process.sleep(20) && wait_until(fun, attempts - 1)
    end
  end
end
