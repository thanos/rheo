defmodule Rheo.FlowReadinessTest do
  @moduledoc """
  Executable Flow readiness checks for v0.8 (ADR 019 / design spike).

  Proves `%Rheo.Lease{}` + `Producer.confirm/2` compose with Flow map /
  partition / reduce / window without inventing a Flow-specific lease type.
  Flow is a **test-only** dependency — not a published Rheo package.
  """
  use ExUnit.Case, async: false

  alias Rheo.{Lease, Producer}

  setup do
    rheo = :"flow_rheo_#{System.unique_integer([:positive])}"
    start_supervised!({Rheo, name: rheo, backend: Rheo.Backend.ETS}, id: rheo)

    stream = "flow-#{System.unique_integer([:positive])}"
    :ok = Rheo.create_stream(stream, rheo: rheo, partition_count: 2)
    :ok = Rheo.create_group(stream, "flow", rheo: rheo)

    %{rheo: rheo, stream: stream}
  end

  test "map settles 1:1 leases and advances the frontier", ctx do
    append(ctx, [%{type: "a", key: "k1"}, %{type: "b", key: "k2"}, %{type: "c", key: "k3"}])
    producer = start_producer!(ctx)

    ids =
      Flow.from_stages([producer])
      |> Flow.map(fn %Lease{} = lease ->
        assert is_binary(lease.lease_id)
        assert lease.receipt == lease.lease_id or is_binary(lease.receipt)
        :ok = Rheo.ack(lease, rheo: ctx.rheo)
        :ok = Producer.confirm(producer, lease.lease_id)
        lease.event.id
      end)
      |> Enum.take(3)

    assert length(ids) == 3
    assert length(Enum.uniq(ids)) == 3
    wait_until(fn -> frontier(ctx) == 3 end)
    assert Producer.inflight_count(producer) == 0
  end

  test "partition + reduce settles only on_trigger (not mid-reduce)", ctx do
    append(ctx, [
      %{type: "tick", key: "alpha"},
      %{type: "tick", key: "beta"},
      %{type: "tick", key: "alpha"}
    ])

    producer = start_producer!(ctx)
    parent = self()

    window =
      Flow.Window.global()
      |> Flow.Window.trigger_every(3)

    [{_partition, leases}] =
      Flow.from_stages([producer])
      |> Flow.partition(
        key: fn %Lease{event: event} -> event.payload["key"] || event.payload[:key] end,
        window: window,
        stages: 1
      )
      |> Flow.reduce(fn -> [] end, fn %Lease{} = lease, acc ->
        send(parent, {:reduced, lease.lease_id})
        [lease | acc]
      end)
      |> Flow.on_trigger(fn leases ->
        Enum.each(leases, fn lease ->
          :ok = Rheo.ack(lease, rheo: ctx.rheo)
          :ok = Producer.confirm(producer, lease.lease_id)
        end)

        send(parent, {:triggered, length(leases)})
        {[{0, leases}], []}
      end)
      |> Enum.take(1)

    assert length(leases) == 3
    assert_receive {:triggered, 3}, 2_000

    # Reduce saw every lease before any settlement trigger completed the batch.
    for _ <- 1..3, do: assert_receive({:reduced, _}, 100)

    wait_until(fn -> frontier(ctx) == 3 end)
    assert Producer.inflight_count(producer) == 0
  end

  test "window trigger retains leases until the window emits", ctx do
    append(ctx, for(i <- 1..4, do: %{type: "w", n: i, key: "same"}))
    producer = start_producer!(ctx)

    window =
      Flow.Window.global()
      |> Flow.Window.trigger_every(2)

    batches =
      Flow.from_stages([producer])
      |> Flow.partition(key: fn _ -> :one end, window: window, stages: 1)
      |> Flow.reduce(fn -> [] end, fn lease, acc -> [lease | acc] end)
      |> Flow.on_trigger(fn leases ->
        Enum.each(leases, fn lease ->
          :ok = Rheo.ack(lease, rheo: ctx.rheo)
          :ok = Producer.confirm(producer, lease.lease_id)
        end)

        {[length(leases)], []}
      end)
      |> Enum.take(2)

    assert batches == [2, 2]
    wait_until(fn -> frontier(ctx) == 4 end)
  end

  test "crash before settlement redelivers the lease (at-least-once)", ctx do
    append(ctx, [%{type: "fragile"}])

    producer =
      start_producer!(ctx, lease_ms: 200, poll_ms: 30, restart: :temporary)

    [%Lease{lease_id: lease_id, event: %{id: event_id}, attempt: 1}] =
      Flow.from_stages([producer])
      |> Flow.map(fn %Lease{} = lease -> lease end)
      |> Enum.take(1)

    # Drop the producer without ACK/confirm — lease must expire and redeliver.
    Process.exit(producer, :kill)
    wait_until(fn -> not Process.alive?(producer) end)

    producer2 =
      start_producer!(ctx,
        lease_ms: 5_000,
        poll_ms: 30,
        consumer_id: "flow-2",
        restart: :temporary
      )

    [%Lease{event: %{id: ^event_id}, attempt: attempt, lease_id: new_id}] =
      Flow.from_stages([producer2])
      |> Flow.map(fn %Lease{} = lease ->
        :ok = Rheo.ack(lease, rheo: ctx.rheo)
        :ok = Producer.confirm(producer2, lease.lease_id)
        lease
      end)
      |> Enum.take(1)

    assert attempt >= 2
    assert new_id != lease_id
    wait_until(fn -> frontier(ctx) == 1 end)
  end

  defp append(ctx, payloads) do
    {:ok, _} = Rheo.append_batch(ctx.stream, payloads, rheo: ctx.rheo)
  end

  defp start_producer!(ctx, opts \\ []) do
    {restart, opts} = Keyword.pop(opts, :restart, :temporary)

    opts =
      Keyword.merge(
        [
          rheo: ctx.rheo,
          stream: ctx.stream,
          group: "flow",
          max_demand: 10,
          poll_ms: 20,
          lease_ms: 30_000
        ],
        opts
      )

    id = {:flow_producer, System.unique_integer()}

    start_supervised!(Supervisor.child_spec({Producer, opts}, id: id, restart: restart))
  end

  defp frontier(ctx) do
    case Rheo.lag(ctx.stream, "flow", rheo: ctx.rheo) do
      {:ok, %{partitions: partitions}} ->
        partitions
        |> Map.values()
        |> Enum.map(& &1.frontier)
        |> Enum.sum()

      _ ->
        0
    end
  end

  defp wait_until(fun, attempts \\ 80) do
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
