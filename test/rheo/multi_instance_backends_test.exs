defmodule Rheo.MultiInstanceBackendsTest do
  @moduledoc """
  Multi-instance isolation across different backends in one BEAM node.

  Prompt matrix example: MarketRheo → ETS, AuditRheo → SQLite. Handles and
  stream data must not bleed between instances.
  """
  use ExUnit.Case, async: false

  alias Rheo.Test.SqliteRepo

  setup do
    market = :"market_#{System.unique_integer([:positive])}"
    audit = :"audit_#{System.unique_integer([:positive])}"

    start_supervised!({Rheo, name: market, backend: Rheo.Backend.ETS}, id: market)

    start_supervised!(
      {Rheo, name: audit, backend: {Rheo.Backend.Ecto, repo: SqliteRepo}},
      id: audit
    )

    stream = "shared-name-#{System.unique_integer([:positive])}"

    :ok = Rheo.create_stream(stream, rheo: market, partition_count: 2)
    :ok = Rheo.create_stream(stream, rheo: audit, partition_count: 1)
    :ok = Rheo.create_group(stream, "traders", rheo: market)
    :ok = Rheo.create_group(stream, "compliance", rheo: audit)

    %{market: market, audit: audit, stream: stream}
  end

  test "same stream name on ETS and SQLite does not bleed events", ctx do
    {:ok, market_event} =
      Rheo.append(ctx.stream, %{type: "fill", venue: "ets"}, rheo: ctx.market)

    {:ok, audit_event} =
      Rheo.append(ctx.stream, %{type: "fill", venue: "sqlite"}, rheo: ctx.audit)

    assert market_event.id != audit_event.id

    assert {:ok, only_market} = Rheo.query(ctx.stream, limit: 10, rheo: ctx.market)
    assert Enum.map(only_market, & &1.id) == [market_event.id]
    assert hd(only_market).payload["venue"] == "ets"

    assert {:ok, only_audit} = Rheo.query(ctx.stream, limit: 10, rheo: ctx.audit)
    assert Enum.map(only_audit, & &1.id) == [audit_event.id]
    assert hd(only_audit).payload["venue"] == "sqlite"
  end

  test "consumer groups and frontiers are isolated per instance", ctx do
    {:ok, _} =
      Rheo.append_batch(ctx.stream, [%{type: "m1"}, %{type: "m2"}],
        rheo: ctx.market,
        partition: 0
      )

    {:ok, _} = Rheo.append_batch(ctx.stream, [%{type: "a1"}], rheo: ctx.audit)

    {:ok, [lease]} =
      Rheo.fetch(ctx.stream, "traders", limit: 1, consumer_id: "m", rheo: ctx.market)

    assert :ok = Rheo.ack(lease, rheo: ctx.market)

    # Audit group must not see market's cursor / events.
    assert {:ok, %Rheo.Lag{lag: 1} = market_lag} =
             Rheo.lag(ctx.stream, "traders", rheo: ctx.market)

    assert market_lag.partitions[0].frontier == 1

    assert {:ok, %Rheo.Lag{lag: 1} = audit_lag} =
             Rheo.lag(ctx.stream, "compliance", rheo: ctx.audit)

    assert Enum.all?(audit_lag.partitions, fn {_p, %{frontier: f}} -> f == 0 end)

    assert {:error, _} = Rheo.fetch(ctx.stream, "traders", limit: 1, rheo: ctx.audit)
    assert {:error, _} = Rheo.fetch(ctx.stream, "compliance", limit: 1, rheo: ctx.market)

    {:ok, [audit_lease]} =
      Rheo.fetch(ctx.stream, "compliance", limit: 1, consumer_id: "a", rheo: ctx.audit)

    assert audit_lease.event.type == "a1"

    assert :ok = Rheo.ack(audit_lease, rheo: ctx.audit)

    assert {:ok, %Rheo.Lag{lag: 0}} =
             Rheo.lag(ctx.stream, "compliance", rheo: ctx.audit)
  end

  test "Instance handles resolve to distinct backends", ctx do
    market_inst = Rheo.Instance.fetch!(ctx.market)
    audit_inst = Rheo.Instance.fetch!(ctx.audit)

    assert market_inst.backend == Rheo.Backend.ETS
    assert audit_inst.backend == Rheo.Backend.Ecto
    assert market_inst.handle != audit_inst.handle
  end
end
