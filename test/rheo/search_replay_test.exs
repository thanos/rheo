defmodule Rheo.SearchReplayTest do
  use ExUnit.Case, async: false

  alias Rheo.Clock.Frozen
  alias Rheo.Event.Lineage

  setup do
    name = :"sr_#{System.unique_integer([:positive])}"
    {:ok, _} = start_supervised({Rheo, name: name, backend: Rheo.Backend.ETS}, id: name)
    Frozen.reset()
    stream = "sr-#{System.unique_integer([:positive])}"
    :ok = Rheo.create_stream(stream, rheo: name)
    %{rheo: name, stream: stream}
  end

  test "sequence bounds and query_page cursor", %{rheo: rheo, stream: stream} do
    assert {:ok, _} =
             Rheo.append_batch(
               stream,
               for(i <- 1..5, do: %{type: "t", n: i}),
               rheo: rheo
             )

    assert {:ok, [e2, e3]} =
             Rheo.query(stream, after_sequence: 1, until_sequence: 3, rheo: rheo)

    assert Enum.map([e2, e3], & &1.sequence) == [2, 3]

    assert {:ok, page} = Rheo.query_page(stream, limit: 2, rheo: rheo)
    assert length(page.events) == 2
    assert page.next_cursor == %{0 => 2}

    assert {:ok, page2} =
             Rheo.query_page(stream, limit: 2, cursor: page.next_cursor, rheo: rheo)

    assert Enum.map(page2.events, & &1.sequence) == [3, 4]
  end

  test "stream_query enumerates all events", %{rheo: rheo, stream: stream} do
    assert {:ok, _} =
             Rheo.append_batch(stream, for(_ <- 1..4, do: %{type: "x"}), rheo: rheo)

    ids =
      stream
      |> Rheo.stream_query(limit: 2, rheo: rheo)
      |> Enum.map(& &1.sequence)

    assert ids == [1, 2, 3, 4]
  end

  test "create_group start_after skips earlier events", %{rheo: rheo, stream: stream} do
    assert {:ok, _} =
             Rheo.append_batch(stream, [%{type: "a"}, %{type: "b"}, %{type: "c"}], rheo: rheo)

    assert :ok = Rheo.create_group(stream, "later", start_after: 2, rheo: rheo)
    assert {:ok, [lease]} = Rheo.fetch(stream, "later", limit: 10, rheo: rheo)
    assert lease.event.sequence == 3
  end

  test "replay from_sequence redelivers", %{rheo: rheo, stream: stream} do
    assert :ok = Rheo.create_group(stream, "g", rheo: rheo)
    assert {:ok, event} = Rheo.append(stream, %{type: "once"}, rheo: rheo)
    assert {:ok, [lease]} = Rheo.fetch(stream, "g", limit: 1, rheo: rheo)
    assert :ok = Rheo.ack(lease, rheo: rheo)
    assert {:ok, []} = Rheo.fetch(stream, "g", limit: 1, rheo: rheo)

    assert :ok = Rheo.replay(stream, "g", from_sequence: 0, rheo: rheo)
    assert {:ok, [again]} = Rheo.fetch(stream, "g", limit: 1, rheo: rheo)
    assert again.event_id == event.id
    assert again.attempt >= 1
  end

  test "reset_group requires confirm and does not delete events", %{rheo: rheo, stream: stream} do
    assert :ok = Rheo.create_group(stream, "g", rheo: rheo)
    assert {:ok, event} = Rheo.append(stream, %{type: "keep"}, rheo: rheo)
    assert {:ok, [lease]} = Rheo.fetch(stream, "g", limit: 1, rheo: rheo)
    assert :ok = Rheo.ack(lease, rheo: rheo)

    assert {:error, :confirm_required} = Rheo.reset_group(stream, "g", rheo: rheo)
    assert :ok = Rheo.reset_group(stream, "g", confirm: true, rheo: rheo)

    assert {:ok, [still]} = Rheo.read(stream, after: 0, rheo: rheo)
    assert still.id == event.id

    assert {:ok, [again]} = Rheo.fetch(stream, "g", limit: 1, rheo: rheo)
    assert again.event_id == event.id
  end

  test "lineage helpers", %{rheo: rheo, stream: stream} do
    meta = Lineage.put(%{}, correlation_id: "c1", causation_id: "cause", producer: "svc")
    assert {:ok, event} = Rheo.append(stream, %{type: "x", metadata: meta}, rheo: rheo)
    assert Lineage.get(event, :correlation_id) == "c1"
    assert Lineage.get(event, :causation_id) == "cause"
  end

  test "replay query pages when matches exceed page size", %{rheo: rheo, stream: stream} do
    assert :ok = Rheo.create_group(stream, "g", rheo: rheo)
    assert {:ok, _} = Rheo.append_batch(stream, for(_ <- 1..12, do: %{type: "t"}), rheo: rheo)
    assert {:ok, leases} = Rheo.fetch(stream, "g", limit: 12, rheo: rheo)
    Enum.each(leases, &Rheo.ack(&1, rheo: rheo))

    parent = self()
    handler = "replay-pages-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(
        handler,
        [:rheo, :query, :stop],
        fn _e, _m, _meta, _ -> send(parent, :query) end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler) end)

    assert :ok = Rheo.replay(stream, "g", query: [limit: 5], rheo: rheo)

    count =
      Enum.reduce_while(1..10, 0, fn _, acc ->
        receive do
          :query -> {:cont, acc + 1}
        after
          50 -> {:halt, acc}
        end
      end)

    assert count >= 3
  end

  test "invalid order_by direction raises ArgumentError" do
    assert_raise ArgumentError, ~r/invalid order_by direction :sideways/, fn ->
      Rheo.Query.new("s", order_by: [sequence: :sideways])
    end
  end
end
