defmodule Rheo.CompetingConsumersTest do
  use Rheo.Case, async: false

  @moduletag :mongo

  setup do
    stream = unique_stream("compete")
    :ok = Rheo.create_stream(stream)
    :ok = Rheo.create_group(stream, "risk")
    %{stream: stream}
  end

  test "workers in a group compete without double-leasing", %{stream: stream} do
    n = 50
    {:ok, _} = Rheo.append_batch(stream, for(i <- 1..n, do: %{type: "e", i: i}))

    results =
      1..10
      |> Enum.map(fn i ->
        Task.async(fn ->
          Rheo.fetch(stream, "risk", limit: 10, consumer_id: "c#{i}")
        end)
      end)
      |> Task.await_many(10_000)

    leases =
      results
      |> Enum.flat_map(fn
        {:ok, ls} -> ls
        _ -> []
      end)

    event_ids = Enum.map(leases, & &1.event_id)
    assert length(event_ids) == length(Enum.uniq(event_ids))
    assert length(event_ids) == n
  end

  test "simultaneous fetches never yield two active leases for same event", %{stream: stream} do
    {:ok, _} = Rheo.append_batch(stream, for(i <- 1..20, do: %{type: "race", i: i}))

    for _ <- 1..5 do
      results =
        1..8
        |> Enum.map(fn i ->
          Task.async(fn ->
            Rheo.fetch(stream, "risk", limit: 3, consumer_id: "w#{i}-#{System.unique_integer()}")
          end)
        end)
        |> Task.await_many(10_000)

      leases = Enum.flat_map(results, fn {:ok, ls} -> ls end)
      ids = Enum.map(leases, & &1.event_id)
      assert length(ids) == length(Enum.uniq(ids))
    end
  end
end
