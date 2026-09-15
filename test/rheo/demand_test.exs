defmodule Rheo.DemandTest do
  use Rheo.Case, async: false

  @moduletag :mongo

  setup do
    stream = unique_stream("demand")
    :ok = Rheo.create_stream(stream)
    :ok = Rheo.create_group(stream, "risk")
    %{stream: stream}
  end

  test "max_demand / limit bounds outstanding leases", %{stream: stream} do
    {:ok, _} = Rheo.append_batch(stream, for(i <- 1..30, do: %{type: "e", i: i}))
    {:ok, leases} = Rheo.fetch(stream, "risk", limit: 5, consumer_id: "c1")
    assert length(leases) == 5
  end
end
