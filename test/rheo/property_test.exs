defmodule Rheo.PropertyTest do
  use Rheo.Case, async: false
  use ExUnitProperties

  @moduletag :mongo

  property "acked events are not normally deliverable again to that group" do
    check all(n <- integer(1..20)) do
      stream = unique_stream("prop-ack")
      :ok = Rheo.create_stream(stream)
      :ok = Rheo.create_group(stream, "g")
      {:ok, _} = Rheo.append_batch(stream, for(i <- 1..n, do: %{type: "e", i: i}))

      {:ok, leases} = Rheo.fetch(stream, "g", limit: n, consumer_id: "c")
      Enum.each(leases, &Rheo.ack/1)

      assert {:ok, []} = Rheo.fetch(stream, "g", limit: n, consumer_id: "c2")
    end
  end

  property "one group's ack does not affect another group" do
    check all(n <- integer(1..10)) do
      stream = unique_stream("prop-groups")
      :ok = Rheo.create_stream(stream)
      :ok = Rheo.create_group(stream, "a")
      :ok = Rheo.create_group(stream, "b")
      {:ok, _} = Rheo.append_batch(stream, for(i <- 1..n, do: %{type: "e", i: i}))

      {:ok, a_leases} = Rheo.fetch(stream, "a", limit: n, consumer_id: "a1")
      Enum.each(a_leases, &Rheo.ack/1)

      {:ok, b_leases} = Rheo.fetch(stream, "b", limit: n, consumer_id: "b1")
      assert length(b_leases) == n
    end
  end

  property "sequences within a partition are monotonic" do
    check all(n <- integer(1..15)) do
      stream = unique_stream("prop-seq")
      :ok = Rheo.create_stream(stream)
      {:ok, events} = Rheo.append_batch(stream, for(i <- 1..n, do: %{type: "e", i: i}))
      seqs = Enum.map(events, & &1.sequence)
      assert seqs == Enum.to_list(1..n)
    end
  end

  property "consumption never deletes immutable events" do
    check all(n <- integer(1..10)) do
      stream = unique_stream("prop-keep")
      :ok = Rheo.create_stream(stream)
      :ok = Rheo.create_group(stream, "g")
      {:ok, written} = Rheo.append_batch(stream, for(i <- 1..n, do: %{type: "e", i: i}))
      {:ok, leases} = Rheo.fetch(stream, "g", limit: n, consumer_id: "c")
      Enum.each(leases, &Rheo.ack/1)

      assert {:ok, read} = Rheo.read(stream, after: 0, limit: n)
      assert Enum.map(read, & &1.id) == Enum.map(written, & &1.id)
    end
  end
end
