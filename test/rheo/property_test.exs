defmodule Rheo.PropertyTest do
  # Invariants from the v0.8 prompt, run on ETS so they execute on every
  # `mix test`. Backend equivalence is covered by the shared contract suite.
  use ExUnit.Case, async: false
  use ExUnitProperties

  alias Rheo.Clock.Frozen
  alias Rheo.Partition

  setup do
    rheo = :"property_rheo_#{System.unique_integer([:positive])}"
    start_supervised!({Rheo, name: rheo, backend: Rheo.Backend.ETS}, id: rheo)
    %{rheo: rheo}
  end

  defp stream(prefix), do: "#{prefix}-#{System.unique_integer([:positive])}"

  property "same key always routes to the same partition" do
    check all(key <- string(:printable), count <- integer(1..16)) do
      {:ok, p1} = Partition.resolve(%{}, [key: key], count)
      {:ok, p2} = Partition.resolve(%{"key" => key}, [], count)
      assert p1 == p2
      assert p1 in 0..(count - 1)
    end
  end

  property "sequences are contiguous within each partition", %{rheo: rheo} do
    check all(
            keys <- list_of(string(:alphanumeric, min_length: 1), min_length: 1, max_length: 20)
          ) do
      s = stream("prop-seq")
      :ok = Rheo.create_stream(s, partition_count: 3, rheo: rheo)
      {:ok, events} = Rheo.append_batch(s, Enum.map(keys, &%{type: "e", key: &1}), rheo: rheo)

      events
      |> Enum.group_by(& &1.partition, & &1.sequence)
      |> Enum.each(fn {_partition, seqs} -> assert seqs == Enum.to_list(1..length(seqs)) end)
    end
  end

  property "the frontier never passes an unACKed lower sequence", %{rheo: rheo} do
    check all(n <- integer(2..8), hole <- integer(1..n)) do
      s = stream("prop-frontier")
      :ok = Rheo.create_stream(s, rheo: rheo)
      :ok = Rheo.create_group(s, "g", rheo: rheo)
      {:ok, _} = Rheo.append_batch(s, for(i <- 1..n, do: %{i: i}), rheo: rheo)
      {:ok, leases} = Rheo.fetch(s, "g", limit: n, rheo: rheo)

      leases
      |> Enum.reject(&(&1.event.sequence == hole))
      |> Enum.each(&(:ok = Rheo.ack(&1, rheo: rheo)))

      {:ok, lag} = Rheo.lag(s, "g", rheo: rheo)
      assert lag.partitions[0].frontier == hole - 1
      assert lag.partitions[0].lag == n - hole + 1
    end
  end

  property "ACK in one group never changes another group's deliveries", %{rheo: rheo} do
    check all(n <- integer(1..10)) do
      s = stream("prop-groups")
      :ok = Rheo.create_stream(s, rheo: rheo)
      :ok = Rheo.create_group(s, "a", rheo: rheo)
      :ok = Rheo.create_group(s, "b", rheo: rheo)
      {:ok, _} = Rheo.append_batch(s, for(i <- 1..n, do: %{i: i}), rheo: rheo)

      {:ok, a_leases} = Rheo.fetch(s, "a", limit: n, rheo: rheo)
      Enum.each(a_leases, &(:ok = Rheo.ack(&1, rheo: rheo)))

      {:ok, b_leases} = Rheo.fetch(s, "b", limit: n, rheo: rheo)
      assert length(b_leases) == n
      assert Enum.all?(b_leases, &(&1.attempt == 1))
      assert {:ok, %{lag: ^n}} = Rheo.lag(s, "b", rheo: rheo)
    end
  end

  property "an old fencing token can never settle a newer lease", %{rheo: rheo} do
    check all(op <- member_of([:ack, :nack, :reject, :renew])) do
      Frozen.set(DateTime.utc_now())
      s = stream("prop-fence")
      :ok = Rheo.create_stream(s, rheo: rheo)
      :ok = Rheo.create_group(s, "g", rheo: rheo)
      {:ok, _} = Rheo.append(s, %{type: "e"}, rheo: rheo)

      {:ok, [old]} = Rheo.fetch(s, "g", limit: 1, lease_ms: 10, rheo: rheo)
      Frozen.advance(20)
      {:ok, [new]} = Rheo.fetch(s, "g", limit: 1, lease_ms: 60_000, rheo: rheo)

      result =
        case op do
          :ack -> Rheo.ack(old, rheo: rheo)
          :nack -> Rheo.nack(old, :late, rheo: rheo)
          :reject -> Rheo.reject(old, :late, rheo: rheo)
          :renew -> Rheo.renew(old, rheo: rheo)
        end

      assert result == {:error, :stale_lease}
      assert :ok = Rheo.ack(new, rheo: rheo)
      Frozen.reset()
    end
  end

  property "replay changes eligibility, not event content", %{rheo: rheo} do
    check all(n <- integer(1..8)) do
      s = stream("prop-replay")
      :ok = Rheo.create_stream(s, rheo: rheo)
      :ok = Rheo.create_group(s, "g", rheo: rheo)
      {:ok, written} = Rheo.append_batch(s, for(i <- 1..n, do: %{i: i}), rheo: rheo)
      {:ok, leases} = Rheo.fetch(s, "g", limit: n, rheo: rheo)
      Enum.each(leases, &(:ok = Rheo.ack(&1, rheo: rheo)))

      :ok = Rheo.replay(s, "g", from_sequence: 0, rheo: rheo)
      {:ok, again} = Rheo.fetch(s, "g", limit: n, rheo: rheo)

      assert Enum.map(again, & &1.event) == written
      assert {:ok, ^written} = Rheo.read(s, after: 0, limit: n, rheo: rheo)
    end
  end

  property "paging never skips or duplicates a static event set", %{rheo: rheo} do
    check all(n <- integer(0..12), page <- integer(1..5)) do
      s = stream("prop-page")
      :ok = Rheo.create_stream(s, rheo: rheo)
      {:ok, written} = Rheo.append_batch(s, for(i <- 1..n//1, do: %{i: i}), rheo: rheo)

      paged = s |> Rheo.stream_query(limit: page, rheo: rheo) |> Enum.map(& &1.id)
      assert paged == Enum.map(written, & &1.id)
    end
  end
end
