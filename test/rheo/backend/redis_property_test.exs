defmodule Rheo.Backend.RedisPropertyTest do
  # Same portable invariants as `Rheo.PropertyTest`, exercised on Redis Streams.
  # Tagged `:redis` and skipped when Redis is unavailable (see test_helper.exs).
  use ExUnit.Case, async: false
  use ExUnitProperties

  @moduletag :redis
  @moduletag timeout: 120_000

  alias Rheo.Backend.Redis
  alias Rheo.Backend.Redis.Keys
  alias Rheo.Clock.Frozen

  setup do
    Frozen.reset()
    rheo = :"redis_prop_#{System.unique_integer([:positive])}"
    start_supervised!({Rheo, name: rheo, backend: {Redis, url: Rheo.Test.Redis.url()}}, id: rheo)
    handle = Rheo.Instance.fetch!(rheo).handle
    on_exit(fn -> Rheo.Test.Redis.flush(Keys.prefix(handle) <> "*") end)
    %{rheo: rheo}
  end

  defp stream(prefix), do: "#{prefix}-#{System.unique_integer([:positive])}"

  property "sequences are contiguous within each partition", %{rheo: rheo} do
    check all(
            keys <- list_of(string(:alphanumeric, min_length: 1), min_length: 1, max_length: 12),
            max_runs: 8
          ) do
      s = stream("rprop-seq")
      :ok = Rheo.create_stream(s, partition_count: 3, rheo: rheo)
      {:ok, events} = Rheo.append_batch(s, Enum.map(keys, &%{type: "e", key: &1}), rheo: rheo)

      events
      |> Enum.group_by(& &1.partition, & &1.sequence)
      |> Enum.each(fn {_partition, seqs} -> assert seqs == Enum.to_list(1..length(seqs)) end)

      assert Enum.all?(events, &(&1.sequence >= 1))
    end
  end

  property "the frontier never passes an unACKed lower sequence", %{rheo: rheo} do
    check all(n <- integer(2..6), hole <- integer(1..n), max_runs: 8) do
      s = stream("rprop-frontier")
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
    check all(n <- integer(1..6), max_runs: 8) do
      s = stream("rprop-groups")
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
    check all(op <- member_of([:ack, :nack, :reject, :renew]), max_runs: 8) do
      Frozen.set(DateTime.utc_now())
      s = stream("rprop-fence")
      :ok = Rheo.create_stream(s, rheo: rheo)
      :ok = Rheo.create_group(s, "g", rheo: rheo)
      {:ok, _} = Rheo.append(s, %{type: "e"}, rheo: rheo)

      {:ok, [old]} = Rheo.fetch(s, "g", limit: 1, lease_ms: 10, rheo: rheo)
      assert is_binary(old.receipt)
      assert old.receipt != old.lease_id

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
    check all(n <- integer(1..5), max_runs: 6) do
      s = stream("rprop-replay")
      :ok = Rheo.create_stream(s, rheo: rheo)
      :ok = Rheo.create_group(s, "g", rheo: rheo)
      {:ok, written} = Rheo.append_batch(s, for(i <- 1..n, do: %{i: i}), rheo: rheo)
      {:ok, leases} = Rheo.fetch(s, "g", limit: n, rheo: rheo)
      Enum.each(leases, &(:ok = Rheo.ack(&1, rheo: rheo)))

      :ok = Rheo.replay(s, "g", from_sequence: 0, rheo: rheo)
      {:ok, again} = Rheo.fetch(s, "g", limit: n, rheo: rheo)

      assert Enum.map(again, & &1.event.id) == Enum.map(written, & &1.id)
      assert Enum.map(again, & &1.event.sequence) == Enum.map(written, & &1.sequence)
      assert {:ok, ^written} = Rheo.read(s, after: 0, limit: n, rheo: rheo)
    end
  end

  property "paging never skips or duplicates a static event set", %{rheo: rheo} do
    check all(n <- integer(0..8), page <- integer(1..4), max_runs: 6) do
      s = stream("rprop-page")
      :ok = Rheo.create_stream(s, rheo: rheo)
      {:ok, written} = Rheo.append_batch(s, for(i <- 1..n//1, do: %{i: i}), rheo: rheo)

      paged = s |> Rheo.stream_query(limit: page, rheo: rheo) |> Enum.map(& &1.id)
      assert paged == Enum.map(written, & &1.id)
    end
  end
end
