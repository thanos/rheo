defmodule Rheo.BackendContract do
  @moduledoc false

  # credo:disable-for-this-file Credo.Check.Refactor.LongQuoteBlocks

  # Executable definition of Rheo backend semantics (ADR 012 / ADR 024).
  # Correctness cases (fencing, at-least-once, group isolation, immutability)
  # always run; only cases for declared guarantees are gated, never skipped
  # silently. Caller must `use ExUnit.Case` or `use Rheo.Case` first.

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      alias Rheo.Backend.Capabilities
      alias Rheo.Clock.Frozen

      @backend Keyword.fetch!(opts, :backend)
      @backend_opts Keyword.get(opts, :backend_opts, [])
      @capabilities Keyword.get_lazy(opts, :capabilities, fn -> @backend.capabilities() end)
      @shared Keyword.get(opts, :shared_rheo)

      setup do
        rheo =
          case @shared do
            nil ->
              name = :"contract_#{System.unique_integer([:positive])}"

              {:ok, _} =
                start_supervised(
                  {Rheo, [name: name, backend: {@backend, @backend_opts}]},
                  id: name
                )

              name

            shared ->
              shared
          end

        Frozen.reset()
        %{rheo: rheo, caps: @capabilities}
      end

      defp stream(prefix \\ "c"), do: "#{prefix}-#{System.unique_integer([:positive])}"

      defp ropts(rheo), do: [rheo: rheo]

      describe "lifecycle" do
        test "capabilities are a validated declaration", %{caps: caps} do
          assert %Capabilities{} = caps
          assert Capabilities.guarantee?(caps, :at_least_once)
          assert Capabilities.guarantee?(caps, :lease_fencing)
          assert Enum.sort(Map.keys(caps.guarantees)) == Enum.sort(Capabilities.guarantee_keys())
          assert Enum.sort(Map.keys(caps.mechanisms)) == Enum.sort(Capabilities.mechanism_keys())
        end

        test "ping and ensure_indexes", %{rheo: rheo} do
          assert :ok = Rheo.ping(ropts(rheo))
          assert :ok = Rheo.ensure_indexes(ropts(rheo))
        end

        test "stream create and duplicate", %{rheo: rheo} do
          s = stream()
          assert :ok = Rheo.create_stream(s, ropts(rheo))
          assert {:error, :already_exists} = Rheo.create_stream(s, ropts(rheo))
        end

        test "group requires an existing stream", %{rheo: rheo} do
          assert {:error, :stream_not_found} = Rheo.create_group(stream(), "g", ropts(rheo))
          assert {:error, :group_not_found} = Rheo.fetch(stream(), "g", ropts(rheo))
        end
      end

      describe "event log" do
        test "append batch assigns contiguous sequences", %{rheo: rheo} do
          s = stream()
          assert :ok = Rheo.create_stream(s, ropts(rheo))

          assert {:ok, events} =
                   Rheo.append_batch(s, [%{type: "a"}, %{type: "b"}, %{type: "c"}], ropts(rheo))

          assert Enum.map(events, & &1.sequence) == [1, 2, 3]
          assert Enum.all?(events, &(&1.stream == s))
        end

        test "read and immutable history after ack", %{rheo: rheo} do
          s = stream()
          assert :ok = Rheo.create_stream(s, ropts(rheo))
          assert :ok = Rheo.create_group(s, "g", ropts(rheo))
          assert {:ok, event} = Rheo.append(s, %{type: "keep"}, ropts(rheo))

          assert {:ok, [lease]} = Rheo.fetch(s, "g", [limit: 1] ++ ropts(rheo))
          assert :ok = Rheo.ack(lease, ropts(rheo))

          assert {:ok, [still]} = Rheo.read(s, [after: 0, limit: 10] ++ ropts(rheo))
          assert still.id == event.id
          assert still.payload == event.payload
        end
      end

      describe "queries" do
        test "portable query filters and order_by", %{rheo: rheo} do
          s = stream()
          assert :ok = Rheo.create_stream(s, ropts(rheo))

          assert {:ok, _} =
                   Rheo.append(s, %{type: "curve_update", currency: "EUR", price: 1}, ropts(rheo))

          assert {:ok, _} =
                   Rheo.append(s, %{type: "curve_update", currency: "USD", price: 2}, ropts(rheo))

          assert {:ok, [eur]} =
                   Rheo.query(s, [type: "curve_update", currency: "EUR"] ++ ropts(rheo))

          assert eur.payload["currency"] == "EUR"

          assert {:ok, [last]} =
                   Rheo.query(s, [order_by: [sequence: :desc], limit: 1] ++ ropts(rheo))

          assert last.payload["currency"] == "USD"
        end

        test "sequence bounds on query", %{rheo: rheo} do
          s = stream()
          assert :ok = Rheo.create_stream(s, ropts(rheo))

          assert {:ok, _} =
                   Rheo.append_batch(s, [%{type: "a"}, %{type: "b"}, %{type: "c"}], ropts(rheo))

          assert {:ok, [only]} =
                   Rheo.query(s, [after_sequence: 1, until_sequence: 2] ++ ropts(rheo))

          assert only.sequence == 2
        end

        test "paging never skips or duplicates under a static set", %{rheo: rheo} do
          s = stream()
          assert :ok = Rheo.create_stream(s, ropts(rheo))
          assert {:ok, written} = Rheo.append_batch(s, for(i <- 1..7, do: %{n: i}), ropts(rheo))

          paged = s |> Rheo.stream_query([limit: 3] ++ ropts(rheo)) |> Enum.map(& &1.id)
          assert paged == Enum.map(written, & &1.id)
        end
      end

      describe "consumer groups" do
        test "independent groups each receive events", %{rheo: rheo} do
          s = stream()
          assert :ok = Rheo.create_stream(s, ropts(rheo))
          assert :ok = Rheo.create_group(s, "a", ropts(rheo))
          assert :ok = Rheo.create_group(s, "b", ropts(rheo))
          assert {:ok, _} = Rheo.append(s, %{type: "x"}, ropts(rheo))

          assert {:ok, [a]} = Rheo.fetch(s, "a", [limit: 1] ++ ropts(rheo))
          assert :ok = Rheo.ack(a, ropts(rheo))
          assert {:ok, [_]} = Rheo.fetch(s, "b", [limit: 1] ++ ropts(rheo))
        end

        test "bounded fetch and ack", %{rheo: rheo} do
          s = stream()
          assert :ok = Rheo.create_stream(s, ropts(rheo))
          assert :ok = Rheo.create_group(s, "g", ropts(rheo))

          assert {:ok, _} =
                   Rheo.append_batch(s, for(_ <- 1..5, do: %{type: "e"}), ropts(rheo))

          assert {:ok, leases} = Rheo.fetch(s, "g", [limit: 2] ++ ropts(rheo))
          assert length(leases) == 2
          Enum.each(leases, &(:ok = Rheo.ack(&1, ropts(rheo))))
        end
      end

      describe "leases and fencing" do
        test "leases carry a receipt", %{rheo: rheo} do
          s = stream()
          assert :ok = Rheo.create_stream(s, ropts(rheo))
          assert :ok = Rheo.create_group(s, "g", ropts(rheo))
          assert {:ok, _} = Rheo.append(s, %{type: "e"}, ropts(rheo))

          assert {:ok, [lease]} = Rheo.fetch(s, "g", [limit: 1] ++ ropts(rheo))
          refute is_nil(lease.receipt)
        end

        test "lease expiry allows redelivery", %{rheo: rheo} do
          Frozen.set(DateTime.utc_now())
          s = stream()
          assert :ok = Rheo.create_stream(s, ropts(rheo))
          assert :ok = Rheo.create_group(s, "g", ropts(rheo))
          assert {:ok, event} = Rheo.append(s, %{type: "e"}, ropts(rheo))

          assert {:ok, [lease]} =
                   Rheo.fetch(s, "g", [limit: 1, lease_ms: 100, consumer_id: "c1"] ++ ropts(rheo))

          assert lease.event_id == event.id
          Frozen.advance(200)

          assert {:ok, [again]} =
                   Rheo.fetch(
                     s,
                     "g",
                     [limit: 1, lease_ms: 1_000, consumer_id: "c2"] ++ ropts(rheo)
                   )

          assert again.event_id == event.id
          assert again.attempt == lease.attempt + 1
          assert again.lease_id != lease.lease_id
        end

        test "stale lease fencing rejects ack, retry, reject, and renew", %{rheo: rheo} do
          Frozen.set(DateTime.utc_now())
          s = stream()
          assert :ok = Rheo.create_stream(s, ropts(rheo))
          assert :ok = Rheo.create_group(s, "g", ropts(rheo))
          assert {:ok, _} = Rheo.append(s, %{type: "e"}, ropts(rheo))

          assert {:ok, [old]} =
                   Rheo.fetch(s, "g", [limit: 1, lease_ms: 50, consumer_id: "old"] ++ ropts(rheo))

          Frozen.advance(100)

          assert {:ok, [new]} =
                   Rheo.fetch(
                     s,
                     "g",
                     [limit: 1, lease_ms: 1_000, consumer_id: "new"] ++ ropts(rheo)
                   )

          assert {:error, :stale_lease} = Rheo.ack(old, ropts(rheo))
          assert {:error, :stale_lease} = Rheo.nack(old, :late, ropts(rheo))
          assert {:error, :stale_lease} = Rheo.reject(old, :late, ropts(rheo))
          assert {:error, :stale_lease} = Rheo.renew(old, ropts(rheo))
          assert :ok = Rheo.ack(new, ropts(rheo))
        end

        test "duplicate ack is stale", %{rheo: rheo} do
          s = stream()
          assert :ok = Rheo.create_stream(s, ropts(rheo))
          assert :ok = Rheo.create_group(s, "g", ropts(rheo))
          assert {:ok, _} = Rheo.append(s, %{type: "e"}, ropts(rheo))
          assert {:ok, [lease]} = Rheo.fetch(s, "g", [limit: 1] ++ ropts(rheo))
          assert :ok = Rheo.ack(lease, ropts(rheo))
          assert {:error, :stale_lease} = Rheo.ack(lease, ropts(rheo))
        end

        test "renew extends lease", %{rheo: rheo} do
          Frozen.set(DateTime.utc_now())
          s = stream()
          assert :ok = Rheo.create_stream(s, ropts(rheo))
          assert :ok = Rheo.create_group(s, "g", ropts(rheo))
          assert {:ok, _} = Rheo.append(s, %{type: "e"}, ropts(rheo))

          assert {:ok, [lease]} =
                   Rheo.fetch(s, "g", [limit: 1, lease_ms: 200] ++ ropts(rheo))

          assert {:ok, renewed} = Rheo.renew(lease, [lease_ms: 5_000] ++ ropts(rheo))
          assert DateTime.compare(renewed.expires_at, lease.expires_at) == :gt

          Frozen.advance(300)
          assert {:ok, []} = Rheo.fetch(s, "g", [limit: 1, consumer_id: "other"] ++ ropts(rheo))
          assert :ok = Rheo.ack(renewed, ropts(rheo))
        end
      end

      describe "retry and reject" do
        test "retry then redelivery; reject dead-letters", %{rheo: rheo} do
          s = stream()
          assert :ok = Rheo.create_stream(s, ropts(rheo))
          assert :ok = Rheo.create_group(s, "g", Keyword.put(ropts(rheo), :max_attempts, 5))
          assert {:ok, event} = Rheo.append(s, %{type: "e"}, ropts(rheo))

          assert {:ok, [lease]} = Rheo.fetch(s, "g", [limit: 1] ++ ropts(rheo))
          assert :ok = Rheo.nack(lease, :tmp, ropts(rheo))

          assert {:ok, [again]} = Rheo.fetch(s, "g", [limit: 1] ++ ropts(rheo))
          assert again.event_id == event.id
          assert again.attempt == 2

          assert :ok = Rheo.reject(again, :bad, ropts(rheo))
          assert {:ok, []} = Rheo.fetch(s, "g", [limit: 1] ++ ropts(rheo))
        end

        test "max attempts rejects", %{rheo: rheo} do
          s = stream()
          assert :ok = Rheo.create_stream(s, ropts(rheo))
          assert :ok = Rheo.create_group(s, "g", Keyword.put(ropts(rheo), :max_attempts, 1))
          assert {:ok, _} = Rheo.append(s, %{type: "e"}, ropts(rheo))

          assert {:ok, [lease]} = Rheo.fetch(s, "g", [limit: 1] ++ ropts(rheo))
          assert :ok = Rheo.nack(lease, :fail, ropts(rheo))
          assert {:ok, []} = Rheo.fetch(s, "g", [limit: 1] ++ ropts(rheo))
        end
      end

      describe "replay" do
        test "replay and reset_group", %{rheo: rheo} do
          s = stream()
          assert :ok = Rheo.create_stream(s, ropts(rheo))
          assert :ok = Rheo.create_group(s, "g", ropts(rheo))
          assert {:ok, event} = Rheo.append(s, %{type: "e"}, ropts(rheo))
          assert {:ok, [lease]} = Rheo.fetch(s, "g", [limit: 1] ++ ropts(rheo))
          assert :ok = Rheo.ack(lease, ropts(rheo))

          assert :ok = Rheo.replay(s, "g", [from_sequence: 0] ++ ropts(rheo))
          assert {:ok, [again]} = Rheo.fetch(s, "g", [limit: 1] ++ ropts(rheo))
          assert again.event_id == event.id
          assert again.event.payload == event.payload

          assert {:error, :confirm_required} = Rheo.reset_group(s, "g", ropts(rheo))
          assert :ok = Rheo.reset_group(s, "g", [confirm: true] ++ ropts(rheo))
          assert {:ok, [still]} = Rheo.read(s, [after: 0] ++ ropts(rheo))
          assert still.id == event.id
        end
      end

      describe "partitions and frontier" do
        test "frontier hole and lag", %{rheo: rheo, caps: caps} do
          if Capabilities.guarantee?(caps, :contiguous_frontier) do
            s = stream()
            assert :ok = Rheo.create_stream(s, [partition_count: 2] ++ ropts(rheo))
            assert :ok = Rheo.create_group(s, "g", ropts(rheo))

            assert {:ok, _} = Rheo.append(s, %{type: "e"}, [partition: 0] ++ ropts(rheo))
            assert {:ok, _} = Rheo.append(s, %{type: "e"}, [partition: 0] ++ ropts(rheo))
            assert {:ok, _} = Rheo.append(s, %{type: "e"}, [partition: 0] ++ ropts(rheo))

            assert {:ok, [a, b, c]} =
                     Rheo.fetch(s, "g", [limit: 3, partition: 0] ++ ropts(rheo))

            assert :ok = Rheo.ack(a, ropts(rheo))
            assert :ok = Rheo.ack(c, ropts(rheo))
            assert {:ok, lag1} = Rheo.lag(s, "g", ropts(rheo))
            assert lag1.partitions[0].frontier == 1

            assert :ok = Rheo.ack(b, ropts(rheo))
            assert {:ok, lag2} = Rheo.lag(s, "g", ropts(rheo))
            assert lag2.partitions[0].frontier == 3
            assert lag2.partitions[0].lag == 0
          end
        end
      end
    end
  end
end
