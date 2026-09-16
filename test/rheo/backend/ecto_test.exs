defmodule Rheo.Backend.EctoTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL
  alias Ecto.Migrator
  alias Exqlite.Error, as: SqliteError
  alias Rheo.Backend.Ecto, as: Backend
  alias Rheo.Backend.Ecto.{Migrations, Server}
  alias Rheo.Query
  alias Rheo.Test.{MigrationRepo, PostgresRepo, Repos, SqliteRepo}

  setup do
    handle = :"ecto_unit_#{System.unique_integer([:positive])}"

    assert {:ok, _pid} =
             start_supervised({Backend, name: handle, repo: SqliteRepo}, id: handle)

    assert :ok = Backend.ensure_indexes(handle)
    %{handle: handle, stream: "eu-#{System.unique_integer([:positive])}"}
  end

  describe "wiring" do
    test "child_spec requires a host-owned repo" do
      assert_raise ArgumentError, ~r/requires a host-owned :repo/, fn ->
        Backend.child_spec([])
      end
    end

    test "default handle is used when no name is given" do
      assert Backend.default_handle() == Rheo.Ecto

      spec = Backend.child_spec(repo: SqliteRepo)
      assert spec.id == {Server, Rheo.Ecto}
    end

    test "Rheo instance derives the handle from the instance name" do
      name = :"ecto_instance_#{System.unique_integer([:positive])}"

      assert {:ok, _pid} =
               start_supervised(
                 {Rheo, name: name, backend: {Backend, repo: SqliteRepo}},
                 id: name
               )

      assert %Rheo.Instance{handle: handle} = Rheo.Instance.fetch!(name)
      assert handle == Module.concat(name, Ecto)
      assert :ok = Rheo.ping(rheo: name)
    end

    test "missing server maps to backend_unavailable" do
      assert {:error, :backend_unavailable} = Backend.ping(:no_such_ecto_server)
      assert {:error, :backend_unavailable} = Backend.ensure_indexes(:no_such_ecto_server)
      assert {:error, :backend_unavailable} = Backend.create_stream(:no_such_ecto_server, "s")
      assert Backend.capabilities(:no_such_ecto_server) == Backend.capabilities()
    end
  end

  describe "capabilities" do
    test "sqlite is durable but not distributed" do
      caps = Backend.capabilities(:sqlite)
      assert caps.durable
      refute caps.distributed
      refute caps.notifications
      assert caps.contiguous_frontier
    end

    test "postgres is distributed and reports notify opt-in", %{} do
      assert Backend.capabilities(:postgres).distributed
      refute Backend.capabilities(:postgres).notifications

      handle = :"ecto_caps_#{System.unique_integer([:positive])}"

      assert {:ok, _pid} =
               start_supervised(
                 {Backend, name: handle, repo: SqliteRepo, dialect: :postgres, notify: true},
                 id: handle
               )

      assert Backend.capabilities(handle).notifications
    end

    test "notify is ignored on sqlite", %{} do
      handle = :"ecto_no_notify_#{System.unique_integer([:positive])}"

      assert {:ok, _pid} =
               start_supervised(
                 {Backend, name: handle, repo: SqliteRepo, notify: true},
                 id: handle
               )

      assert {:ok, config} = Server.config(handle)
      refute config.notify?
    end
  end

  describe "migrations" do
    test "dialect_for maps supported adapters" do
      assert Migrations.dialect_for(SqliteRepo) == :sqlite
      assert Migrations.dialect_for(PostgresRepo) == :postgres
    end

    test "unsupported adapters raise" do
      assert_raise ArgumentError, ~r/SQL-only|Ecto.Adapters.Postgres/, fn ->
        Migrations.dialect_for(UnsupportedRepoStub)
      end
    end

    test "statements are dialect-specific and idempotent", %{handle: handle} do
      postgres = Enum.join(Migrations.statements(:postgres), "\n")
      sqlite = Enum.join(Migrations.statements(:sqlite), "\n")

      assert postgres =~ "JSONB"
      assert postgres =~ "TIMESTAMPTZ"
      refute sqlite =~ "JSONB"
      assert Enum.all?(Migrations.statements(:sqlite), &String.contains?(&1, "IF NOT EXISTS"))

      # Running twice must not fail.
      assert :ok = Backend.ensure_indexes(handle)
      assert :ok = Backend.ensure_indexes(handle)
    end

    test "prefix qualifies table names" do
      [streams | _rest] = Migrations.statements(:postgres, "rheo")
      assert streams =~ ~s("rheo"."rheo_streams")
    end

    test "V1 runs under Ecto.Migrator and rolls back" do
      {repo, database} = Repos.start_sqlite!(MigrationRepo)
      on_exit(fn -> Repos.cleanup_sqlite(database) end)

      version = 20_260_101_000_000
      assert :ok = Migrator.up(repo, version, Migrations.V1, log: false)
      assert {:ok, %{rows: [[0]]}} = SQL.query(repo, sqlite_count(), [])

      assert :ok = Migrator.down(repo, version, Migrations.V1, log: false)
      assert {:error, _reason} = SQL.query(repo, sqlite_count(), [])
    end
  end

  defp sqlite_count, do: "SELECT count(*) FROM rheo_streams"

  describe "streams and groups" do
    test "duplicate stream and group are rejected", %{handle: handle, stream: stream} do
      assert :ok = Backend.create_stream(handle, stream)
      assert {:error, :already_exists} = Backend.create_stream(handle, stream)
      assert :ok = Backend.create_group(handle, stream, "g")
      assert {:error, :already_exists} = Backend.create_group(handle, stream, "g")
    end

    test "invalid partition count is rejected", %{handle: handle, stream: stream} do
      assert {:error, :invalid_partition_count} =
               Backend.create_stream(handle, stream, partition_count: 0)

      assert {:error, :invalid_partition_count} =
               Backend.create_stream(handle, stream, partition_count: :many)
    end

    test "missing stream and group surface as errors", %{handle: handle} do
      assert {:error, :stream_not_found} = Backend.create_group(handle, "nope", "g")
      assert {:error, :stream_not_found} = Backend.append(handle, "nope", %{type: "x"})
      assert {:error, :group_not_found} = Backend.fetch(handle, "nope", "g")
      assert {:error, :group_not_found} = Backend.lag(handle, "nope", "g")
    end

    test "start_after and start_at resolve cursors", %{handle: handle, stream: stream} do
      assert :ok = Backend.create_stream(handle, stream)

      assert {:ok, _first} =
               Backend.append(handle, stream, %{n: 1}, timestamp: ~U[2026-01-01 00:00:00Z])

      assert {:ok, second} =
               Backend.append(handle, stream, %{n: 2}, timestamp: ~U[2026-02-01 00:00:00Z])

      assert :ok = Backend.create_group(handle, stream, "after", start_after: 1)
      assert {:ok, [lease]} = Backend.fetch(handle, stream, "after", limit: 10)
      assert lease.event.sequence == 2

      assert :ok = Backend.create_group(handle, stream, "at", start_at: second.timestamp)
      assert {:ok, [at_lease | _]} = Backend.fetch(handle, stream, "at", limit: 10)
      assert at_lease.event.sequence == 2

      # No events at or after start_at → cursor stays at the beginning.
      assert :ok =
               Backend.create_group(handle, stream, "future",
                 start_at: ~U[2099-01-01 00:00:00.000Z]
               )

      assert {:ok, [from_start | _]} = Backend.fetch(handle, stream, "future", limit: 10)
      assert from_start.event.sequence == 1
    end

    test "start_after accepts a per-partition map", %{handle: handle, stream: stream} do
      assert :ok = Backend.create_stream(handle, stream, partition_count: 2)
      assert {:ok, _e} = Backend.append(handle, stream, %{n: 1}, partition: 1)
      assert {:ok, _e} = Backend.append(handle, stream, %{n: 2}, partition: 1)

      assert :ok =
               Backend.create_group(handle, stream, "g",
                 start_after: %{"1" => 1},
                 partitions: [1]
               )

      assert {:ok, [lease]} = Backend.fetch(handle, stream, "g", limit: 10, partition: 1)
      assert lease.event.sequence == 2
    end
  end

  describe "append" do
    test "empty batch short-circuits", %{handle: handle, stream: stream} do
      assert :ok = Backend.create_stream(handle, stream)
      assert {:ok, []} = Backend.append_batch(handle, stream, [])
    end

    test "sequences are per partition", %{handle: handle, stream: stream} do
      assert :ok = Backend.create_stream(handle, stream, partition_count: 3)

      assert {:ok, events} =
               Backend.append_batch(
                 handle,
                 stream,
                 [%{n: 1}, %{n: 2}, %{n: 3}, %{n: 4}],
                 partition: 2
               )

      assert Enum.map(events, & &1.sequence) == [1, 2, 3, 4]
      assert Enum.all?(events, &(&1.partition == 2))

      assert {:ok, [other]} = Backend.append_batch(handle, stream, [%{n: 5}], partition: 0)
      assert other.sequence == 1
    end

    test "out-of-range partition is rejected", %{handle: handle, stream: stream} do
      assert :ok = Backend.create_stream(handle, stream)

      assert {:error, :invalid_partition} =
               Backend.append(handle, stream, %{n: 1}, partition: 7)
    end

    test "the unique index refuses a replayed sequence", %{handle: handle, stream: stream} do
      assert :ok = Backend.create_stream(handle, stream)
      assert {:ok, first} = Backend.append(handle, stream, %{n: 1})
      assert first.sequence == 1

      # Losing the allocator row makes the next append restart at 1, which the
      # unique index on (stream, partition, sequence) must refuse rather than
      # silently forking the log.
      {:ok, _result} =
        SQL.query(SqliteRepo, "DELETE FROM rheo_stream_sequences WHERE stream = ?", [stream])

      assert {:error, %SqliteError{}} = Backend.append(handle, stream, %{n: 2})
      assert {:ok, [only]} = Backend.read(handle, stream, after: 0, limit: 10)
      assert only.id == first.id
    end

    test "nested payloads, datetimes, and metadata round-trip", %{handle: handle, stream: stream} do
      assert :ok = Backend.create_stream(handle, stream)

      assert {:ok, event} =
               Backend.append(handle, stream, %{
                 "type" => "nested",
                 "metadata" => %{"schema" => "v1"},
                 "bag" => %{"n" => 1, "at" => ~U[2026-01-01 00:00:00Z], "xs" => [1, %{a: 2}]}
               })

      assert {:ok, [stored]} = Backend.read(handle, stream, after: 0, limit: 10)
      assert stored.id == event.id
      assert stored.metadata["schema"] == "v1"
      assert stored.payload["bag"]["n"] == 1
      assert stored.payload["bag"]["at"] == "2026-01-01T00:00:00Z"
      assert stored.payload["bag"]["xs"] == [1, %{"a" => 2}]
    end
  end

  describe "query" do
    setup %{handle: handle, stream: stream} do
      assert :ok = Backend.create_stream(handle, stream, partition_count: 2)

      assert {:ok, eur} =
               Backend.append(
                 handle,
                 stream,
                 %{
                   type: "curve_update",
                   key: "EUR-1",
                   currency: "EUR",
                   curve: "EURIBOR",
                   metadata: %{correlation_id: "c1", producer: "svc", schema: "v1"}
                 },
                 partition: 0,
                 timestamp: ~U[2026-01-02 12:00:00Z]
               )

      assert {:ok, usd} =
               Backend.append(
                 handle,
                 stream,
                 %{type: "trade", key: "USD-1", currency: "USD"},
                 partition: 1,
                 timestamp: ~U[2026-01-03 12:00:00Z]
               )

      %{eur: eur, usd: usd}
    end

    test "filters on columns, payload, and metadata", %{handle: handle, stream: stream, eur: eur} do
      assert {:ok, [found]} =
               Backend.query(handle, %Query{
                 stream: stream,
                 where: [type: "curve_update", key: "EUR-1", partition: 0, currency: "EUR"],
                 limit: 10
               })

      assert found.id == eur.id

      assert {:ok, [by_metadata]} =
               Backend.query(handle, %Query{
                 stream: stream,
                 where: [correlation_id: "c1", producer: "svc", schema: "v1"],
                 limit: 10
               })

      assert by_metadata.id == eur.id
    end

    test "time and sequence bounds", %{handle: handle, stream: stream, eur: eur, usd: usd} do
      assert {:ok, [only]} =
               Backend.query(handle, %Query{
                 stream: stream,
                 from: ~U[2026-01-02 00:00:00Z],
                 to: ~U[2026-01-02 23:59:59Z],
                 limit: 10
               })

      assert only.id == eur.id

      assert {:ok, both} =
               Backend.query(handle, %Query{
                 stream: stream,
                 from: ~U[2026-01-01 00:00:00Z],
                 limit: 10
               })

      assert length(both) == 2

      assert {:ok, [after_first]} =
               Backend.query(handle, %Query{
                 stream: stream,
                 where: [partition: 1],
                 after_sequence: 0,
                 until_sequence: 1,
                 limit: 10
               })

      assert after_first.id == usd.id
    end

    test "order_by falls back to sequence for unknown fields", %{handle: handle, stream: stream} do
      assert {:ok, descending} =
               Backend.query(handle, %Query{
                 stream: stream,
                 order_by: [timestamp: :desc, sequence: :desc],
                 limit: 10
               })

      assert length(descending) == 2

      assert {:ok, fallback} =
               Backend.query(handle, %Query{
                 stream: stream,
                 order_by: [nonsense: :asc],
                 limit: 10
               })

      assert length(fallback) == 2
    end

    test "non-atom filter keys are ignored", %{handle: handle, stream: stream} do
      assert {:ok, both} =
               Backend.query(handle, %Query{stream: stream, where: [{1, :ignored}], limit: 10})

      assert length(both) == 2
    end

    test "cursor is applied", %{handle: handle, stream: stream, usd: usd} do
      assert {:ok, [next]} =
               Backend.query(handle, %Query{
                 stream: stream,
                 where: [partition: 1],
                 cursor: %{after_sequence: 0},
                 limit: 10
               })

      assert next.id == usd.id
    end
  end

  describe "leases" do
    setup %{handle: handle, stream: stream} do
      assert :ok = Backend.create_stream(handle, stream)
      assert :ok = Backend.create_group(handle, stream, "g", max_attempts: 2)
      assert {:ok, events} = Backend.append_batch(handle, stream, [%{n: 1}, %{n: 2}, %{n: 3}])
      %{events: events}
    end

    test "renew on a stale lease fails", %{handle: handle, stream: stream} do
      assert {:ok, [lease]} = Backend.fetch(handle, stream, "g", limit: 1)
      assert {:ok, renewed} = Backend.renew(handle, lease, lease_ms: 60_000)
      assert DateTime.compare(renewed.expires_at, lease.expires_at) == :gt

      assert :ok = Backend.ack(handle, renewed)
      assert {:error, :stale_lease} = Backend.renew(handle, renewed, lease_ms: 60_000)
    end

    test "retry dead-letters at max_attempts", %{handle: handle, stream: stream} do
      assert {:ok, [lease]} = Backend.fetch(handle, stream, "g", limit: 1)
      assert :ok = Backend.retry(handle, lease, :first)

      assert {:ok, [second]} = Backend.fetch(handle, stream, "g", limit: 1)
      assert second.attempt == 2
      assert :ok = Backend.retry(handle, second, :second)

      # Attempt 2 hit max_attempts, so the delivery is now dead-lettered.
      assert {:ok, [third]} = Backend.fetch(handle, stream, "g", limit: 1)
      assert third.event.sequence == 2
    end

    test "reject advances the frontier past a hole", %{handle: handle, stream: stream} do
      assert {:ok, [first, second, third]} = Backend.fetch(handle, stream, "g", limit: 3)
      assert :ok = Backend.ack(handle, first)
      assert :ok = Backend.ack(handle, third)

      assert {:ok, lag} = Backend.lag(handle, stream, "g")
      assert lag.partitions[0].frontier == 1
      assert lag.partitions[0].high_watermark == 3
      assert lag.partitions[0].lag == 2

      assert :ok = Backend.reject(handle, second, :poison)
      assert {:ok, closed} = Backend.lag(handle, stream, "g")
      assert closed.partitions[0].frontier == 3
      assert closed.lag == 0
    end

    test "fetch keeps claiming until demand is met", %{handle: handle, stream: stream} do
      assert {:ok, leases} = Backend.fetch(handle, stream, "g", limit: 10)
      assert length(leases) == 3
      assert Enum.map(leases, & &1.event.sequence) == [1, 2, 3]
      assert Enum.uniq(Enum.map(leases, & &1.lease_id)) == Enum.map(leases, & &1.lease_id)
    end

    test "a consumer assigned no partitions claims nothing", %{handle: handle, stream: stream} do
      assert {:ok, []} = Backend.fetch(handle, stream, "g", limit: 10, partitions: [])
      assert {:ok, lag} = Backend.lag(handle, stream, "g", partitions: [])
      assert lag.lag == 0
    end

    test "settling a lease Rheo never issued is refused", %{handle: handle, stream: stream} do
      assert {:ok, [lease]} = Backend.fetch(handle, stream, "g", limit: 1)
      forged = %{lease | lease_id: "forged"}

      assert {:error, :stale_lease} = Backend.ack(handle, forged)
      assert {:error, :stale_lease} = Backend.retry(handle, forged, :nope)
      assert {:error, :stale_lease} = Backend.reject(handle, forged, :nope)
      assert :ok = Backend.ack(handle, lease)
    end
  end

  describe "replay and reset" do
    setup %{handle: handle, stream: stream} do
      assert :ok = Backend.create_stream(handle, stream)
      assert :ok = Backend.create_group(handle, stream, "g")
      assert {:ok, [e1, e2]} = Backend.append_batch(handle, stream, [%{n: 1}, %{n: 2}])
      assert {:ok, [l1, l2]} = Backend.fetch(handle, stream, "g", limit: 2)
      assert :ok = Backend.ack(handle, l1)
      assert :ok = Backend.ack(handle, l2)
      %{e1: e1, e2: e2}
    end

    test "unknown options are rejected", %{handle: handle, stream: stream} do
      assert {:error, :invalid_replay_opts} = Backend.replay(handle, stream, "g")
      assert {:error, :invalid_replay_opts} = Backend.replay(handle, stream, "g", foo: 1)
    end

    test "from_sequence rewinds the frontier", %{handle: handle, stream: stream} do
      assert {:ok, before} = Backend.lag(handle, stream, "g")
      assert before.partitions[0].frontier == 2

      assert :ok = Backend.replay(handle, stream, "g", from_sequence: 0)
      assert {:ok, rewound} = Backend.lag(handle, stream, "g")
      assert rewound.partitions[0].frontier == 0

      assert {:ok, leases} = Backend.fetch(handle, stream, "g", limit: 10)
      assert length(leases) == 2
    end

    test "event_ids reopens only the listed deliveries", %{
      handle: handle,
      stream: stream,
      e2: e2
    } do
      assert :ok = Backend.replay(handle, stream, "g", event_ids: [e2.id])
      assert {:ok, [lease]} = Backend.fetch(handle, stream, "g", limit: 10)
      assert lease.event_id == e2.id

      assert {:ok, lag} = Backend.lag(handle, stream, "g")
      assert lag.partitions[0].frontier == 1
    end

    test "empty replay lists are no-ops", %{handle: handle, stream: stream} do
      assert :ok = Backend.replay(handle, stream, "g", event_ids: [])
      assert :ok = Backend.replay(handle, stream, "g", events: [])
    end

    test "events recreates deleted delivery rows", %{handle: handle, stream: stream, e2: e2} do
      assert :ok = Backend.reset_group(handle, stream, "g", start_after: 100)
      assert {:ok, []} = Backend.fetch(handle, stream, "g", limit: 10)

      assert :ok = Backend.replay(handle, stream, "g", events: [e2])
      assert {:ok, [lease]} = Backend.fetch(handle, stream, "g", limit: 10)
      assert lease.event_id == e2.id
    end

    test "reset_group keeps the immutable log", %{handle: handle, stream: stream, e1: e1} do
      assert :ok = Backend.reset_group(handle, stream, "g")
      assert {:ok, [first | _rest]} = Backend.read(handle, stream, after: 0, limit: 10)
      assert first.id == e1.id

      assert {:ok, leases} = Backend.fetch(handle, stream, "g", limit: 10)
      assert length(leases) == 2
    end
  end

  describe "postgres" do
    @describetag :ecto_postgres

    setup do
      handle = :"ecto_pg_#{System.unique_integer([:positive])}"

      assert {:ok, _pid} =
               start_supervised(
                 {Backend, name: handle, repo: PostgresRepo, notify: true},
                 id: handle
               )

      assert :ok = Backend.ensure_indexes(handle)
      %{pg: handle, pg_stream: "pg-#{System.unique_integer([:positive])}"}
    end

    test "append notifies listeners", %{pg: handle, pg_stream: stream} do
      assert Backend.capabilities(handle).notifications
      assert :ok = Backend.create_stream(handle, stream)

      {:ok, listener} = Postgrex.Notifications.start_link(Repos.postgres_opts())
      {:ok, _ref} = Postgrex.Notifications.listen(listener, "rheo_events")

      assert {:ok, _event} = Backend.append(handle, stream, %{n: 1})

      assert_receive {:notification, _pid, _ref, "rheo_events", payload}, 2_000
      assert Jason.decode!(payload) == %{"stream" => stream, "count" => 1}
    end

    test "concurrent fetches never hand out the same event twice", %{
      pg: handle,
      pg_stream: stream
    } do
      assert :ok = Backend.create_stream(handle, stream)
      assert :ok = Backend.create_group(handle, stream, "g")

      assert {:ok, _events} =
               Backend.append_batch(handle, stream, for(n <- 1..20, do: %{n: n}))

      claimed =
        1..4
        |> Task.async_stream(
          fn n ->
            {:ok, leases} =
              Backend.fetch(handle, stream, "g", limit: 5, consumer_id: "worker-#{n}")

            Enum.map(leases, & &1.event_id)
          end,
          max_concurrency: 4,
          timeout: 10_000
        )
        |> Enum.flat_map(fn {:ok, ids} -> ids end)

      assert length(claimed) == 20
      assert length(Enum.uniq(claimed)) == 20
    end
  end
end

defmodule UnsupportedRepoStub do
  @moduledoc false
  def __adapter__, do: Ecto.Adapters.MyMadeUpAdapter
end
