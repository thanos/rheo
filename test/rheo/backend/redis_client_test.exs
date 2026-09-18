defmodule Rheo.Backend.Redis.ClientTest do
  use ExUnit.Case, async: false

  import Mox

  alias Rheo.Backend.Redis, as: RedisBackend
  alias Rheo.Backend.Redis.{ClientMock, Codec, Keys}
  alias Rheo.{Event, Lease, Query}

  @handle :redis_mox_handle

  setup :set_mox_global
  setup :verify_on_exit!

  setup do
    previous = Application.get_env(:rheo, :redis_client)
    Application.put_env(:rheo, :redis_client, ClientMock)

    on_exit(fn ->
      if previous do
        Application.put_env(:rheo, :redis_client, previous)
      else
        Application.delete_env(:rheo, :redis_client)
      end
    end)

    :ok
  end

  test "capabilities declare native stream mechanisms" do
    caps = RedisBackend.capabilities()
    assert caps.guarantees.durable
    assert caps.mechanisms.native_consumer_groups
    assert caps.mechanisms.blocking_reads
    refute caps.mechanisms.secondary_indexes
  end

  test "ensure_indexes is a no-op" do
    assert :ok = RedisBackend.ensure_indexes(@handle)
  end

  test "ping maps connection errors" do
    expect(ClientMock, :command, fn @handle, ["PING"], _opts ->
      {:error, %Redix.ConnectionError{reason: :closed}}
    end)

    assert {:error, :backend_unavailable} = RedisBackend.ping(@handle)
  end

  test "ping succeeds" do
    expect(ClientMock, :command, fn @handle, ["PING"], _opts -> {:ok, "PONG"} end)
    assert :ok = RedisBackend.ping(@handle)
  end

  test "create_stream rejects invalid partition_count" do
    assert {:error, :invalid_partition_count} =
             RedisBackend.create_stream(@handle, "s", partition_count: 0)
  end

  test "create_stream maps already_exists" do
    expect(ClientMock, :command, fn @handle, ["HSETNX", _key, "partition_count", "2"], _opts ->
      {:ok, 0}
    end)

    assert {:error, :already_exists} =
             RedisBackend.create_stream(@handle, "s", partition_count: 2)
  end

  test "create_stream writes meta on first create" do
    expect(ClientMock, :command, fn @handle, ["HSETNX", key, "partition_count", "1"], _opts ->
      assert key == Keys.meta(@handle, "s")
      {:ok, 1}
    end)

    expect(ClientMock, :command, fn @handle, ["HSET", _key, "created_at", _ts], _opts ->
      {:ok, 1}
    end)

    assert :ok = RedisBackend.create_stream(@handle, "s")
  end

  test "create_stream maps Redix errors" do
    expect(ClientMock, :command, fn @handle, ["HSETNX" | _], _opts ->
      {:error, %Redix.Error{message: "OOM"}}
    end)

    assert {:error, {:failed, "OOM"}} = RedisBackend.create_stream(@handle, "s")
  end

  test "create_group when stream is missing" do
    expect(ClientMock, :command, fn @handle, ["HGET", _key, "partition_count"], _opts ->
      {:ok, nil}
    end)

    assert {:error, :stream_not_found} = RedisBackend.create_group(@handle, "s", "g")
  end

  test "create_group opens redis groups and tolerates BUSYGROUP" do
    stub(ClientMock, :command, fn
      @handle, ["HGET", _key, "partition_count"], _opts ->
        {:ok, "1"}

      @handle, ["HSETNX", _key, "max_attempts", _], _opts ->
        {:ok, 1}

      @handle, ["XGROUP", "CREATE" | _], _opts ->
        {:error, %Redix.Error{message: "BUSYGROUP Consumer Group name already exists"}}

      @handle, ["HSET", _key | _fields], _opts ->
        {:ok, 2}
    end)

    assert :ok = RedisBackend.create_group(@handle, "s", "g")
  end

  test "append_batch empty is ok" do
    assert {:ok, []} = RedisBackend.append_batch(@handle, "s", [])
  end

  test "append when stream missing" do
    expect(ClientMock, :command, fn @handle, ["HGET", _key, "partition_count"], _opts ->
      {:ok, nil}
    end)

    assert {:error, :stream_not_found} = RedisBackend.append(@handle, "s", %{type: "t"})
  end

  test "append allocates sequence and writes entry" do
    stub_stream_meta(1)

    expect(ClientMock, :pipeline, fn @handle, [["INCRBY", _key, "1"]], _opts ->
      {:ok, [1]}
    end)

    expect(ClientMock, :pipeline, fn @handle, [["XADD", _stream, "*" | _fields]], _opts ->
      {:ok, ["1-0"]}
    end)

    expect(ClientMock, :pipeline, fn @handle, [["ZADD", _index, "1", "1-0"]], _opts ->
      {:ok, [1]}
    end)

    assert {:ok, %Event{sequence: 1, type: "t", stream: "s"}} =
             RedisBackend.append(@handle, "s", %{type: "t"})
  end

  test "pipeline maps first Redix.Error in results" do
    stub_stream_meta(1)

    expect(ClientMock, :pipeline, fn @handle, [["INCRBY" | _]], _opts ->
      {:ok, [%Redix.Error{message: "NOSCRIPT"}]}
    end)

    assert {:error, {:failed, "NOSCRIPT"}} =
             RedisBackend.append_batch(@handle, "s", [%{type: "t"}])
  end

  test "fetch maps NOGROUP to group_not_found" do
    stub(ClientMock, :command, fn
      @handle, ["HGETALL", key], _opts ->
        assert String.contains?(key, "gmeta:")
        {:ok, ["max_attempts", "5", "frontier:0", "0", "cursor:0", "0"]}

      @handle, ["HGET", _key, "partition_count"], _opts ->
        {:ok, "1"}

      @handle, ["XPENDING" | _], _opts ->
        {:ok, []}

      @handle, ["XREADGROUP" | _], _opts ->
        {:error, %Redix.Error{message: "NOGROUP No such key"}}
    end)

    assert {:error, :group_not_found} =
             RedisBackend.fetch(@handle, "s", "g", limit: 1, consumer_id: "c1")
  end

  test "fetch grants a lease from XREADGROUP" do
    fields = Codec.to_fields(sample_event())

    stub(ClientMock, :command, fn
      @handle, ["HGETALL", key], _opts ->
        assert String.contains?(key, "gmeta:")
        {:ok, ["max_attempts", "5", "frontier:0", "0", "cursor:0", "0"]}

      @handle, ["HGET", _key, "partition_count"], _opts ->
        {:ok, "1"}

      @handle, ["XPENDING" | _], _opts ->
        {:ok, []}

      @handle, ["XREADGROUP" | _], _opts ->
        {:ok, [[Keys.events(@handle, "s", 0), [["1-0", fields]]]]}
    end)

    stub(ClientMock, :pipeline, fn @handle, cmds, _opts ->
      assert Enum.any?(cmds, fn cmd -> hd(cmd) == "HSET" end)
      {:ok, List.duplicate(1, length(cmds))}
    end)

    assert {:ok, [%Lease{receipt: "1-0", attempt: 1, event: %Event{id: "evt_1"}}]} =
             RedisBackend.fetch(@handle, "s", "g", limit: 1, consumer_id: "c1")
  end

  test "ack returns stale_lease when fence is empty" do
    lease = sample_lease()

    expect(ClientMock, :command, fn @handle, ["ZRANGEBYSCORE" | _], _opts ->
      {:ok, ["1-0"]}
    end)

    expect(ClientMock, :command, fn @handle, ["HGETALL" | _], _opts ->
      {:ok, []}
    end)

    assert {:error, :stale_lease} = RedisBackend.ack(@handle, lease)
  end

  test "ack succeeds when fence matches" do
    lease = sample_lease()

    expect(ClientMock, :command, fn @handle, ["ZRANGEBYSCORE" | _], _opts ->
      {:ok, ["1-0"]}
    end)

    expect(ClientMock, :command, fn @handle, ["HGETALL" | _], _opts ->
      {:ok, ["lease_id", lease.lease_id, "attempt", "1"]}
    end)

    expect(ClientMock, :pipeline, fn @handle, cmds, _opts ->
      assert Enum.any?(cmds, &(hd(&1) == "XACK"))
      {:ok, List.duplicate(1, length(cmds))}
    end)

    # frontier collapse
    expect(ClientMock, :command, fn @handle, ["HGET" | _], _opts -> {:ok, "0"} end)
    expect(ClientMock, :command, fn @handle, ["ZRANGEBYSCORE" | _], _opts -> {:ok, ["1"]} end)
    expect(ClientMock, :pipeline, fn @handle, _cmds, _opts -> {:ok, [1, 1]} end)

    assert :ok = RedisBackend.ack(@handle, lease)
  end

  test "renew updates fence expiry" do
    lease = sample_lease()

    expect(ClientMock, :command, fn @handle, ["ZRANGEBYSCORE" | _], _opts ->
      {:ok, ["1-0"]}
    end)

    expect(ClientMock, :command, fn @handle, ["HGETALL" | _], _opts ->
      {:ok, ["lease_id", lease.lease_id, "attempt", "1"]}
    end)

    expect(ClientMock, :command, fn @handle, ["HSET", _key, "expires_at_ms", _], _opts ->
      {:ok, 1}
    end)

    assert {:ok, %Lease{lease_id: id}} = RedisBackend.renew(@handle, lease, lease_ms: 1_000)
    assert id == lease.lease_id
  end

  test "reject dead-letters after authorize" do
    lease = sample_lease()

    expect(ClientMock, :command, fn @handle, ["ZRANGEBYSCORE" | _], _opts ->
      {:ok, ["1-0"]}
    end)

    expect(ClientMock, :command, fn @handle, ["HGETALL" | _], _opts ->
      {:ok, ["lease_id", lease.lease_id, "attempt", "1"]}
    end)

    expect(ClientMock, :pipeline, fn @handle, cmds, _opts ->
      assert Enum.any?(cmds, &(hd(&1) == "XADD"))
      {:ok, List.duplicate(1, length(cmds))}
    end)

    expect(ClientMock, :command, fn @handle, ["HGET" | _], _opts -> {:ok, "0"} end)
    expect(ClientMock, :command, fn @handle, ["ZRANGEBYSCORE" | _], _opts -> {:ok, ["1"]} end)
    expect(ClientMock, :pipeline, fn @handle, _cmds, _opts -> {:ok, [1, 1]} end)

    assert :ok = RedisBackend.reject(@handle, lease, :poison)
  end

  test "retry releases when under max_attempts" do
    lease = sample_lease()

    expect(ClientMock, :command, fn @handle, ["HGETALL", key], _opts ->
      assert key == Keys.group_meta(@handle, "s", "g")
      {:ok, ["max_attempts", "5", "frontier:0", "0"]}
    end)

    expect(ClientMock, :command, fn @handle, ["ZRANGEBYSCORE" | _], _opts ->
      {:ok, ["1-0"]}
    end)

    expect(ClientMock, :command, fn @handle, ["HGETALL" | _], _opts ->
      {:ok, ["lease_id", lease.lease_id, "attempt", "1"]}
    end)

    expect(ClientMock, :command, fn @handle, ["HSET", _fence, "lease_id", "" | _], _opts ->
      {:ok, 1}
    end)

    assert :ok = RedisBackend.retry(@handle, lease, :later)
  end

  test "lag builds from group and stream watermarks" do
    stub(ClientMock, :command, fn
      @handle, ["HGETALL", key], _opts ->
        assert String.contains?(key, "gmeta:")
        {:ok, ["max_attempts", "5", "frontier:0", "1"]}

      @handle, ["HGET", _key, "partition_count"], _opts ->
        {:ok, "1"}
    end)

    stub(ClientMock, :pipeline, fn @handle, [["GET", _seq]], _opts ->
      {:ok, ["3"]}
    end)

    assert {:ok, lag} = RedisBackend.lag(@handle, "s", "g")
    assert lag.lag >= 0
  end

  test "replay with empty opts is invalid" do
    stub(ClientMock, :command, fn
      @handle, ["HGETALL", key], _opts ->
        assert String.contains?(key, "gmeta:")
        {:ok, ["max_attempts", "5", "frontier:0", "0"]}

      @handle, ["HGET", _key, "partition_count"], _opts ->
        {:ok, "1"}
    end)

    assert {:error, :invalid_replay_opts} = RedisBackend.replay(@handle, "s", "g", [])
  end

  test "wait without stream hints idles briefly" do
    assert :ok = RedisBackend.wait(@handle, timeout: 1)
  end

  test "query scans partition ranges and filters in adapter" do
    stub(ClientMock, :command, fn
      @handle, ["HGET", _key, "partition_count"], _opts ->
        {:ok, "1"}

      @handle, ["ZRANGEBYSCORE" | _], _opts ->
        {:ok, ["1-0"]}

      @handle, ["XRANGE" | _], _opts ->
        {:ok, [["1-0", Codec.to_fields(sample_event())]]}
    end)

    query = Query.new("s", type: "t", limit: 10)
    assert {:ok, [%Event{type: "t"}]} = RedisBackend.query(@handle, query)
  end

  test "read returns empty when the index has no entries" do
    expect(ClientMock, :command, fn @handle, ["ZRANGEBYSCORE" | _], _opts ->
      {:ok, []}
    end)

    assert {:ok, []} = RedisBackend.read(@handle, "s", after: 0, limit: 10)
  end

  defp stub_stream_meta(count) do
    stub(ClientMock, :command, fn
      @handle, ["HGET", _key, "partition_count"], _opts ->
        {:ok, Integer.to_string(count)}

      @handle, cmd, opts ->
        flunk("unexpected command #{inspect(cmd)} opts=#{inspect(opts)}")
    end)
  end

  defp sample_event do
    %Event{
      id: "evt_1",
      stream: "s",
      partition: 0,
      sequence: 1,
      timestamp: ~U[2026-01-01 00:00:00.000Z],
      type: "t",
      payload: %{"n" => 1}
    }
  end

  defp sample_lease do
    event = sample_event()

    %Lease{
      lease_id: "L1",
      stream: "s",
      group: "g",
      event_id: event.id,
      event: event,
      consumer_id: "c1",
      attempt: 1,
      leased_at: ~U[2026-01-01 00:00:00.000Z],
      expires_at: ~U[2026-01-01 00:00:30.000Z],
      receipt: "1-0"
    }
  end
end
