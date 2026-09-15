defmodule Rheo.Backend.Mongo.ClientTest do
  use ExUnit.Case, async: false

  import Mox

  alias Rheo.Backend.Mongo, as: MongoBackend
  alias Rheo.Backend.Mongo.ClientMock
  alias Rheo.{Lease, Query}

  setup :set_mox_global
  setup :verify_on_exit!

  setup do
    previous = Application.get_env(:rheo, :mongo_client)
    Application.put_env(:rheo, :mongo_client, ClientMock)

    on_exit(fn ->
      if previous do
        Application.put_env(:rheo, :mongo_client, previous)
      else
        Application.delete_env(:rheo, :mongo_client)
      end
    end)

    :ok
  end

  test "ping maps driver errors" do
    expect(ClientMock, :command, fn :h, [ping: 1] -> {:error, :down} end)
    assert {:error, :down} = MongoBackend.ping(:h)
  end

  test "ping success" do
    expect(ClientMock, :command, fn :h, [ping: 1] -> {:ok, %{"ok" => 1}} end)
    assert :ok = MongoBackend.ping(:h)
  end

  test "create_stream duplicate key becomes already_exists" do
    expect(ClientMock, :insert_one, fn :h, "streams", _doc ->
      {:error, %Mongo.WriteError{write_errors: [%{"code" => 11_000}]}}
    end)

    assert {:error, :already_exists} = MongoBackend.create_stream(:h, "s")
  end

  test "create_stream other insert errors pass through" do
    expect(ClientMock, :insert_one, fn :h, "streams", _ -> {:error, :boom} end)
    assert {:error, :boom} = MongoBackend.create_stream(:h, "s")
  end

  test "create_stream success ensures indexes" do
    expect(ClientMock, :insert_one, fn :h, "streams", _ -> {:ok, %{}} end)
    stub(ClientMock, :create_indexes, fn :h, _coll, _indexes -> {:ok, %{}} end)

    assert :ok = MongoBackend.create_stream(:h, "fresh-stream")
  end

  test "ensure_indexes halts on first error" do
    expect(ClientMock, :create_indexes, fn :h, "streams", _ -> {:error, :idx} end)
    assert {:error, :idx} = MongoBackend.ensure_indexes(:h)
  end

  test "create_group when stream missing" do
    expect(ClientMock, :find_one, fn :h, "streams", %{"name" => "nope"} -> nil end)
    assert {:error, :stream_not_found} = MongoBackend.create_group(:h, "nope", "g")
  end

  test "create_group duplicate" do
    expect(ClientMock, :find_one, fn :h, "streams", _ -> %{"name" => "s"} end)

    expect(ClientMock, :insert_one, fn :h, "groups", _ ->
      {:error, %Mongo.WriteError{write_errors: [%{code: 11_000}]}}
    end)

    assert {:error, :already_exists} = MongoBackend.create_group(:h, "s", "g")
  end

  test "append_batch empty short-circuits" do
    assert {:ok, []} = MongoBackend.append_batch(:h, "s", [])
  end

  test "append_batch stream missing" do
    expect(ClientMock, :find_one, fn :h, "streams", _ -> nil end)
    assert {:error, :stream_not_found} = MongoBackend.append_batch(:h, "s", [%{type: "x"}])
  end

  test "read maps docs through codec" do
    expect(ClientMock, :find, fn :h, "events", _filter, _opts ->
      [
        %{
          "_id" => "e1",
          "stream" => "s",
          "partition" => 0,
          "sequence" => 1,
          "timestamp" => ~U[2026-01-01 00:00:00Z],
          "payload" => %{}
        }
      ]
    end)

    assert {:ok, [%Rheo.Event{id: "e1", sequence: 1}]} = MongoBackend.read(:h, "s", limit: 1)
  end

  test "query with order_by desc" do
    expect(ClientMock, :find, fn :h, "events", filter, opts ->
      assert filter["stream"] == "s"
      assert opts[:sort] == %{"sequence" => -1}
      []
    end)

    q = %Query{stream: "s", order_by: [sequence: :desc], limit: 5}
    assert {:ok, []} = MongoBackend.query(:h, q)
  end

  test "renew stale lease" do
    lease = sample_lease()

    expect(ClientMock, :find_one_and_update, fn :h, "deliveries", _f, _u, _o ->
      {:ok, %Mongo.FindAndModifyResult{value: nil}}
    end)

    assert {:error, :stale_lease} = MongoBackend.renew(:h, lease)
  end

  test "renew maps connection errors to backend_unavailable" do
    lease = sample_lease()

    expect(ClientMock, :find_one_and_update, fn :h, "deliveries", _f, _u, _o ->
      {:error, %DBConnection.ConnectionError{message: "gone"}}
    end)

    assert {:error, :backend_unavailable} = MongoBackend.renew(:h, lease)
  end

  test "renew success updates expires_at" do
    lease = sample_lease()

    expect(ClientMock, :find_one_and_update, fn :h, "deliveries", _f, _u, _o ->
      {:ok, %Mongo.FindAndModifyResult{value: %{"status" => "leased"}}}
    end)

    assert {:ok, %Lease{expires_at: expires}} = MongoBackend.renew(:h, lease, lease_ms: 1_000)
    assert DateTime.compare(expires, lease.expires_at) == :gt
  end

  test "ack stale and backend unavailable" do
    lease = sample_lease()

    expect(ClientMock, :find_one_and_update, fn :h, "deliveries", _f, _u, _o ->
      {:ok, %Mongo.FindAndModifyResult{value: nil}}
    end)

    assert {:error, :stale_lease} = MongoBackend.ack(:h, lease)

    expect(ClientMock, :find_one_and_update, fn :h, "deliveries", _f, _u, _o ->
      {:error, %Mongo.Error{code: 6, message: "host unreachable"}}
    end)

    assert {:error, :backend_unavailable} = MongoBackend.ack(:h, lease)
  end

  test "fetch group not found" do
    expect(ClientMock, :find_one, fn :h, "groups", _ -> nil end)
    assert {:error, :group_not_found} = MongoBackend.fetch(:h, "s", "g", limit: 1)
  end

  test "fetch claim with missing event" do
    stub(ClientMock, :find_one, fn
      :h, "groups", _ ->
        %{"stream" => "s", "name" => "g", "next_sequence" => 1, "max_attempts" => 5}

      :h, "events", %{"_id" => "missing"} ->
        nil
    end)

    expect(ClientMock, :find, fn :h, "events", _f, _o -> [] end)

    expect(ClientMock, :find_one_and_update, fn :h, "deliveries", _f, _u, _o ->
      {:ok, %Mongo.FindAndModifyResult{value: %{"event_id" => "missing", "attempt" => 1}}}
    end)

    assert {:error, {:event_missing, "missing"}} = MongoBackend.fetch(:h, "s", "g", limit: 1)
  end

  test "retry dead-letters at max attempts" do
    lease = %{sample_lease() | attempt: 5}

    expect(ClientMock, :find_one, fn :h, "groups", _ ->
      %{"max_attempts" => 5}
    end)

    expect(ClientMock, :find_one_and_update, fn :h, "deliveries", _f, update, _o ->
      assert update["$set"]["status"] == "rejected"
      {:ok, %Mongo.FindAndModifyResult{value: %{}}}
    end)

    assert :ok = MongoBackend.retry(:h, lease, :give_up)
  end

  test "reject success" do
    lease = sample_lease()

    expect(ClientMock, :find_one_and_update, fn :h, "deliveries", _f, update, _o ->
      assert update["$set"]["status"] == "rejected"
      {:ok, %Mongo.FindAndModifyResult{value: %{}}}
    end)

    assert :ok = MongoBackend.reject(:h, lease, "bad")
  end

  test "append_batch insert_many failure" do
    expect(ClientMock, :find_one, fn :h, "streams", _ -> %{"name" => "s"} end)

    expect(ClientMock, :find_one_and_update, fn :h, "streams", _f, _u, _o ->
      {:ok, %Mongo.FindAndModifyResult{value: %{"next_sequence" => 1}}}
    end)

    expect(ClientMock, :insert_many, fn :h, "events", _docs, _opts ->
      {:error, :write_failed}
    end)

    assert {:error, :write_failed} = MongoBackend.append_batch(:h, "s", [%{type: "x"}])
  end

  test "child_spec and default_handle" do
    spec = MongoBackend.child_spec(url: "mongodb://localhost:27017/x", name: :mox_mongo)
    assert spec.id == {Mongo, :mox_mongo}
    assert MongoBackend.default_handle() == Application.get_env(:rheo, :topology, Rheo.Mongo)
  end

  test "retry returns available when under max attempts" do
    lease = sample_lease()

    expect(ClientMock, :find_one, fn :h, "groups", _ -> %{"max_attempts" => 5} end)

    expect(ClientMock, :find_one_and_update, fn :h, "deliveries", _f, update, _o ->
      assert update["$set"]["status"] == "available"
      {:ok, %Mongo.FindAndModifyResult{value: %{}}}
    end)

    assert :ok = MongoBackend.retry(:h, lease, "temporary")
  end

  test "query where filters include metadata and payload fields" do
    expect(ClientMock, :find, fn :h, "events", filter, _opts ->
      assert filter["type"] == "t"
      assert filter["payload.currency"] == "EUR"
      assert filter["metadata.correlation_id"] == "c"
      assert filter["timestamp"]["$gte"]
      assert filter["timestamp"]["$lte"]
      []
    end)

    q = %Query{
      stream: "s",
      where: [type: "t", currency: "EUR", correlation_id: "c"],
      from: ~U[2026-01-01 00:00:00Z],
      to: ~U[2026-01-02 00:00:00Z],
      order_by: [sequence: 1],
      limit: 3
    }

    assert {:ok, []} = MongoBackend.query(:h, q)
  end

  test "allocate_sequences missing stream" do
    expect(ClientMock, :find_one, fn :h, "streams", _ -> %{"name" => "s"} end)

    expect(ClientMock, :find_one_and_update, fn :h, "streams", _f, _u, _o ->
      {:ok, %Mongo.FindAndModifyResult{value: nil}}
    end)

    assert {:error, :stream_not_found} = MongoBackend.append_batch(:h, "s", [%{type: "x"}])
  end

  test "materialize ignore_duplicate on delivery insert" do
    stub(ClientMock, :find_one, fn
      :h, "groups", _ ->
        %{"stream" => "s", "name" => "g", "next_sequence" => 1, "max_attempts" => 5}

      :h, "events", %{"_id" => "e1"} ->
        %{
          "_id" => "e1",
          "stream" => "s",
          "partition" => 0,
          "sequence" => 1,
          "timestamp" => ~U[2026-01-01 00:00:00Z],
          "type" => "t",
          "payload" => %{}
        }
    end)

    expect(ClientMock, :find, fn :h, "events", _f, _o ->
      [
        %{
          "_id" => "e1",
          "stream" => "s",
          "partition" => 0,
          "sequence" => 1,
          "timestamp" => ~U[2026-01-01 00:00:00Z],
          "type" => "t",
          "payload" => %{}
        }
      ]
    end)

    expect(ClientMock, :insert_many, fn :h, "deliveries", _docs, _opts ->
      {:error, %Mongo.WriteError{write_errors: [%{"code" => 11_000}]}}
    end)

    stub(ClientMock, :find_one_and_update, fn
      :h, "groups", _f, _u, _o ->
        {:ok, %Mongo.FindAndModifyResult{value: %{}}}

      :h, "deliveries", _f, _u, _o ->
        {:ok,
         %Mongo.FindAndModifyResult{
           value: %{"event_id" => "e1", "attempt" => 1}
         }}
    end)

    assert {:ok, [%Lease{event_id: "e1"}]} =
             MongoBackend.fetch(:h, "s", "g", limit: 1, consumer_id: "c1")
  end

  defp sample_lease do
    event = %Rheo.Event{
      id: "e1",
      stream: "s",
      partition: 0,
      sequence: 1,
      timestamp: ~U[2026-01-01 00:00:00Z],
      payload: %{}
    }

    %Lease{
      lease_id: "L1",
      stream: "s",
      group: "g",
      event_id: "e1",
      event: event,
      consumer_id: "c1",
      attempt: 1,
      leased_at: ~U[2026-01-01 00:00:00Z],
      expires_at: ~U[2026-01-01 00:00:30Z]
    }
  end
end
