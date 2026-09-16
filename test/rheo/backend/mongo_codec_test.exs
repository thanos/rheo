defmodule Rheo.Backend.Mongo.CodecTest do
  use ExUnit.Case, async: true

  alias Rheo.Backend.Mongo.Codec
  alias Rheo.Event

  defp base_doc(overrides) do
    Map.merge(
      %{
        "_id" => "evt_1",
        "stream" => "s",
        "sequence" => 1,
        "type" => "t",
        "payload" => %{}
      },
      overrides
    )
  end

  describe "cast_datetime via event_from_doc/1" do
    test "passes through DateTime" do
      dt = ~U[2026-03-15 12:00:00.000Z]
      event = Codec.event_from_doc(base_doc(%{"timestamp" => dt}))
      assert %Event{timestamp: ^dt} = event
    end

    test "parses valid ISO-8601 binary" do
      event = Codec.event_from_doc(base_doc(%{"timestamp" => "2026-03-15T12:00:00.000Z"}))
      assert event.timestamp == ~U[2026-03-15 12:00:00.000Z]
    end

    test "parses ISO-8601 binary with offset" do
      event = Codec.event_from_doc(base_doc(%{"timestamp" => "2026-03-15T13:00:00+01:00"}))
      assert %DateTime{} = event.timestamp
      assert DateTime.to_unix(event.timestamp) == DateTime.to_unix(~U[2026-03-15 12:00:00Z])
    end

    test "leaves invalid ISO-8601 binary unchanged" do
      bad = "not-a-datetime"
      event = Codec.event_from_doc(base_doc(%{"timestamp" => bad}))
      assert event.timestamp == bad
    end

    test "leaves incomplete date-only binary unchanged" do
      bad = "2026-03-15"
      event = Codec.event_from_doc(base_doc(%{"timestamp" => bad}))
      assert event.timestamp == bad
    end

    test "leaves nil unchanged" do
      event = Codec.event_from_doc(base_doc(%{"timestamp" => nil}))
      assert event.timestamp == nil
    end

    test "leaves non-datetime terms unchanged" do
      event = Codec.event_from_doc(base_doc(%{"timestamp" => 1_700_000_000}))
      assert event.timestamp == 1_700_000_000

      event = Codec.event_from_doc(base_doc(%{"timestamp" => %{secs: 1}}))
      assert event.timestamp == %{secs: 1}
    end

    test "missing timestamp yields nil (default field + catch-all)" do
      doc = Map.delete(base_doc(%{}), "timestamp")
      event = Codec.event_from_doc(doc)
      assert event.timestamp == nil
    end

    test "atom-key timestamp DateTime" do
      dt = ~U[2026-01-01 00:00:00.000Z]
      event = Codec.event_from_doc(base_doc(%{timestamp: dt}))
      assert event.timestamp == dt
    end
  end
end
