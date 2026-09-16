defmodule Rheo.Backend.EctoCodecTest do
  use ExUnit.Case, async: true

  alias Rheo.Backend.Ecto.Codec

  describe "decode_json/1" do
    test "passes maps through untouched" do
      assert Codec.decode_json(%{"a" => 1}) == %{"a" => 1}
    end

    test "unparseable and non-JSON values decode to an empty map" do
      assert Codec.decode_json("not json") == %{}
      assert Codec.decode_json("[1, 2]") == %{}
      assert Codec.decode_json(42) == %{}
    end
  end

  describe "timestamps" do
    test "postgres keeps the DateTime struct" do
      datetime = ~U[2026-01-15 12:00:00.123456Z]
      assert Codec.encode_datetime(:postgres, datetime) == datetime
      assert Codec.encode_datetime(:postgres, nil) == nil
    end

    test "sqlite pads to microseconds so strings sort chronologically" do
      earlier = Codec.encode_datetime(:sqlite, ~U[2026-01-15 12:00:00.900Z])
      later = Codec.encode_datetime(:sqlite, ~U[2026-01-15 12:00:01.000Z])

      assert earlier == "2026-01-15T12:00:00.900000Z"
      assert earlier < later
    end

    test "decoding accepts naive, aware, and malformed values" do
      assert Codec.decode_datetime(~N[2026-01-15 12:00:00]) == ~U[2026-01-15 12:00:00Z]
      assert Codec.decode_datetime(~U[2026-01-15 12:00:00Z]) == ~U[2026-01-15 12:00:00Z]
      assert Codec.decode_datetime("nonsense") == nil
    end
  end

  describe "rows/2" do
    test "an empty result set yields no rows" do
      assert Codec.rows(nil, [[1]]) == []
      assert Codec.rows(["a"], nil) == []
      assert Codec.rows([], []) == []
    end
  end

  describe "event_from_row/1" do
    test "defaults a missing partition to zero" do
      event = Codec.event_from_row(%{"id" => "e1", "stream" => "s", "sequence" => 1})

      assert event.partition == 0
      assert event.timestamp == nil
      assert event.metadata == %{}
      assert event.payload == %{}
    end
  end
end
