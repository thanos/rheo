defmodule Rheo.DoctestTest do
  use ExUnit.Case, async: false

  doctest Rheo
  doctest Rheo.Event
  doctest Rheo.Lease
  doctest Rheo.Id
  doctest Rheo.Query
  doctest Rheo.Clock
  doctest Rheo.Clock.System
  doctest Rheo.Clock.Frozen
  doctest Rheo.Backend.Mongo
  doctest Rheo.Backend.Mongo.Codec
  doctest Rheo.Backend.Ecto
  doctest Rheo.Backend.Ecto.Codec
  doctest Rheo.Telemetry
  doctest Rheo.Instance
end
