defmodule Rheo.Test.Mongo do
  @moduledoc false

  # Helpers for the `:mongo`-tagged suites. A live Mongo is optional: unit
  # tests and mocked client tests run without it. Availability is a short TCP
  # connect so a down server does not stall the suite for the driver timeout.

  def url, do: System.get_env("RHEO_MONGO_URL", "mongodb://localhost:27017/rheo_test")

  def available? do
    if Code.ensure_loaded?(Mongo) do
      uri = URI.parse(url())
      host = String.to_charlist(uri.host || "localhost")
      port = uri.port || 27_017

      case :gen_tcp.connect(host, port, [:binary, active: false], 250) do
        {:ok, sock} ->
          :gen_tcp.close(sock)
          true

        _error ->
          false
      end
    else
      false
    end
  end
end
