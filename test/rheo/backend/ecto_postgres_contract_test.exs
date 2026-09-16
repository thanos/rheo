defmodule Rheo.Backend.EctoPostgresContractTest do
  use ExUnit.Case, async: false
  @moduletag :ecto_postgres

  use Rheo.BackendContract,
    backend: Rheo.Backend.Ecto,
    backend_opts: [repo: Rheo.Test.PostgresRepo],
    capabilities: Rheo.Backend.Ecto.capabilities(:postgres)
end
