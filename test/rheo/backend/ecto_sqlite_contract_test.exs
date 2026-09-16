defmodule Rheo.Backend.EctoSqliteContractTest do
  use ExUnit.Case, async: false

  use Rheo.BackendContract,
    backend: Rheo.Backend.Ecto,
    backend_opts: [repo: Rheo.Test.SqliteRepo],
    capabilities: Rheo.Backend.Ecto.capabilities(:sqlite)
end
