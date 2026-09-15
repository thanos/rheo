defmodule Rheo.Backend.MongoContractTest do
  use Rheo.Case, async: false
  @moduletag :mongo

  use Rheo.BackendContract,
    backend: Rheo.Backend.Mongo,
    shared_rheo: Rheo
end
