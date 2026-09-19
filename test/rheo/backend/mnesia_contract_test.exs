defmodule Rheo.Backend.MnesiaContractTest do
  use ExUnit.Case, async: false
  use Rheo.BackendContract, backend: Rheo.Backend.Mnesia
end
