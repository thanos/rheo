defmodule Rheo.Backend.NativeStreamContractTest do
  use ExUnit.Case, async: false
  use Rheo.BackendContract, backend: Rheo.Backend.NativeStreamDouble
end
