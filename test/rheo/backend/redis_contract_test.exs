defmodule Rheo.Backend.RedisContractTest do
  use ExUnit.Case, async: false

  @moduletag :redis

  use Rheo.BackendContract,
    backend: Rheo.Backend.Redis,
    backend_opts: [url: Rheo.Test.Redis.url()]

  setup do
    on_exit(fn -> Rheo.Test.Redis.flush() end)
    :ok
  end
end
