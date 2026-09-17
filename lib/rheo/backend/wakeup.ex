defmodule Rheo.Backend.Wakeup do
  @moduledoc """
  Optional hint that work may be available (v0.8 / ADR 025).

  Wakeup is never authoritative. Runtimes must still `fetch/4` and honour
  leases. Missing or spurious wakeups are safe; polling remains the fallback.
  """

  @type handle :: Rheo.Backend.handle()

  @doc """
  Blocks until the backend believes work may be available, or `timeout` elapses.

  Default implementation returns `:ok` immediately (poll-only backends).
  """
  @callback wait(handle(), keyword()) :: :ok | {:error, term()}

  @optional_callbacks wait: 2

  @doc "Invokes `wait/2` when exported; otherwise returns `:ok`."
  @spec wait(module(), handle(), keyword()) :: :ok | {:error, term()}
  def wait(backend, handle, opts \\ []) when is_atom(backend) and is_list(opts) do
    if function_exported?(backend, :wait, 2) do
      backend.wait(handle, opts)
    else
      :ok
    end
  end
end
