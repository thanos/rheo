defmodule Rheo.Backend.Wakeup do
  @moduledoc """
  Optional hint that work may be available (ADR 025).

  Wakeup is never authoritative. Polling remains the fallback: a coordinator
  keeps its poll timer and treats a wakeup only as a reason to fetch sooner. A
  blocking backend call must never run inside `Rheo.Group` or `Rheo.Producer`;
  the coordinator runs `wait/3` in a reader task under the instance
  `Task.Supervisor`.

  A backend implements `c:wait/2` when it can block until work arrives
  (`XREADGROUP BLOCK`, `LISTEN/NOTIFY`, …) and declares `:blocking_reads` or
  `:notifications` in its capabilities. Backends that cannot block simply omit
  the callback, and `wait/3` returns `:ok` immediately.

  ## Options

  Coordinators pass hints the backend may use or ignore:

    * `:timeout` — milliseconds to block before returning (default backend-specific)
    * `:stream`, `:group`, `:partitions` — the assignment being waited on
  """

  @typedoc "Opaque backend connection handle."
  @type handle :: Rheo.Backend.handle()

  @doc """
  Blocks until work may be available for the given handle, or `opts[:timeout]`.

  Returns `:ok` both when work may exist and when the wait timed out; the caller
  must still fetch to learn the truth.
  """
  @callback wait(handle(), keyword()) :: :ok | {:error, term()}

  @optional_callbacks wait: 2

  @doc """
  Calls `c:wait/2` when `backend` implements it, otherwise returns `:ok`.

  Backends that omit `c:wait/2` (for example `Rheo.Backend.ETS`) never block:
  this function returns immediately so coordinators can keep a poll timer.

  ## Arguments

    * `backend` — backend module
    * `handle` — backend handle from `Rheo.Instance`
    * `opts` — hints passed through to the backend (`:timeout`, `:stream`,
      `:group`, `:partitions`)

  ## Examples

      iex> Rheo.Backend.Wakeup.wait(Rheo.Backend.ETS, :no_such_handle)
      :ok

      iex> Rheo.Backend.Wakeup.wait(Rheo.Backend.ETS, :no_such_handle, timeout: 50, stream: "orders")
      :ok

  ## Returns

  `:ok`, or `{:error, reason}` from the backend when `c:wait/2` is implemented.
  """
  @spec wait(module(), handle(), keyword()) :: :ok | {:error, term()}
  def wait(backend, handle, opts \\ []) when is_atom(backend) and is_list(opts) do
    if Code.ensure_loaded?(backend) and function_exported?(backend, :wait, 2) do
      backend.wait(handle, opts)
    else
      :ok
    end
  end
end
