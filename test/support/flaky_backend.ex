defmodule Rheo.Backend.Flaky do
  @moduledoc false

  # ETS with injectable settle failures. `fail/2` makes the next calls of one
  # operation return `{:error, reason}` without touching durable state, which
  # is exactly what a dropped connection after a successful handler looks like.

  @behaviour Rheo.Backend

  alias Rheo.Backend.ETS

  @table :rheo_flaky_failures

  def fail(op, reason) when op in [:ack, :retry, :reject, :renew] do
    ensure_table()
    :ets.insert(@table, {op, reason})
    :ok
  end

  def clear do
    ensure_table()
    :ets.delete_all_objects(@table)
    :ok
  end

  defp ensure_table do
    if :ets.whereis(@table) == :undefined do
      :ets.new(@table, [:named_table, :public, :set])
    end
  end

  defp injected(op) do
    ensure_table()

    case :ets.lookup(@table, op) do
      [{^op, reason}] -> {:error, reason}
      [] -> nil
    end
  end

  @impl true
  def capabilities, do: ETS.capabilities()

  @impl true
  def child_spec(opts), do: ETS.child_spec(opts)

  @impl true
  defdelegate ping(handle), to: ETS
  @impl true
  defdelegate ensure_indexes(handle), to: ETS
  @impl true
  defdelegate create_stream(handle, stream, opts), to: ETS
  @impl true
  defdelegate create_group(handle, stream, group, opts), to: ETS
  @impl true
  defdelegate append(handle, stream, payload, opts), to: ETS
  @impl true
  defdelegate append_batch(handle, stream, payloads, opts), to: ETS
  @impl true
  defdelegate read(handle, stream, opts), to: ETS
  @impl true
  defdelegate query(handle, query), to: ETS
  @impl true
  defdelegate fetch(handle, stream, group, opts), to: ETS
  @impl true
  defdelegate replay(handle, stream, group, opts), to: ETS
  @impl true
  defdelegate reset_group(handle, stream, group, opts), to: ETS
  @impl true
  defdelegate lag(handle, stream, group, opts), to: ETS

  @impl true
  def renew(handle, lease, opts), do: injected(:renew) || ETS.renew(handle, lease, opts)

  @impl true
  def ack(handle, lease), do: injected(:ack) || ETS.ack(handle, lease)

  @impl true
  def retry(handle, lease, reason), do: injected(:retry) || ETS.retry(handle, lease, reason)

  @impl true
  def reject(handle, lease, reason), do: injected(:reject) || ETS.reject(handle, lease, reason)
end
