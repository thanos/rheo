defmodule Rheo.Telemetry do
  @moduledoc """
  Thin helpers around `:telemetry` for Rheo operations.

  ## Events

  Backend spans (`:start` / `:stop` / `:exception` suffix, metadata `stream`
  and, where relevant, `group`):

    * `[:rheo, :append]`, `[:rheo, :append_batch]`, `[:rheo, :query]`,
      `[:rheo, :fetch]`, `[:rheo, :lease, :renew]`, `[:rheo, :ack]`,
      `[:rheo, :retry]`, `[:rheo, :reject]`

  Backend counters (`count` measurement):

    * `[:rheo, :stream, :create]`, `[:rheo, :group, :create]` — `stream`, `group`
    * `[:rheo, :lease]` — leases handed out by one fetch; `stream`, `group`, `consumer_id`
    * `[:rheo, :redelivery]`, `[:rheo, :dead_letter]` — `stream`, `group`, `event_id`
    * `[:rheo, :group, :frontier]`, `[:rheo, :group, :replay]`, `[:rheo, :group, :reset]`

  Runtime (`Rheo.Group`, `Rheo.Producer`, `Rheo.Broadway.Acknowledger`):

    * `[:rheo, :consumer, :start | :stop]`, `[:rheo, :producer, :start | :stop]`
      — `stream`, `group`, `consumer_id`
    * `[:rheo, :fetch, :error]` — `stream`, `group`, `reason` (`Rheo.Settle`)
    * `[:rheo, :lease, :renew]` — `stream`, `group`, `event_id`, `result`
      (`:ok` or a `Rheo.Settle` reason)
    * `[:rheo, :ack | :retry | :reject, :error]` — `stream`, `group`,
      `event_id`, `reason` (`Rheo.Settle`)
    * `[:rheo, :handler, :error]` — a handler raised or threw; `event_id`, `reason`
    * `[:rheo, :worker, :crash]` — a handler task exited; `event_id`, `reason`
    * `[:rheo, :broadway, :ack | :retry | :reject]` — settled through the acknowledger

  Metadata never includes event payloads. Attach handlers with
  `:telemetry.attach/4` in your application.
  """

  @doc """
  Executes `fun` while emitting start/stop (or exception) telemetry events.

  ## Arguments

    * `event` — event name prefix as a list of atoms, e.g. `[:rheo, :append]`
    * `metadata` — map attached to all emissions
    * `fun` — zero-arity function to run

  ## Examples

      iex> Rheo.Telemetry.span([:rheo, :doctest_span], %{demo: true}, fn -> :ok end)
      :ok

  ## Returns

  The return value of `fun`.

  ## Errors / raises

  Re-raises any exception, throw, or exit from `fun` after emitting an
  `:exception` event.
  """
  @spec span(list(atom()), map(), (-> result)) :: result when result: term()
  def span(event, metadata, fun) when is_function(fun, 0) do
    start = System.monotonic_time()
    :telemetry.execute(event ++ [:start], %{system_time: System.system_time()}, metadata)

    try do
      result = fun.()
      duration = System.monotonic_time() - start
      :telemetry.execute(event ++ [:stop], %{duration: duration}, Map.put(metadata, :result, :ok))
      result
    rescue
      error ->
        duration = System.monotonic_time() - start

        :telemetry.execute(
          event ++ [:exception],
          %{duration: duration},
          Map.merge(metadata, %{kind: :error, reason: error, stacktrace: __STACKTRACE__})
        )

        reraise error, __STACKTRACE__
    catch
      kind, reason ->
        duration = System.monotonic_time() - start

        :telemetry.execute(
          event ++ [:exception],
          %{duration: duration},
          Map.merge(metadata, %{kind: kind, reason: reason, stacktrace: __STACKTRACE__})
        )

        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  @doc """
  Emits a telemetry event with measurements and metadata.

  ## Arguments

    * `event` — full event name, e.g. `[:rheo, :lease]`
    * `measurements` — map of numeric measurements
    * `metadata` — map of contextual metadata

  ## Examples

      iex> Rheo.Telemetry.execute([:rheo, :doctest_execute], %{count: 1}, %{stream: "demo"})
      :ok

  ## Returns

  `:ok`
  """
  @spec execute(list(atom()), map(), map()) :: :ok
  def execute(event, measurements, metadata) do
    :telemetry.execute(event, measurements, metadata)
  end
end
