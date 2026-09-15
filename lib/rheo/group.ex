defmodule Rheo.Group do
  @moduledoc """
  Local OTP coordinator for one `{instance, stream, group}` triple.

  Owns demand, fetch scheduling, inflight leases, renewals, and drain — **not**
  durable ACK truth. Competing nodes may each run a Group for the same durable
  group; the backend arbitrates leases.
  """
  use GenServer
  require Logger

  alias Rheo.{Lease, Telemetry}

  @min_backoff_ms 100
  @max_backoff_ms 5_000
  @drain_timeout_ms 5_000

  defstruct [
    :rheo,
    :stream,
    :group,
    :module,
    :max_demand,
    :concurrency,
    :lease_ms,
    :poll_ms,
    :consumer_id,
    :user_state,
    :renew_timer,
    inflight: %{},
    draining: false,
    backoff_ms: 0
  ]

  @doc false
  def child_spec(opts) do
    rheo = Keyword.fetch!(opts, :rheo)
    stream = Keyword.fetch!(opts, :stream)
    group = Keyword.fetch!(opts, :group)

    %{
      id: {__MODULE__, rheo, stream, group},
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      type: :worker,
      shutdown: @drain_timeout_ms + 1_000
    }
  end

  @doc false
  def start_link(opts) do
    rheo = Keyword.fetch!(opts, :rheo)
    stream = Keyword.fetch!(opts, :stream)
    group = Keyword.fetch!(opts, :group)
    GenServer.start_link(__MODULE__, opts, name: Rheo.Names.group(rheo, stream, group))
  end

  @doc """
  Asks a running group to stop fetching and await inflight work.
  """
  @spec drain(GenServer.server(), timeout()) :: :ok
  def drain(server, timeout \\ @drain_timeout_ms) do
    GenServer.call(server, {:drain, timeout}, timeout + 1_000)
  end

  @impl true
  def init(opts) do
    module = Keyword.fetch!(opts, :module)
    stream = Keyword.fetch!(opts, :stream)
    group = Keyword.fetch!(opts, :group)
    rheo = Keyword.fetch!(opts, :rheo)

    max_demand =
      Keyword.get(opts, :max_demand, Application.get_env(:rheo, :default_max_demand, 10))

    concurrency = Keyword.get(opts, :concurrency, 1)

    lease_ms =
      Keyword.get(opts, :lease_ms, Application.get_env(:rheo, :default_lease_ms, 30_000))

    poll_ms = Keyword.get(opts, :poll_ms, 200)
    consumer_id = Keyword.get(opts, :consumer_id) || Rheo.Id.generate()

    user_state =
      case maybe_setup(module, opts) do
        {:ok, state} -> state
        {:stop, reason} -> throw({:stop, reason})
      end

    state = %__MODULE__{
      rheo: rheo,
      stream: stream,
      group: group,
      module: module,
      max_demand: max_demand,
      concurrency: max(concurrency, 1),
      lease_ms: lease_ms,
      poll_ms: poll_ms,
      consumer_id: consumer_id,
      user_state: user_state
    }

    Telemetry.execute([:rheo, :consumer, :start], %{count: 1}, %{
      stream: stream,
      group: group,
      consumer_id: consumer_id
    })

    {:ok, schedule_renew(state), {:continue, :schedule}}
  catch
    {:stop, reason} -> {:stop, reason}
  end

  @impl true
  def handle_continue(:schedule, state), do: schedule_fetch(state)

  @impl true
  def handle_info(:fetch, state), do: do_fetch(state)

  def handle_info(:renew, state), do: do_renew(state)

  def handle_info({ref, outcome}, state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])
    handle_worker_result(ref, outcome, state)
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    handle_worker_down(ref, reason, state)
  end

  def handle_info(_other, state), do: {:noreply, state}

  @impl true
  def handle_call({:drain, timeout}, _from, state) do
    state = %{state | draining: true}
    reply = await_inflight(state, timeout)
    {:reply, reply, state}
  end

  @impl true
  def terminate(reason, state) do
    _ = cancel_timer(state.renew_timer)
    state = %{state | draining: true}

    # Best-effort wait; supervisor shutdown budget is the hard limit.
    _ = await_inflight(state, min(@drain_timeout_ms, 2_000))

    Telemetry.execute([:rheo, :consumer, :stop], %{count: 1}, %{
      stream: state.stream,
      group: state.group,
      consumer_id: state.consumer_id,
      reason: reason
    })

    :ok
  end

  defp maybe_setup(module, opts) do
    if function_exported?(module, :setup, 1) do
      module.setup(opts)
    else
      {:ok, %{}}
    end
  end

  defp schedule_fetch(%{draining: true} = state), do: {:noreply, state}

  defp schedule_fetch(state) do
    delay = if state.backoff_ms > 0, do: state.backoff_ms, else: 0
    Process.send_after(self(), :fetch, delay)
    {:noreply, state}
  end

  defp do_fetch(%{draining: true} = state), do: {:noreply, state}

  defp do_fetch(state) do
    slots = available_slots(state)

    if slots <= 0 do
      Process.send_after(self(), :fetch, state.poll_ms)
      {:noreply, state}
    else
      fetch_and_spawn(state, slots)
    end
  end

  defp available_slots(state) do
    inflight = map_size(state.inflight)
    demand_left = max(state.max_demand - inflight, 0)
    concurrency_left = max(state.concurrency - inflight, 0)
    min(demand_left, concurrency_left)
  end

  defp fetch_and_spawn(state, slots) do
    case Rheo.fetch(state.stream, state.group,
           rheo: state.rheo,
           limit: slots,
           consumer_id: state.consumer_id,
           lease_ms: state.lease_ms
         ) do
      {:ok, []} ->
        Process.send_after(self(), :fetch, state.poll_ms)
        {:noreply, %{state | backoff_ms: 0}}

      {:ok, leases} ->
        state = Enum.reduce(leases, state, &spawn_worker/2)
        send(self(), :fetch)
        {:noreply, %{state | backoff_ms: 0}}

      {:error, reason} ->
        emit_fetch_error(state, reason)
        backoff = next_backoff(state.backoff_ms)

        Logger.warning(
          "Rheo.Group fetch failed stream=#{state.stream} group=#{state.group} reason=#{inspect(reason)} backoff_ms=#{backoff}"
        )

        Process.send_after(self(), :fetch, backoff)
        {:noreply, %{state | backoff_ms: backoff}}
    end
  end

  defp spawn_worker(%Lease{} = lease, state) do
    user_state = state.user_state
    module = state.module

    task =
      Task.Supervisor.async_nolink(Rheo.Names.task_supervisor(state.rheo), fn ->
        try do
          module.handle_event(lease.event, user_state)
        rescue
          error -> {:handler_error, {error, __STACKTRACE__}}
        catch
          kind, reason -> {:handler_error, {{kind, reason}, __STACKTRACE__}}
        end
      end)

    put_in(state.inflight[task.ref], %{lease: lease, task: task})
  end

  defp handle_worker_result(ref, outcome, state) do
    case Map.pop(state.inflight, ref) do
      {nil, _} ->
        schedule_after_worker(state)

      {meta, inflight} ->
        state = %{state | inflight: inflight}
        state = apply_outcome(state, meta.lease, outcome)
        schedule_after_worker(state)
    end
  end

  defp handle_worker_down(ref, reason, state) do
    case Map.pop(state.inflight, ref) do
      {nil, _} ->
        {:noreply, state}

      {meta, inflight} ->
        state = %{state | inflight: inflight}
        Logger.warning("Rheo.Group worker crashed before settle: #{inspect(reason)}")

        Telemetry.execute([:rheo, :worker, :crash], %{count: 1}, %{
          stream: state.stream,
          group: state.group,
          event_id: meta.lease.event_id,
          reason: reason
        })

        # Leave lease to expire / redelivery — do not ACK
        schedule_after_worker(state)
    end
  end

  defp apply_outcome(state, lease, {:ack, new_state}) do
    case Rheo.ack(lease, rheo: state.rheo) do
      :ok ->
        %{state | user_state: new_state}

      {:error, reason} ->
        emit_persist_error(state, :ack, lease, reason)
        # Incomplete: treat as failed settle; leave for redelivery via nack attempt
        _ = Rheo.nack(lease, {:ack_failed, reason}, rheo: state.rheo)
        %{state | user_state: new_state}
    end
  end

  defp apply_outcome(state, lease, {:retry, reason, new_state}) do
    case Rheo.nack(lease, reason, rheo: state.rheo) do
      :ok ->
        %{state | user_state: new_state}

      {:error, err} ->
        emit_persist_error(state, :retry, lease, err)
        %{state | user_state: new_state}
    end
  end

  defp apply_outcome(state, lease, {:reject, reason, new_state}) do
    case Rheo.reject(lease, reason, rheo: state.rheo) do
      :ok ->
        %{state | user_state: new_state}

      {:error, err} ->
        emit_persist_error(state, :reject, lease, err)
        %{state | user_state: new_state}
    end
  end

  defp apply_outcome(state, lease, {:handler_error, detail}) do
    Logger.error("Rheo.Group handler error: #{inspect(detail)}")
    _ = Rheo.nack(lease, {:handler_error, detail}, rheo: state.rheo)
    state
  end

  defp apply_outcome(state, lease, other) do
    Logger.error("Rheo.Group invalid handler return: #{inspect(other)}")
    _ = Rheo.nack(lease, {:invalid_return, other}, rheo: state.rheo)
    state
  end

  defp schedule_after_worker(%{draining: true} = state), do: {:noreply, state}

  defp schedule_after_worker(state) do
    send(self(), :fetch)
    {:noreply, state}
  end

  defp do_renew(state) do
    state =
      Enum.reduce(state.inflight, state, fn {ref, meta}, acc ->
        case Rheo.renew(meta.lease, rheo: acc.rheo, lease_ms: acc.lease_ms) do
          {:ok, lease} ->
            Telemetry.execute([:rheo, :lease, :renew], %{count: 1}, %{
              stream: acc.stream,
              group: acc.group,
              event_id: lease.event_id,
              result: :ok
            })

            put_in(acc.inflight[ref].lease, lease)

          {:error, reason} ->
            Telemetry.execute([:rheo, :lease, :renew], %{count: 1}, %{
              stream: acc.stream,
              group: acc.group,
              event_id: meta.lease.event_id,
              result: reason
            })

            Logger.warning("Rheo.Group renew failed: #{inspect(reason)}")
            acc
        end
      end)

    {:noreply, schedule_renew(state)}
  end

  defp schedule_renew(state) do
    _ = cancel_timer(state.renew_timer)
    interval = max(div(state.lease_ms, 2), 50)
    ref = Process.send_after(self(), :renew, interval)
    %{state | renew_timer: ref}
  end

  defp cancel_timer(nil), do: :ok
  defp cancel_timer(ref), do: Process.cancel_timer(ref)

  defp await_inflight(state, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    wait_inflight(state, deadline)
  end

  defp wait_inflight(%{inflight: inflight}, _deadline) when map_size(inflight) == 0, do: :ok

  defp wait_inflight(state, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      :timeout
    else
      receive do
        {ref, outcome} when is_reference(ref) ->
          Process.demonitor(ref, [:flush])
          {:noreply, new_state} = handle_worker_result(ref, outcome, state)
          wait_inflight(new_state, deadline)

        {:DOWN, ref, :process, _pid, reason} ->
          {:noreply, new_state} = handle_worker_down(ref, reason, state)
          wait_inflight(new_state, deadline)

        :renew ->
          {:noreply, new_state} = do_renew(state)
          wait_inflight(new_state, deadline)

        :fetch ->
          wait_inflight(state, deadline)
      after
        max(remaining, 1) ->
          :timeout
      end
    end
  end

  defp next_backoff(0), do: @min_backoff_ms
  defp next_backoff(current), do: min(current * 2, @max_backoff_ms)

  defp emit_fetch_error(state, reason) do
    Telemetry.execute([:rheo, :fetch, :error], %{count: 1}, %{
      stream: state.stream,
      group: state.group,
      reason: reason
    })
  end

  defp emit_persist_error(state, op, lease, reason) do
    Telemetry.execute([:rheo, op, :error], %{count: 1}, %{
      stream: state.stream,
      group: state.group,
      event_id: lease.event_id,
      reason: reason
    })
  end
end
