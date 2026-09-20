defmodule Rheo.Group do
  @moduledoc """
  Local OTP coordinator for one `{instance, stream, group}` triple.

  Owns demand, fetch scheduling, inflight leases, renewals, and drain — not
  durable ACK truth. Competing nodes may each run a Group for the same durable
  group; the backend arbitrates leases.

  `use Rheo.Consumer` produces a child spec for this process, so a consumer's
  Group lives in the host application's supervision tree. Exactly one Group per
  `{rheo, stream, group}` may run on a node; a second start returns
  `{:error, {:already_started, pid}}`.

  Optional `:partitions` (`:all` or a list of ids) scopes fetch to a static
  assignment. Automatic rebalancing is not implemented (ADR 016).
  """
  use GenServer
  require Logger

  alias Rheo.Backend.Wakeup
  alias Rheo.{Backoff, Inflight, Instance, Lease, Settle, Telemetry}

  @drain_timeout_ms 5_000
  @wakeup_floor_ms 25

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
    :context,
    :renew_timer,
    :poll_timer,
    :partitions,
    :drain_from,
    :drain_timer,
    inflight: %{},
    draining: false,
    backoff_ms: 0
  ]

  @doc """
  Milliseconds the supervisor should wait for `terminate/2` to drain inflight work.
  """
  @spec shutdown_ms() :: pos_integer()
  def shutdown_ms, do: @drain_timeout_ms + 1_000

  @doc false
  def child_spec(opts) do
    rheo = Keyword.fetch!(opts, :rheo)
    stream = Keyword.fetch!(opts, :stream)
    group = Keyword.fetch!(opts, :group)

    %{
      id: Keyword.get(opts, :id, {__MODULE__, rheo, stream, group}),
      start: {__MODULE__, :start_link, [Keyword.delete(opts, :id)]},
      restart: :permanent,
      type: :worker,
      shutdown: shutdown_ms()
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
  Stops fetching and waits until inflight work settles or `timeout` elapses.

  Returns `:ok` when nothing is inflight, `:timeout` otherwise, or
  `{:error, :already_draining}` if a drain is already pending. The group keeps
  serving renewals and worker results while a drain is pending.
  """
  @spec drain(GenServer.server(), timeout()) :: :ok | :timeout | {:error, :already_draining}
  def drain(server, timeout \\ @drain_timeout_ms) do
    GenServer.call(server, {:drain, timeout}, timeout + 1_000)
  end

  @impl true
  def init(opts) do
    module = Keyword.fetch!(opts, :module)

    case maybe_setup(module, opts) do
      {:ok, context} when is_map(context) ->
        Process.flag(:trap_exit, true)
        state = build_state(opts, module, context)

        Telemetry.execute([:rheo, :consumer, :start], %{count: 1}, %{
          stream: state.stream,
          group: state.group,
          consumer_id: state.consumer_id
        })

        {:ok, schedule_renew(state), {:continue, :schedule}}

      {:ok, other} ->
        {:stop, {:invalid_context, other}}

      {:stop, reason} ->
        {:stop, reason}
    end
  end

  defp build_state(opts, module, context) do
    %__MODULE__{
      rheo: Keyword.fetch!(opts, :rheo),
      stream: Keyword.fetch!(opts, :stream),
      group: Keyword.fetch!(opts, :group),
      module: module,
      max_demand:
        Keyword.get(opts, :max_demand, Application.get_env(:rheo, :default_max_demand, 10)),
      concurrency: max(Keyword.get(opts, :concurrency, 1), 1),
      lease_ms:
        Keyword.get(opts, :lease_ms, Application.get_env(:rheo, :default_lease_ms, 30_000)),
      poll_ms: Keyword.get(opts, :poll_ms, 200),
      consumer_id: Keyword.get(opts, :consumer_id) || Rheo.Id.generate(),
      partitions: Keyword.get(opts, :partitions, :all),
      context: context,
      inflight: Inflight.new()
    }
  end

  @impl true
  def handle_continue(:schedule, state) do
    maybe_start_wakeup(state)
    {:noreply, schedule_poll(state, state.backoff_ms)}
  end

  @impl true
  def handle_info(:fetch, state), do: do_fetch(%{state | poll_timer: nil})

  def handle_info(:renew, state), do: {:noreply, do_renew(state)}

  def handle_info(:drain_timeout, state), do: {:noreply, finish_drain(state, :timeout)}

  def handle_info({ref, outcome}, state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])
    {:noreply, handle_worker_result(ref, outcome, state)}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    {:noreply, handle_worker_down(ref, reason, state)}
  end

  # Exits are trapped so a supervisor shutdown drains inflight work in
  # terminate/2; a linked process dying still stops the group.
  def handle_info({:EXIT, _pid, reason}, state), do: {:stop, reason, state}

  def handle_info(_other, state), do: {:noreply, state}

  @impl true
  def handle_call({:drain, _timeout}, _from, %{drain_from: from} = state) when not is_nil(from) do
    {:reply, {:error, :already_draining}, state}
  end

  def handle_call({:drain, timeout}, from, state) do
    state = stop_fetching(state)

    if Inflight.size(state.inflight) == 0 do
      {:reply, :ok, state}
    else
      timer = Process.send_after(self(), :drain_timeout, timeout)
      {:noreply, %{state | drain_from: from, drain_timer: timer}}
    end
  end

  @impl true
  def terminate(reason, state) do
    _ = cancel_timer(state.renew_timer)
    state = stop_fetching(state)

    # Best-effort wait; the child spec's shutdown budget is the hard limit.
    _ = await_inflight(state, @drain_timeout_ms)

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

  # ADR 025: a backend that can block waits in its own task and only hints at
  # work. The poll timer below stays the source of truth for liveness.
  defp maybe_start_wakeup(state) do
    case instance(state.rheo) do
      %Instance{backend: backend, handle: handle} ->
        if Code.ensure_loaded?(backend) and function_exported?(backend, :wait, 2) do
          start_wakeup_task(state, backend, handle)
        end

      nil ->
        :ok
    end

    :ok
  end

  defp instance(rheo) do
    Instance.fetch!(rheo)
  catch
    :exit, _reason -> nil
  end

  defp start_wakeup_task(state, backend, handle) do
    group = self()

    opts = [
      timeout: state.lease_ms |> div(2) |> min(state.poll_ms * 5) |> max(state.poll_ms),
      stream: state.stream,
      group: state.group,
      partitions: state.partitions
    ]

    Task.Supervisor.start_child(Rheo.Names.task_supervisor(state.rheo), fn ->
      wakeup_loop(backend, handle, group, opts)
    end)
  end

  defp wakeup_loop(backend, handle, group, opts) do
    if Process.alive?(group) do
      _ = Wakeup.wait(backend, handle, opts)

      # A floor between hints keeps a permanent backlog from spinning the task
      # when the group has no free slots.
      Process.sleep(@wakeup_floor_ms)

      if Process.alive?(group) do
        send(group, :fetch)
        wakeup_loop(backend, handle, group, opts)
      end
    end

    :ok
  end

  defp schedule_poll(%{draining: true} = state, _delay), do: state

  defp schedule_poll(state, delay) do
    state = cancel_poll(state)
    %{state | poll_timer: Process.send_after(self(), :fetch, delay)}
  end

  defp cancel_poll(%{poll_timer: nil} = state), do: state

  defp cancel_poll(%{poll_timer: ref} = state) do
    _ = Process.cancel_timer(ref)
    %{state | poll_timer: nil}
  end

  defp stop_fetching(state), do: %{cancel_poll(state) | draining: true}

  defp do_fetch(%{draining: true} = state), do: {:noreply, state}

  defp do_fetch(state) do
    slots = available_slots(state)

    if slots <= 0 do
      {:noreply, schedule_poll(state, state.poll_ms)}
    else
      fetch_and_spawn(state, slots)
    end
  end

  defp available_slots(state) do
    demand_left = Inflight.capacity(state.inflight, state.max_demand)
    concurrency_left = max(state.concurrency - Inflight.size(state.inflight), 0)
    min(demand_left, concurrency_left)
  end

  defp fetch_and_spawn(state, slots) do
    fetch_opts =
      [
        rheo: state.rheo,
        limit: slots,
        consumer_id: state.consumer_id,
        lease_ms: state.lease_ms
      ]
      |> maybe_put_partitions(state.partitions)

    case Rheo.fetch(state.stream, state.group, fetch_opts) do
      {:ok, []} ->
        {:noreply, schedule_poll(%{state | backoff_ms: 0}, state.poll_ms)}

      {:ok, leases} ->
        state = Enum.reduce(leases, state, &spawn_worker/2)
        {:noreply, schedule_poll(%{state | backoff_ms: 0}, 0)}

      {:error, reason} ->
        reason = Settle.classify(reason)
        emit_fetch_error(state, reason)
        backoff = Backoff.next(state.backoff_ms)

        Logger.warning(
          "Rheo.Group fetch failed stream=#{state.stream} group=#{state.group} " <>
            "reason=#{inspect(reason)} backoff_ms=#{backoff}"
        )

        {:noreply, schedule_poll(%{state | backoff_ms: backoff}, backoff)}
    end
  end

  defp maybe_put_partitions(opts, :all), do: opts
  defp maybe_put_partitions(opts, nil), do: opts
  defp maybe_put_partitions(opts, partitions), do: Keyword.put(opts, :partitions, partitions)

  defp spawn_worker(%Lease{} = lease, state) do
    context = state.context
    module = state.module

    task =
      Task.Supervisor.async_nolink(Rheo.Names.task_supervisor(state.rheo), fn ->
        try do
          module.handle_event(lease.event, context)
        rescue
          error -> {:handler_error, {error, __STACKTRACE__}}
        catch
          kind, reason -> {:handler_error, {{kind, reason}, __STACKTRACE__}}
        end
      end)

    %{state | inflight: Inflight.put(state.inflight, task.ref, lease, %{task: task})}
  end

  defp handle_worker_result(ref, outcome, state) do
    case Inflight.pop(state.inflight, ref) do
      :error ->
        after_worker(state)

      {:ok, %{lease: lease}, inflight} ->
        %{state | inflight: inflight}
        |> apply_outcome(lease, outcome)
        |> after_worker()
    end
  end

  defp handle_worker_down(ref, reason, state) do
    case Inflight.pop(state.inflight, ref) do
      :error ->
        state

      {:ok, %{lease: lease}, inflight} ->
        Logger.warning(
          "Rheo.Group worker crashed before settle stream=#{state.stream} " <>
            "group=#{state.group} event_id=#{lease.event_id} reason=#{inspect(reason)}"
        )

        Telemetry.execute([:rheo, :worker, :crash], %{count: 1}, %{
          stream: state.stream,
          group: state.group,
          event_id: lease.event_id,
          reason: reason
        })

        # The lease is left to expire so the backend redelivers it.
        after_worker(%{state | inflight: inflight})
    end
  end

  defp apply_outcome(state, lease, :ack) do
    case Rheo.ack(lease, rheo: state.rheo) do
      :ok ->
        state

      {:error, reason} ->
        classified = Settle.classify(reason)
        emit_persist_error(state, :ack, lease, classified)

        if Settle.nack_after_failed_ack?(classified) do
          _ = Rheo.nack(lease, {:ack_failed, classified}, rheo: state.rheo)
        end

        state
    end
  end

  defp apply_outcome(state, lease, {:retry, reason}) do
    case Rheo.nack(lease, reason, rheo: state.rheo) do
      :ok -> state
      {:error, err} -> emit_persist_error(state, :retry, lease, Settle.classify(err))
    end
  end

  defp apply_outcome(state, lease, {:reject, reason}) do
    case Rheo.reject(lease, reason, rheo: state.rheo) do
      :ok -> state
      {:error, err} -> emit_persist_error(state, :reject, lease, Settle.classify(err))
    end
  end

  defp apply_outcome(state, lease, {:handler_error, {error, stacktrace}}) do
    Logger.error(
      "Rheo.Group handler raised stream=#{state.stream} group=#{state.group} " <>
        "event_id=#{lease.event_id}: #{format_error(error, stacktrace)}"
    )

    Telemetry.execute([:rheo, :handler, :error], %{count: 1}, %{
      stream: state.stream,
      group: state.group,
      event_id: lease.event_id,
      reason: error
    })

    _ = Rheo.nack(lease, {:handler_error, error}, rheo: state.rheo)
    state
  end

  defp apply_outcome(state, lease, other) do
    Logger.error(
      "Rheo.Group invalid handler outcome stream=#{state.stream} group=#{state.group} " <>
        "event_id=#{lease.event_id}: expected :ack | {:retry, reason} | {:reject, reason}"
    )

    _ = Rheo.nack(lease, {:invalid_outcome, other}, rheo: state.rheo)
    state
  end

  defp format_error({kind, reason}, stacktrace) when kind in [:throw, :exit] do
    Exception.format(kind, reason, stacktrace)
  end

  defp format_error(error, stacktrace), do: Exception.format(:error, error, stacktrace)

  defp after_worker(%{draining: true} = state), do: maybe_finish_drain(state)

  defp after_worker(state), do: schedule_poll(state, 0)

  defp maybe_finish_drain(%{drain_from: nil} = state), do: state

  defp maybe_finish_drain(state) do
    if Inflight.size(state.inflight) == 0, do: finish_drain(state, :ok), else: state
  end

  defp finish_drain(%{drain_from: nil} = state, _reply), do: state

  defp finish_drain(state, reply) do
    _ = cancel_timer(state.drain_timer)
    GenServer.reply(state.drain_from, reply)
    %{state | drain_from: nil, drain_timer: nil}
  end

  defp do_renew(state) do
    {inflight, results} =
      Inflight.renew_all(
        state.inflight,
        &Rheo.renew(&1, rheo: state.rheo, lease_ms: state.lease_ms)
      )

    Enum.each(results, fn {_key, lease, result} ->
      Telemetry.execute([:rheo, :lease, :renew], %{count: 1}, %{
        stream: state.stream,
        group: state.group,
        event_id: lease.event_id,
        result: result
      })

      if result != :ok do
        Logger.warning(
          "Rheo.Group renew failed stream=#{state.stream} group=#{state.group} " <>
            "event_id=#{lease.event_id} reason=#{inspect(result)}"
        )
      end
    end)

    %{state | inflight: inflight}
    |> maybe_finish_drain()
    |> schedule_renew()
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
    wait_inflight(state, System.monotonic_time(:millisecond) + timeout)
  end

  defp wait_inflight(%{inflight: inflight}, _deadline) when map_size(inflight) == 0, do: :ok

  # Runs only inside terminate/2, where blocking the process is acceptable.
  defp wait_inflight(state, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      :timeout
    else
      receive do
        {ref, outcome} when is_reference(ref) ->
          Process.demonitor(ref, [:flush])
          wait_inflight(handle_worker_result(ref, outcome, state), deadline)

        {:DOWN, ref, :process, _pid, reason} ->
          wait_inflight(handle_worker_down(ref, reason, state), deadline)

        :renew ->
          wait_inflight(do_renew(state), deadline)

        :fetch ->
          wait_inflight(state, deadline)
      after
        max(remaining, 1) -> :timeout
      end
    end
  end

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

    state
  end
end
