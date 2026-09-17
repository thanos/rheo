defmodule Rheo.Producer do
  @moduledoc """
  `GenStage` producer that turns demand into Rheo leases.

  `Rheo.Producer` is the **demand-driven** consumption surface for a durable
  consumer group. It is an alternative to `Rheo.Consumer` / `Rheo.Group`, not a
  layer on top of them: the producer owns fetch and lease renewal, while the
  downstream pipeline owns concurrency and settles each lease through an
  acknowledger. Do not point both a `Rheo.Consumer` and a `Rheo.Producer` at the
  same `{rheo, stream, group}` unless you intend competing consumers.

  Emitted events are `%Rheo.Lease{}` structs, so plain GenStage consumers can
  pattern match on the lease and call `Rheo.ack/2`, `Rheo.nack/3`, or
  `Rheo.reject/3` themselves. For Broadway, add
  `transformer: {Rheo.Broadway, :transform, []}` and the lease is wrapped into a
  `%Broadway.Message{}` whose acknowledger settles it (see `Rheo.Broadway`).

  ## Options

    * `:stream` — required stream name
    * `:group` — required consumer group
    * `:rheo` — instance name (default `Rheo`)
    * `:max_demand` — maximum unsettled leases held at once (default from
      `:default_max_demand`, typically `10`). This bounds leases, not pipeline
      concurrency, which Broadway's `:concurrency` owns.
    * `:lease_ms` — lease TTL; inflight leases are renewed every `lease_ms / 2`
    * `:poll_ms` — idle poll interval when demand outruns available work
      (default `200`)
    * `:consumer_id` — worker identity recorded on each lease (default: generated)
    * `:partitions` — `:all` (default) or a list of partition ids to fetch from
    * `:on_failure` — `:nack` (default) or `:reject`, used by
      `Rheo.Broadway.Acknowledger` when a message fails
    * `:name` — optional process name (ignored under Broadway, which names the
      producer stage itself)

  ## Broadway

      producer: [
        module: {Rheo.Producer, rheo: MyRheo, stream: "market-events", group: "risk"},
        transformer: {Rheo.Broadway, :transform, []},
        concurrency: 1
      ]

  ## GenStage

      {:ok, producer} =
        Rheo.Producer.start_link(rheo: MyRheo, stream: "market-events", group: "risk")

      # In a consumer's handle_events/3:
      def handle_events(leases, _from, state) do
        Enum.each(leases, fn lease ->
          :ok = Risk.process(lease.event)
          :ok = Rheo.ack(lease, rheo: MyRheo)
          Rheo.Producer.confirm(state.producer, lease.lease_id)
        end)

        {:noreply, [], state}
      end

  Settling a lease is only half the job: the producer must also be told, with
  `confirm/2`, so it stops renewing the lease and frees a `:max_demand` slot.
  `Rheo.Broadway.Acknowledger` does this for you.

  ## Backpressure and renewal

  Each `handle_demand/2` accumulates demand and fetches at most
  `min(demand, max_demand - inflight)` leases. When the backend returns fewer
  leases than asked for, the producer polls every `:poll_ms`; when a fetch fails
  it backs off exponentially (100 ms → 5 s) and emits
  `[:rheo, :fetch, :error]`. Inflight leases are renewed on a timer and emit
  `[:rheo, :lease, :renew]`; a lease that has gone stale is dropped so the
  backend can redeliver it (at-least-once).

  See ADR 018 and `Rheo.Broadway`.
  """

  use GenStage
  @behaviour Broadway.Producer

  require Logger

  alias Rheo.{Inflight, Lease, Settle, Telemetry}

  @config_key {__MODULE__, :config}
  @min_backoff_ms 100
  @max_backoff_ms 5_000
  @drain_timeout_ms 5_000

  defstruct [
    :rheo,
    :stream,
    :group,
    :max_demand,
    :lease_ms,
    :poll_ms,
    :consumer_id,
    :partitions,
    :on_failure,
    :renew_timer,
    :poll_timer,
    inflight: Inflight.new(),
    pending: 0,
    draining: false,
    backoff_ms: 0
  ]

  @doc """
  Starts a producer.

  Accepts the options documented in the module doc. `:name`, when given, is
  passed through to `GenStage.start_link/3`.

  ## Returns

    * `{:ok, pid}`
    * `{:error, {:already_started, pid}}`
    * `{:error, reason}`
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    {start_opts, opts} = Keyword.split(opts, [:name])
    GenStage.start_link(__MODULE__, opts, start_opts)
  end

  @doc """
  Tells the producer that leases have been settled durably.

  Removes them from the inflight set so renewal stops and `:max_demand` capacity
  is released. Accepts one `lease_id` or a list. Asynchronous.
  """
  @spec confirm(GenServer.server(), String.t() | [String.t()]) :: :ok
  def confirm(producer, lease_id) when is_binary(lease_id), do: confirm(producer, [lease_id])

  def confirm(producer, lease_ids) when is_list(lease_ids) do
    GenStage.cast(producer, {:confirm, lease_ids})
  end

  @doc """
  Stops fetching and waits until inflight leases are settled or `timeout` elapses.

  Returns `:ok` when nothing is inflight, `:timeout` otherwise. Broadway calls
  `prepare_for_draining/1` on its own during shutdown; this is the equivalent for
  plain GenStage pipelines.
  """
  @spec drain(GenServer.server(), timeout()) :: :ok | :timeout
  def drain(producer, timeout \\ @drain_timeout_ms) do
    GenStage.call(producer, {:drain, timeout}, timeout + 1_000)
  end

  @doc """
  Returns the number of leases the producer holds but has not seen confirmed.
  """
  @spec inflight_count(GenServer.server()) :: non_neg_integer()
  def inflight_count(producer), do: GenStage.call(producer, :inflight_count)

  @doc """
  Returns the configuration of the producer running in the **calling** process.

  Returns `nil` outside a producer process. Broadway invokes the transformer
  inside the producer, so `Rheo.Broadway.transform/2` uses this to pick up
  `:rheo` and `:on_failure` without repeating them in the transformer arguments.
  """
  @spec config() ::
          %{
            required(:rheo) => atom(),
            required(:stream) => Rheo.stream(),
            required(:group) => Rheo.group(),
            required(:on_failure) => :nack | :reject
          }
          | nil
  def config, do: Process.get(@config_key)

  @impl true
  def init(opts) when is_list(opts) do
    opts = Keyword.drop(opts, [:broadway])
    rheo = Keyword.get(opts, :rheo, Rheo)
    stream = Keyword.fetch!(opts, :stream)
    group = Keyword.fetch!(opts, :group)
    on_failure = validate_on_failure(Keyword.get(opts, :on_failure, :nack))

    state = %__MODULE__{
      rheo: rheo,
      stream: stream,
      group: group,
      max_demand:
        Keyword.get(opts, :max_demand, Application.get_env(:rheo, :default_max_demand, 10)),
      lease_ms:
        Keyword.get(opts, :lease_ms, Application.get_env(:rheo, :default_lease_ms, 30_000)),
      poll_ms: Keyword.get(opts, :poll_ms, 200),
      consumer_id: Keyword.get(opts, :consumer_id) || Rheo.Id.generate(),
      partitions: Keyword.get(opts, :partitions, :all),
      on_failure: on_failure
    }

    Process.put(@config_key, %{
      rheo: rheo,
      stream: stream,
      group: group,
      on_failure: on_failure
    })

    Telemetry.execute([:rheo, :producer, :start], %{count: 1}, %{
      stream: stream,
      group: group,
      consumer_id: state.consumer_id
    })

    {:producer, schedule_renew(state)}
  end

  @impl true
  def handle_demand(demand, state) when demand >= 0 do
    dispatch(%{state | pending: state.pending + demand})
  end

  @impl true
  def handle_cast({:confirm, lease_ids}, state) do
    state = %{state | inflight: Inflight.drop(state.inflight, lease_ids)}
    dispatch(state)
  end

  def handle_cast(_other, state), do: {:noreply, [], state}

  @impl true
  def handle_call({:drain, timeout}, _from, state) do
    {reply, state} = state |> stop_fetching() |> await_inflight(timeout)
    {:reply, reply, [], state}
  end

  def handle_call(:inflight_count, _from, state) do
    {:reply, Inflight.size(state.inflight), [], state}
  end

  @impl true
  def handle_info(:poll, state), do: dispatch(%{state | poll_timer: nil})

  def handle_info(:renew, state) do
    state
    |> renew_inflight()
    |> schedule_renew()
    |> dispatch()
  end

  def handle_info(_other, state), do: {:noreply, [], state}

  @impl Broadway.Producer
  def prepare_for_draining(state), do: {:noreply, [], stop_fetching(state)}

  @impl true
  def terminate(reason, state) do
    state = state |> stop_fetching() |> cancel_renew()
    {_result, _state} = await_inflight(state, min(@drain_timeout_ms, 2_000))

    Telemetry.execute([:rheo, :producer, :stop], %{count: 1}, %{
      stream: state.stream,
      group: state.group,
      consumer_id: state.consumer_id,
      reason: reason
    })

    :ok
  end

  defp validate_on_failure(on_failure) when on_failure in [:nack, :reject], do: on_failure

  defp validate_on_failure(other) do
    raise ArgumentError,
          "Rheo.Producer :on_failure must be :nack or :reject, got: #{inspect(other)}"
  end

  defp dispatch(%__MODULE__{draining: true} = state), do: {:noreply, [], state}

  defp dispatch(%__MODULE__{} = state) do
    case fetch_limit(state) do
      0 -> {:noreply, [], maybe_poll(state)}
      limit -> fetch_and_emit(state, limit)
    end
  end

  defp fetch_limit(state) do
    state.pending
    |> min(Inflight.capacity(state.inflight, state.max_demand))
    |> max(0)
  end

  # Demand is waiting but the inflight bound is reached: poll so a lost
  # `confirm/2` cannot wedge the producer permanently.
  defp maybe_poll(%{pending: pending} = state) when pending > 0, do: schedule_poll(state)
  defp maybe_poll(state), do: state

  defp fetch_and_emit(state, limit) do
    case Rheo.fetch(state.stream, state.group, fetch_opts(state, limit)) do
      {:ok, []} ->
        {:noreply, [], schedule_poll(%{state | backoff_ms: 0})}

      {:ok, leases} ->
        state = %{state | backoff_ms: 0, pending: state.pending - length(leases)}
        state = Enum.reduce(leases, state, &track_inflight/2)
        state = if length(leases) < limit, do: schedule_poll(state), else: state
        {:noreply, leases, state}

      {:error, reason} ->
        {:noreply, [], backoff(state, reason)}
    end
  end

  defp fetch_opts(state, limit) do
    opts = [
      rheo: state.rheo,
      limit: limit,
      consumer_id: state.consumer_id,
      lease_ms: state.lease_ms
    ]

    case state.partitions do
      :all -> opts
      nil -> opts
      partitions -> Keyword.put(opts, :partitions, partitions)
    end
  end

  defp track_inflight(%Lease{} = lease, state) do
    %{state | inflight: Inflight.put(state.inflight, lease.lease_id, lease)}
  end

  defp drop_inflight(state, lease_ids) do
    %{state | inflight: Inflight.drop(state.inflight, lease_ids)}
  end

  defp backoff(state, reason) do
    Telemetry.execute([:rheo, :fetch, :error], %{count: 1}, %{
      stream: state.stream,
      group: state.group,
      reason: Settle.classify(reason)
    })

    delay = next_backoff(state.backoff_ms)

    Logger.warning(
      "Rheo.Producer fetch failed stream=#{state.stream} group=#{state.group} " <>
        "reason=#{inspect(reason)} backoff_ms=#{delay}"
    )

    schedule_poll(%{state | backoff_ms: delay}, delay)
  end

  defp next_backoff(0), do: @min_backoff_ms
  defp next_backoff(current), do: min(current * 2, @max_backoff_ms)

  defp renew_inflight(state) do
    Enum.reduce(state.inflight, state, fn {lease_id, meta}, acc ->
      lease = meta.lease

      case Rheo.renew(lease, rheo: acc.rheo, lease_ms: acc.lease_ms) do
        {:ok, renewed} ->
          emit_renew(acc, lease.event_id, :ok)
          %{acc | inflight: Inflight.update_lease(acc.inflight, lease_id, renewed)}

        {:error, reason} ->
          classified = Settle.classify(reason)
          emit_renew(acc, lease.event_id, classified)

          if Settle.lost?(reason) do
            Logger.warning(
              "Rheo.Producer dropping stale lease stream=#{acc.stream} " <>
                "group=#{acc.group} event_id=#{lease.event_id}"
            )

            drop_inflight(acc, [lease_id])
          else
            Logger.warning("Rheo.Producer renew failed: #{inspect(reason)}")
            acc
          end
      end
    end)
  end

  defp emit_renew(state, event_id, result) do
    Telemetry.execute([:rheo, :lease, :renew], %{count: 1}, %{
      stream: state.stream,
      group: state.group,
      event_id: event_id,
      result: result
    })
  end

  defp schedule_renew(state) do
    state = cancel_renew(state)
    interval = max(div(state.lease_ms, 2), 50)
    %{state | renew_timer: Process.send_after(self(), :renew, interval)}
  end

  defp cancel_renew(%{renew_timer: nil} = state), do: state

  defp cancel_renew(%{renew_timer: ref} = state) do
    _ = Process.cancel_timer(ref)
    %{state | renew_timer: nil}
  end

  defp schedule_poll(state), do: schedule_poll(state, state.poll_ms)

  defp schedule_poll(%{draining: true} = state, _delay), do: state

  defp schedule_poll(state, delay) do
    state = cancel_poll(state)
    %{state | poll_timer: Process.send_after(self(), :poll, delay)}
  end

  defp cancel_poll(%{poll_timer: nil} = state), do: state

  defp cancel_poll(%{poll_timer: ref} = state) do
    _ = Process.cancel_timer(ref)
    %{state | poll_timer: nil}
  end

  defp stop_fetching(state), do: %{cancel_poll(state) | draining: true}

  defp await_inflight(state, timeout) do
    wait_inflight(state, System.monotonic_time(:millisecond) + timeout)
  end

  defp wait_inflight(%{inflight: inflight} = state, _deadline) when map_size(inflight) == 0 do
    {:ok, state}
  end

  defp wait_inflight(state, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      {:timeout, state}
    else
      receive_settle(state, deadline, remaining)
    end
  end

  # Callbacks run in the producer process, so settle notifications and the renew
  # timer are drained straight from its mailbox while waiting.
  defp receive_settle(state, deadline, remaining) do
    receive do
      {:"$gen_cast", {:confirm, lease_ids}} ->
        state
        |> drop_inflight(lease_ids)
        |> wait_inflight(deadline)

      :renew ->
        state
        |> renew_inflight()
        |> schedule_renew()
        |> wait_inflight(deadline)

      :poll ->
        wait_inflight(%{state | poll_timer: nil}, deadline)
    after
      max(remaining, 1) -> {:timeout, state}
    end
  end
end
