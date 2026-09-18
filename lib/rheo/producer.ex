# Optional integration: compiled only when `:gen_stage` is present.
if Code.ensure_loaded?(GenStage) do
  defmodule Rheo.Producer do
    @moduledoc """
    `GenStage` producer that turns demand into Rheo leases.

    `Rheo.Producer` is the demand-driven consumption surface for a durable
    consumer group. It is an alternative to `Rheo.Consumer` / `Rheo.Group`, not a
    layer on top of them: the producer owns fetch and lease renewal, while the
    downstream pipeline owns concurrency and settles each lease. Do not point both
    a `Rheo.Consumer` and a `Rheo.Producer` at the same `{rheo, stream, group}`
    unless you intend competing consumers.

    Emitted events are `%Rheo.Lease{}` structs. For Broadway, add
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
            case Risk.process(lease.event) do
              :ok -> :ok = Rheo.Producer.ack(state.producer, lease, rheo: MyRheo)
              {:error, reason} -> :ok = Rheo.Producer.nack(state.producer, lease, reason, rheo: MyRheo)
            end
          end)

          {:noreply, [], state}
        end

    ## Settlement boundary

    A lease held by a pipeline has two pieces of state: the durable delivery in
    the backend and the producer's inflight entry that drives renewal and
    `:max_demand`. `ack/3`, `nack/4`, and `reject/4` settle both in one call:
    they run the durable settle and then release the inflight entry, whatever the
    settle result. A fenced or unavailable settle leaves the lease to expire and
    redeliver (at-least-once), so the producer must not keep renewing it.

    `confirm/2` is the lower-level half for callers that settle with `Rheo.ack/2`
    directly. Flow pipelines built on `Flow.from_stages/2` use the same three
    helpers from `on_trigger` once an aggregate is durable — see the Flow
    readiness spike.

    ## Backpressure and renewal

    Each `handle_demand/2` accumulates demand and fetches at most
    `min(demand, max_demand - inflight)` leases. When the backend returns fewer
    leases than asked for, the producer polls every `:poll_ms`; when a fetch fails
    it backs off exponentially (`Rheo.Backoff`) and emits
    `[:rheo, :fetch, :error]`. Inflight leases are renewed on a timer and emit
    `[:rheo, :lease, :renew]`; a lease that has gone stale is dropped so the
    backend can redeliver it.

    Requires `{:gen_stage, "~> 1.2"}` in your dependencies; Broadway's
    `prepare_for_draining/1` is implemented when `{:broadway, "~> 1.2"}` is present.

    See ADR 018 and `Rheo.Broadway`.
    """

    use GenStage

    if Code.ensure_loaded?(Broadway.Producer), do: @behaviour(Broadway.Producer)

    require Logger

    alias Rheo.{Backoff, Inflight, Lease, Settle, Telemetry}

    @config_key {__MODULE__, :config}
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
      :drain_from,
      :drain_timer,
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
    Acknowledges `lease` durably and releases it from the producer.

    ## Arguments

      * `producer` — the producer that emitted the lease
      * `lease` — the `%Rheo.Lease{}`
      * `opts` — `:rheo` instance name (default `Rheo`)

    ## Returns

    The result of `Rheo.ack/2`. The inflight entry is released even on error;
    see "Settlement boundary".
    """
    @spec ack(GenServer.server(), Lease.t(), keyword()) :: :ok | {:error, term()}
    def ack(producer, %Lease{} = lease, opts \\ []) do
      settle(producer, lease, fn -> Rheo.ack(lease, opts) end)
    end

    @doc """
    Returns `lease` for retry (or dead-letters it at `max_attempts`) and releases
    it from the producer.

    ## Arguments

      * `producer` — the producer that emitted the lease
      * `lease` — the `%Rheo.Lease{}`
      * `reason` — stored for diagnostics (default `:retry`)
      * `opts` — `:rheo` instance name (default `Rheo`)

    ## Returns

    The result of `Rheo.nack/3`.
    """
    @spec nack(GenServer.server(), Lease.t(), term(), keyword()) :: :ok | {:error, term()}
    def nack(producer, %Lease{} = lease, reason \\ :retry, opts \\ []) do
      settle(producer, lease, fn -> Rheo.nack(lease, reason, opts) end)
    end

    @doc """
    Dead-letters `lease` for its group and releases it from the producer.

    ## Arguments

      * `producer` — the producer that emitted the lease
      * `lease` — the `%Rheo.Lease{}`
      * `reason` — stored on the delivery (default `:rejected`)
      * `opts` — `:rheo` instance name (default `Rheo`)

    ## Returns

    The result of `Rheo.reject/3`.
    """
    @spec reject(GenServer.server(), Lease.t(), term(), keyword()) :: :ok | {:error, term()}
    def reject(producer, %Lease{} = lease, reason \\ :rejected, opts \\ []) do
      settle(producer, lease, fn -> Rheo.reject(lease, reason, opts) end)
    end

    @doc """
    Tells the producer that leases were settled elsewhere.

    Removes them from the inflight set so renewal stops and `:max_demand` capacity
    is released. Accepts one `lease_id` or a list. Asynchronous. Prefer `ack/3`,
    `nack/4`, and `reject/4`, which settle and confirm together.
    """
    @spec confirm(GenServer.server(), String.t() | [String.t()]) :: :ok
    def confirm(producer, lease_id) when is_binary(lease_id), do: confirm(producer, [lease_id])

    def confirm(producer, lease_ids) when is_list(lease_ids) do
      GenStage.cast(producer, {:confirm, lease_ids})
    end

    @doc """
    Stops fetching and waits until inflight leases are settled or `timeout` elapses.

    Returns `:ok` when nothing is inflight, `:timeout` otherwise. The producer
    keeps serving confirmations and renewals while a drain is pending. Broadway
    calls `prepare_for_draining/1` on its own during shutdown; this is the
    equivalent for plain GenStage pipelines.
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
    Returns the configuration of the producer running in the calling process.

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
      state
      |> drop_inflight(lease_ids)
      |> maybe_finish_drain()
      |> dispatch()
    end

    def handle_cast(_other, state), do: {:noreply, [], state}

    @impl true
    def handle_call({:drain, timeout}, from, state) do
      state = stop_fetching(state)

      if Inflight.size(state.inflight) == 0 do
        {:reply, :ok, [], state}
      else
        timer = Process.send_after(self(), :drain_timeout, timeout)
        {:noreply, [], %{state | drain_from: from, drain_timer: timer}}
      end
    end

    def handle_call(:inflight_count, _from, state) do
      {:reply, Inflight.size(state.inflight), [], state}
    end

    @impl true
    def handle_info(:poll, state), do: dispatch(%{state | poll_timer: nil})

    def handle_info(:renew, state) do
      state
      |> renew_inflight()
      |> maybe_finish_drain()
      |> schedule_renew()
      |> dispatch()
    end

    def handle_info(:drain_timeout, state), do: {:noreply, [], finish_drain(state, :timeout)}

    def handle_info(_other, state), do: {:noreply, [], state}

    if Code.ensure_loaded?(Broadway.Producer) do
      @impl Broadway.Producer
      def prepare_for_draining(state), do: {:noreply, [], stop_fetching(state)}
    end

    @impl true
    def terminate(reason, state) do
      state = state |> stop_fetching() |> cancel_renew()
      {_result, _state} = await_inflight(state, @drain_timeout_ms)

      Telemetry.execute([:rheo, :producer, :stop], %{count: 1}, %{
        stream: state.stream,
        group: state.group,
        consumer_id: state.consumer_id,
        reason: reason
      })

      :ok
    end

    defp settle(producer, lease, settle_fun) do
      result = settle_fun.()
      :ok = confirm(producer, lease.lease_id)
      result
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
    # confirmation cannot wedge the producer permanently.
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
          {:noreply, [], backoff(state, Settle.classify(reason))}
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
        reason: reason
      })

      delay = Backoff.next(state.backoff_ms)

      Logger.warning(
        "Rheo.Producer fetch failed stream=#{state.stream} group=#{state.group} " <>
          "reason=#{inspect(reason)} backoff_ms=#{delay}"
      )

      schedule_poll(%{state | backoff_ms: delay}, delay)
    end

    defp renew_inflight(state) do
      {inflight, results} =
        Inflight.renew_all(
          state.inflight,
          &Rheo.renew(&1, rheo: state.rheo, lease_ms: state.lease_ms)
        )

      Enum.each(results, fn {_lease_id, lease, result} ->
        Telemetry.execute([:rheo, :lease, :renew], %{count: 1}, %{
          stream: state.stream,
          group: state.group,
          event_id: lease.event_id,
          result: result
        })

        cond do
          result == :ok ->
            :ok

          Settle.lost?(result) ->
            Logger.warning(
              "Rheo.Producer dropping stale lease stream=#{state.stream} " <>
                "group=#{state.group} event_id=#{lease.event_id}"
            )

          true ->
            Logger.warning(
              "Rheo.Producer renew failed stream=#{state.stream} group=#{state.group} " <>
                "event_id=#{lease.event_id} reason=#{inspect(result)}"
            )
        end
      end)

      %{state | inflight: inflight}
    end

    defp maybe_finish_drain(%{drain_from: nil} = state), do: state

    defp maybe_finish_drain(state) do
      if Inflight.size(state.inflight) == 0, do: finish_drain(state, :ok), else: state
    end

    defp finish_drain(%{drain_from: nil} = state, _reply), do: state

    defp finish_drain(state, reply) do
      _ = Process.cancel_timer(state.drain_timer)
      GenStage.reply(state.drain_from, reply)
      %{state | drain_from: nil, drain_timer: nil}
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

    # Runs only inside terminate/2, where blocking the process is acceptable.
    defp wait_inflight(state, deadline) do
      remaining = deadline - System.monotonic_time(:millisecond)

      if remaining <= 0 do
        {:timeout, state}
      else
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
  end
end
