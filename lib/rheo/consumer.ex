defmodule Rheo.Consumer do
  @moduledoc """
  Idiomatic OTP consumer behaviour for Rheo streams.

  `use Rheo.Consumer` defines a `GenServer` that polls with bounded demand,
  dispatches `c:handle_event/2`, and maps outcomes to `Rheo.ack/1`,
  `Rheo.nack/2`, or `Rheo.reject/2`.

  ## Options (`use` and `start_link/1`)

    * `:stream` — required stream name
    * `:group` — required consumer group
    * `:max_demand` — fetch limit / outstanding bound (default from config)
    * `:lease_ms` — lease TTL in milliseconds
    * `:poll_ms` — idle poll interval (default `200`)
    * `:consumer_id` — worker identity (default: generated)
    * `:name` — GenServer name (default: the consumer module)
    * `:id` — supervisor child id (default: the consumer module)

  ## Example

      defmodule MyApp.RiskConsumer do
        use Rheo.Consumer,
          stream: "market-events",
          group: "risk",
          max_demand: 10

        @impl true
        def setup(_opts) do
          {:ok, %{processed: 0}}
        end

        @impl true
        def handle_event(event, state) do
          case Risk.process(event) do
            :ok ->
              {:ack, %{state | processed: state.processed + 1}}

            {:temporary_error, reason} ->
              {:retry, reason, state}

            {:permanent_error, reason} ->
              {:reject, reason, state}
          end
        end
      end

      children = [
        {Rheo, url: "mongodb://localhost:27017/rheo"},
        {MyApp.RiskConsumer, consumer_id: "risk-1"}
      ]

  ## Outcomes

    * `{:ack, state}` — success; calls `Rheo.ack/1`
    * `{:retry, reason, state}` — temporary failure; calls `Rheo.nack/2`
    * `{:reject, reason, state}` — permanent failure; calls `Rheo.reject/2`
  """

  alias Rheo.Lease

  @doc """
  Handles a single leased event.

  ## Arguments

    * `event` — `%Rheo.Event{}` from the lease
    * `state` — consumer state from `c:setup/1` or previous returns

  ## Returns

    * `{:ack, new_state}`
    * `{:retry, reason, new_state}`
    * `{:reject, reason, new_state}`

  ## Example

      @impl true
      def handle_event(%Rheo.Event{type: "curve_update"} = event, state) do
        :ok = Curves.apply(event.payload)
        {:ack, state}
      end

      def handle_event(%Rheo.Event{type: "poison"}, state) do
        {:reject, :poison_message, state}
      end
  """
  @callback handle_event(Rheo.Event.t(), term()) ::
              {:ack, term()}
              | {:retry, term(), term()}
              | {:reject, term(), term()}

  @doc """
  Optional callback to initialize consumer state when the process starts.

  ## Arguments

    * `opts` — merged `use` / `start_link/1` options

  ## Returns

    * `{:ok, state}` — continue startup with `state`
    * `{:stop, reason}` — abort GenServer start

  ## Example

      @impl true
      def setup(opts) do
        repo = Keyword.fetch!(opts, :repo)
        {:ok, %{repo: repo, seen: MapSet.new()}}
      end
  """
  @callback setup(keyword()) :: {:ok, term()} | {:stop, term()}

  @optional_callbacks setup: 1

  @doc false
  def dispatch(module, %Lease{} = lease, user_state) do
    case module.handle_event(lease.event, user_state) do
      {:ack, new_state} ->
        _ = Rheo.ack(lease)
        new_state

      {:retry, reason, new_state} ->
        _ = Rheo.nack(lease, reason)
        new_state

      {:reject, reason, new_state} ->
        _ = Rheo.reject(lease, reason)
        new_state
    end
  end

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @behaviour Rheo.Consumer
      @rheo_consumer_opts opts

      use GenServer

      @doc """
      Returns a supervisor child spec for this consumer.

      ## Examples

          children = [{__MODULE__, max_demand: 5, consumer_id: "w1"}]
      """
      def child_spec(arg) when is_list(arg) do
        merged = Keyword.merge(@rheo_consumer_opts, arg)

        %{
          id: Keyword.get(merged, :id, __MODULE__),
          start: {__MODULE__, :start_link, [merged]},
          type: :worker,
          restart: :permanent
        }
      end

      def child_spec(_arg), do: child_spec([])

      @doc """
      Starts the consumer GenServer.

      ## Arguments

        * `arg` — keyword options merged over `use Rheo.Consumer` options

      ## Returns

        * `{:ok, pid}`
        * `{:error, {:already_started, pid}}`
        * `{:error, reason}`
      """
      def start_link(arg \\ [])

      def start_link(arg) when is_list(arg) do
        opts = Keyword.merge(@rheo_consumer_opts, arg)
        name = Keyword.get(opts, :name, __MODULE__)
        GenServer.start_link(__MODULE__, opts, name: name)
      end

      @impl GenServer
      def init(opts) do
        stream = Keyword.fetch!(opts, :stream)
        group = Keyword.fetch!(opts, :group)

        max_demand =
          Keyword.get(opts, :max_demand, Application.get_env(:rheo, :default_max_demand, 10))

        lease_ms =
          Keyword.get(opts, :lease_ms, Application.get_env(:rheo, :default_lease_ms, 30_000))

        poll_ms = Keyword.get(opts, :poll_ms, 200)
        consumer_id = Keyword.get(opts, :consumer_id) || Rheo.Id.generate()

        base = %{
          stream: stream,
          group: group,
          max_demand: max_demand,
          lease_ms: lease_ms,
          poll_ms: poll_ms,
          consumer_id: consumer_id,
          user_state: nil
        }

        case maybe_setup(opts) do
          {:ok, user_state} ->
            Rheo.Telemetry.execute([:rheo, :consumer, :start], %{count: 1}, %{
              stream: stream,
              group: group,
              consumer_id: consumer_id
            })

            {:ok, %{base | user_state: user_state}, {:continue, :poll}}

          {:stop, reason} ->
            {:stop, reason}
        end
      end

      defp maybe_setup(opts) do
        if function_exported?(__MODULE__, :setup, 1) do
          apply(__MODULE__, :setup, [opts])
        else
          {:ok, %{}}
        end
      end

      @impl GenServer
      def handle_continue(:poll, state), do: do_poll(state)

      @impl GenServer
      def handle_info(:poll, state), do: do_poll(state)

      defp do_poll(state) do
        case Rheo.fetch(state.stream, state.group,
               limit: state.max_demand,
               consumer_id: state.consumer_id,
               lease_ms: state.lease_ms
             ) do
          {:ok, []} ->
            Process.send_after(self(), :poll, state.poll_ms)
            {:noreply, state}

          {:ok, leases} ->
            new_user_state =
              Enum.reduce(leases, state.user_state, fn lease, user_state ->
                handle_lease(lease, user_state)
              end)

            send(self(), :poll)
            {:noreply, %{state | user_state: new_user_state}}

          {:error, _reason} ->
            Process.send_after(self(), :poll, state.poll_ms)
            {:noreply, state}
        end
      end

      defp handle_lease(%Lease{} = lease, user_state) do
        Rheo.Consumer.dispatch(__MODULE__, lease, user_state)
      end

      @impl GenServer
      def terminate(reason, state) do
        Rheo.Telemetry.execute([:rheo, :consumer, :stop], %{count: 1}, %{
          stream: state.stream,
          group: state.group,
          consumer_id: state.consumer_id,
          reason: reason
        })

        :ok
      end
    end
  end
end
