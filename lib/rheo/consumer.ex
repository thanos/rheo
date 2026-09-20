defmodule Rheo.Consumer do
  @moduledoc """
  OTP consumer behaviour for Rheo streams.

  `use Rheo.Consumer` turns a module into a child spec for one local
  `Rheo.Group`. The Group owns demand, concurrency, lease renewal, and
  settlement; the module implements `c:handle_event/2`.

  Part of the v0.12 frozen surface (ADR 029) — handler outcomes and read-only
  context will not change meaning without a major version after 1.0.

  ## Handler contract

  `handle_event/2` receives the event and a read-only context map built once by
  `c:setup/1` (or `%{}`). It returns one outcome:

      :ack
      {:retry, reason}
      {:reject, reason}

  Handlers run as tasks, up to `:concurrency` at a time. There is no callback
  state: shared mutable state belongs in an Agent, ETS table, or GenServer owned
  by the application and referenced from the context (ADR 022).

  ## Options (`use` and `start_link/1`)

    * `:stream` — required stream name
    * `:group` — required consumer group
    * `:rheo` — Rheo instance name (default `Rheo`)
    * `:max_demand` — outstanding lease bound (default from config)
    * `:concurrency` — max concurrent handler tasks (default `1`)
    * `:lease_ms` — lease TTL in milliseconds
    * `:poll_ms` — idle poll interval (default `200`)
    * `:consumer_id` — worker identity (default: generated)
    * `:partitions` — `:all` (default) or a list of partition ids
    * `:id` — supervisor child id (default `{module, rheo, stream, group}`)

  Every option is also passed to `c:setup/1`, so application-specific keys
  (an Agent pid, a repo, …) can travel through the child spec.

  ## Example

      defmodule MyApp.RiskConsumer do
        use Rheo.Consumer,
          stream: "market-events",
          group: "risk",
          concurrency: 8,
          max_demand: 100

        @impl true
        def setup(opts) do
          {:ok, %{counter: Keyword.fetch!(opts, :counter)}}
        end

        @impl true
        def handle_event(event, %{counter: counter}) do
          case Risk.process(event) do
            :ok ->
              :ok = MyApp.Counter.increment(counter)
              :ack

            {:temporary_error, reason} ->
              {:retry, reason}

            {:permanent_error, reason} ->
              {:reject, reason}
          end
        end
      end

      children = [
        {Rheo, name: MyRheo, backend: Rheo.Backend.ETS},
        {MyApp.Counter, name: MyApp.Counter},
        {MyApp.RiskConsumer, rheo: MyRheo, counter: MyApp.Counter}
      ]

  ## Local ownership

  One `Rheo.Group` per `{rheo, stream, group}` may run on a BEAM node, and the
  supervisor that starts the consumer owns it. Scale local work with
  `:concurrency`; scale across nodes by running one consumer per node. Starting
  a second consumer for the same identity returns
  `{:error, {:already_started, pid}}`.
  """

  @doc """
  Handles a single leased event.

  `context` is the read-only map returned by `c:setup/1` (or `%{}`). The Group
  settles the lease according to the returned outcome; raising, throwing, or
  returning anything else releases the lease for redelivery.
  """
  @callback handle_event(Rheo.Event.t(), context :: map()) ::
              :ack
              | {:retry, term()}
              | {:reject, term()}

  @doc """
  Builds the read-only handler context when the Group starts.

  Receives every option given to the child spec. Return `{:stop, reason}` to
  refuse to start.
  """
  @callback setup(keyword()) :: {:ok, map()} | {:stop, term()}

  @optional_callbacks setup: 1

  @doc false
  def child_spec(module, opts) when is_atom(module) and is_list(opts) do
    opts = group_opts(module, opts)

    %{
      id:
        Keyword.get(
          opts,
          :id,
          {module, Keyword.fetch!(opts, :rheo), Keyword.fetch!(opts, :stream),
           Keyword.fetch!(opts, :group)}
        ),
      start: {__MODULE__, :start_link, [module, Keyword.delete(opts, :id)]},
      type: :worker,
      restart: :permanent,
      shutdown: Rheo.Group.shutdown_ms()
    }
  end

  @doc false
  def start_link(module, opts) when is_atom(module) and is_list(opts) do
    Rheo.Group.start_link(group_opts(module, opts))
  end

  defp group_opts(module, opts) do
    opts
    |> Keyword.put_new(:rheo, Rheo)
    |> Keyword.put(:module, module)
  end

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @behaviour Rheo.Consumer
      @rheo_consumer_opts opts

      @doc """
      Returns a supervisor child spec for this consumer's local `Rheo.Group`.
      """
      def child_spec(arg) when is_list(arg) do
        Rheo.Consumer.child_spec(__MODULE__, Keyword.merge(@rheo_consumer_opts, arg))
      end

      def child_spec(_arg), do: child_spec([])

      @doc """
      Starts this consumer's local `Rheo.Group`.
      """
      def start_link(arg \\ [])

      def start_link(arg) when is_list(arg) do
        Rheo.Consumer.start_link(__MODULE__, Keyword.merge(@rheo_consumer_opts, arg))
      end
    end
  end
end
