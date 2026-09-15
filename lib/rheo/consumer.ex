defmodule Rheo.Consumer do
  @moduledoc """
  Idiomatic OTP consumer behaviour for Rheo streams.

  `use Rheo.Consumer` defines a thin child spec that starts a bridge process
  which starts (or joins) a local `Rheo.Group` for `{rheo, stream, group}`. The
  Group owns demand, concurrency, lease renewal, and persistence settle —
  handlers only implement `c:handle_event/2`.

  ## Options (`use` and `start_link/1`)

    * `:stream` — required stream name
    * `:group` — required consumer group
    * `:rheo` — Rheo instance name (default `Rheo`)
    * `:max_demand` — outstanding lease bound (default from config)
    * `:concurrency` — max concurrent handler tasks (default `1`)
    * `:lease_ms` — lease TTL in milliseconds
    * `:poll_ms` — idle poll interval (default `200`)
    * `:consumer_id` — worker identity (default: generated)
    * `:name` — optional bridge GenServer name
    * `:id` — supervisor child id (default: `{module, rheo, stream, group}`)

  ## Example

      defmodule MyApp.RiskConsumer do
        use Rheo.Consumer,
          stream: "market-events",
          group: "risk",
          concurrency: 8,
          max_demand: 100

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
        {Rheo, name: MyRheo, backend: {Rheo.Backend.Mongo, url: "mongodb://localhost:27017/rheo"}},
        {MyApp.RiskConsumer, rheo: MyRheo, consumer_id: "risk-1"}
      ]

  ## Outcomes

    * `{:ack, state}` — success; Group calls `Rheo.ack/2` and checks the result
    * `{:retry, reason, state}` — temporary failure; Group calls `Rheo.nack/3`
    * `{:reject, reason, state}` — permanent failure; Group calls `Rheo.reject/3`

  Concurrent handlers should treat `state` as a snapshot; use an Agent/ETS for
  shared mutable state when `concurrency > 1`.
  """

  @doc """
  Handles a single leased event.
  """
  @callback handle_event(Rheo.Event.t(), term()) ::
              {:ack, term()}
              | {:retry, term(), term()}
              | {:reject, term(), term()}

  @doc """
  Optional callback to initialize consumer state when the group starts.
  """
  @callback setup(keyword()) :: {:ok, term()} | {:stop, term()}

  @optional_callbacks setup: 1

  @doc false
  def child_spec(module, opts) when is_atom(module) and is_list(opts) do
    rheo = Keyword.get(opts, :rheo, Rheo)
    stream = Keyword.fetch!(opts, :stream)
    group = Keyword.fetch!(opts, :group)

    %{
      id: Keyword.get(opts, :id, {module, rheo, stream, group}),
      start: {__MODULE__, :start_link, [module, opts]},
      type: :worker,
      restart: :permanent,
      shutdown: 6_000
    }
  end

  @doc false
  def start_link(module, opts) when is_atom(module) and is_list(opts) do
    name_opts =
      case Keyword.fetch(opts, :name) do
        {:ok, name} -> [name: name]
        :error -> []
      end

    GenServer.start_link(__MODULE__.Bridge, {module, opts}, name_opts)
  end

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @behaviour Rheo.Consumer
      @rheo_consumer_opts opts

      @doc """
      Returns a supervisor child spec that starts this consumer's bridge + group.
      """
      def child_spec(arg) when is_list(arg) do
        Rheo.Consumer.child_spec(__MODULE__, Keyword.merge(@rheo_consumer_opts, arg))
      end

      def child_spec(_arg), do: child_spec([])

      @doc """
      Starts the consumer bridge (and local `Rheo.Group`).
      """
      def start_link(arg \\ [])

      def start_link(arg) when is_list(arg) do
        Rheo.Consumer.start_link(__MODULE__, Keyword.merge(@rheo_consumer_opts, arg))
      end
    end
  end

  defmodule Bridge do
    @moduledoc false
    use GenServer

    @impl true
    def init({module, opts}) do
      rheo = Keyword.get(opts, :rheo, Rheo)
      group_opts = Keyword.put(opts, :module, module)
      starter = Keyword.get(opts, :__group_starter__, &Rheo.GroupSupervisor.start_group/2)

      case starter.(rheo, group_opts) do
        {:ok, group_pid} ->
          ref = Process.monitor(group_pid)

          {:ok,
           %{
             rheo: rheo,
             group_pid: group_pid,
             group_ref: ref,
             opts: group_opts,
             terminator: Keyword.get(opts, :__group_terminator__)
           }}

        {:error, {:already_started, group_pid}} ->
          ref = Process.monitor(group_pid)

          {:ok,
           %{
             rheo: rheo,
             group_pid: group_pid,
             group_ref: ref,
             opts: group_opts,
             shared: true,
             terminator: Keyword.get(opts, :__group_terminator__)
           }}

        {:error, reason} ->
          {:stop, reason}
      end
    end

    @impl true
    def handle_info({:DOWN, ref, :process, _pid, reason}, %{group_ref: ref} = state) do
      {:stop, reason, state}
    end

    def handle_info(_msg, state), do: {:noreply, state}

    @impl true
    def terminate(_reason, %{shared: true}), do: :ok

    def terminate(_reason, %{terminator: fun, rheo: rheo, group_pid: pid})
        when is_function(fun, 2) do
      fun.(rheo, pid)
      :ok
    catch
      :exit, _ -> :ok
    end

    def terminate(_reason, %{rheo: rheo, group_pid: pid}) do
      default_terminate_group(rheo, pid)
    catch
      :exit, _ -> :ok
    end

    defp default_terminate_group(rheo, pid) do
      sup = Rheo.Names.group_supervisor(rheo)

      if is_pid(pid) and Process.alive?(pid) do
        case DynamicSupervisor.terminate_child(sup, pid) do
          :ok -> :ok
          {:error, _} -> Process.exit(pid, :kill)
        end
      end

      :ok
    end
  end
end
