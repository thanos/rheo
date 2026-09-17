defmodule Rheo.Consumer do
  @moduledoc """
  Idiomatic OTP consumer behaviour for Rheo streams.

  `use Rheo.Consumer` starts a bridge that owns a local `Rheo.Group` for
  `{rheo, stream, group}`. The Group owns demand, concurrency, lease renewal,
  and settlement — handlers implement `c:handle_event/2`.

  ## Handler contract (v0.8 / ADR 022)

  Context is **read-only**. Return outcomes without mutating callback state:

      :ack
      {:retry, reason}
      {:reject, reason}

  Put shared mutable state in an Agent, ETS, or GenServer owned by your app.

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
    * `:name` — optional bridge GenServer name
    * `:id` — supervisor child id

  ## Example

      defmodule MyApp.RiskConsumer do
        use Rheo.Consumer,
          stream: "market-events",
          group: "risk",
          concurrency: 8,
          max_demand: 100

        @impl true
        def setup(opts) do
          {:ok, agent} = Agent.start_link(fn -> 0 end)
          {:ok, %{agent: agent, rheo: Keyword.get(opts, :rheo, Rheo)}}
        end

        @impl true
        def handle_event(event, %{agent: agent}) do
          case Risk.process(event) do
            :ok ->
              Agent.update(agent, &(&1 + 1))
              :ack

            {:temporary_error, reason} ->
              {:retry, reason}

            {:permanent_error, reason} ->
              {:reject, reason}
          end
        end
      end

  ## Local ownership

  Only one local `Rheo.Consumer` / `Rheo.Group` may exist per
  `{rheo, stream, group}` on a BEAM node. Scale with `:concurrency`. Starting a
  second bridge for the same identity returns `{:error, {:already_started, pid}}`.
  """

  @doc """
  Handles a single leased event.

  `context` is the read-only map from `c:setup/1` (or `%{}`).
  """
  @callback handle_event(Rheo.Event.t(), context :: map()) ::
              :ack
              | {:retry, term()}
              | {:reject, term()}

  @doc """
  Optional callback to initialize read-only context when the group starts.
  """
  @callback setup(keyword()) :: {:ok, map()} | {:stop, term()}

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
          # v0.8: one local owner per {rheo, stream, group} (ADR 022)
          {:stop, {:group_already_started, group_pid}}

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

    def terminate(_reason, _state), do: :ok

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
