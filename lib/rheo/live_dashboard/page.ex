# Optional: compiled only when Phoenix LiveDashboard is present.
if Code.ensure_loaded?(Phoenix.LiveDashboard.PageBuilder) do
  defmodule Rheo.LiveDashboard.Page do
    @moduledoc """
    Thin LiveDashboard page over Rheo inspect APIs (ADR 027).

    Read-only: group health (lag, inflight, **dead letters / DLQ**). Does not
    settle leases. A dead letter is a delivery parked for the group after
    `reject` or exhausted nacks — see the [ops guide](ops.html).

    ![Rheo LiveDashboard — group health](screenshots/Rheo-Screenshot-LiveDashboard.jpg)

    Enable with:

        {:phoenix_live_dashboard, "~> 0.8"}

    and register in your router:

        live_dashboard "/dashboard",
          additional_pages: [rheo: Rheo.LiveDashboard.Page]

    Configure the Rheo instance via application env:

        config :rheo, Rheo.LiveDashboard, rheo: MyRheo

    Try without a host Phoenix app: `iex examples/live_dashboard_ops.exs` or
    `notebooks/live_dashboard.livemd` (control panel + this page). See the
    [ops guide](ops.html).
    """

    use Phoenix.LiveDashboard.PageBuilder

    @impl true
    def menu_link(_session, _capabilities) do
      {:ok, "Rheo"}
    end

    @impl true
    def render(assigns) do
      ~H"""
      <.live_table
        id="rheo-group-health"
        dom_id="rheo-group-health"
        page={@page}
        title="Rheo group health"
        row_fetcher={{&fetch_health/3, nil}}
        rows_name="groups"
      >
        <:col field={:stream} header="Stream" />
        <:col field={:group} header="Group" />
        <:col field={:lag} header="Lag" text_align="right" sortable={:desc} />
        <:col field={:inflight} header="Inflight" text_align="right" />
        <:col field={:dead_letters} header="Dead letters (DLQ)" text_align="right" />
      </.live_table>
      """
    end

    # Building the table costs one `list_streams` plus a `list_groups` per
    # stream plus a `group_info` per group. Re-sorting or re-paging must not
    # replay that against the backend, and neither should the dashboard's
    # refresh timer firing faster than the data is worth. Rows are cached for
    # `@cache_ttl_ms` in the row_fetcher's state.
    @cache_ttl_ms 2_000

    @doc false
    def fetch_health(params, _node, state) do
      {rows, state} = cached_rows(state)
      sorted = sort_rows(rows, params)

      {Enum.take(sorted, row_limit(params, length(sorted))), length(sorted), state}
    end

    defp cached_rows({rows, fetched_at}) do
      if monotonic_ms() - fetched_at < @cache_ttl_ms do
        {rows, {rows, fetched_at}}
      else
        fresh_rows()
      end
    end

    defp cached_rows(_state), do: fresh_rows()

    defp fresh_rows do
      rows = health_rows(rheo_name())
      {rows, {rows, monotonic_ms()}}
    end

    defp monotonic_ms, do: System.monotonic_time(:millisecond)

    defp health_rows(rheo) do
      case Rheo.list_streams(rheo: rheo) do
        {:ok, streams} -> Enum.flat_map(streams, &group_rows(rheo, &1))
        _ -> []
      end
    end

    defp group_rows(rheo, stream) do
      case Rheo.list_groups(stream, rheo: rheo) do
        {:ok, groups} -> Enum.map(groups, &health_row(rheo, stream, &1))
        _ -> []
      end
    end

    defp health_row(rheo, stream, group) do
      case Rheo.group_info(stream, group, rheo: rheo) do
        {:ok, info} ->
          %{
            stream: stream,
            group: group,
            lag: info.lag.lag,
            inflight: info.inflight_count,
            dead_letters: info.dead_letter_count
          }

        _ ->
          %{stream: stream, group: group, lag: :error, inflight: :error, dead_letters: :error}
      end
    end

    defp sort_rows(rows, params) do
      field = param(params, :sort_by, :stream)
      dir = param(params, :sort_dir, :asc)

      Enum.sort_by(rows, &Map.get(&1, field, &1.stream), sort_sorter(dir))
    end

    defp row_limit(params, default) do
      case param(params, :limit, default) do
        n when is_integer(n) and n > 0 -> n
        _ -> default
      end
    end

    defp param(params, key, default) when is_map(params) do
      Map.get(params, key) || Map.get(params, Atom.to_string(key), default)
    end

    defp param(_params, _key, default), do: default

    defp sort_sorter(:desc), do: :desc
    defp sort_sorter("desc"), do: :desc
    defp sort_sorter(_), do: :asc

    defp rheo_name do
      Application.get_env(:rheo, Rheo.LiveDashboard, [])
      |> Keyword.get(:rheo, Rheo)
    end
  end
end
