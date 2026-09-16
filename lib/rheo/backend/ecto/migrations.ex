defmodule Rheo.Backend.Ecto.Migrations do
  @moduledoc """
  Dialect-aware DDL for the `Rheo.Backend.Ecto` schema.

  Rheo owns five tables. They are created idempotently, so `up/2` is safe to run
  from `Rheo.ensure_indexes/1` as well as from a host migration.

  | Table | Purpose |
  |---|---|
  | `rheo_streams` | Stream registry (`partition_count`) |
  | `rheo_stream_sequences` | Per `(stream, partition)` sequence allocator |
  | `rheo_events` | Immutable event log |
  | `rheo_groups` | Consumer group registry, cursors, ACK frontiers |
  | `rheo_deliveries` | Per `(stream, group, event)` lease / ACK / DLQ state |

  Column types differ per dialect: `jsonb` / `timestamptz` on PostgreSQL versus
  `text` on SQLite (see `Rheo.Backend.Ecto.Codec` for the matching encoding).

  ## Host migration

  Generate one with `mix rheo.ecto.gen_migration`, or write it by hand:

      defmodule MyApp.Repo.Migrations.CreateRheoTables do
        use Ecto.Migration

        def up, do: Rheo.Backend.Ecto.Migrations.up(repo())
        def down, do: Rheo.Backend.Ecto.Migrations.down(repo())
      end
  """

  @tables ~w(rheo_deliveries rheo_groups rheo_events rheo_stream_sequences rheo_streams)

  @doc """
  Creates every Rheo table and index if it does not already exist.

  ## Arguments

    * `repo` — an `Ecto.Repo` module
    * `opts` — `:dialect` (`:postgres` | `:sqlite`, inferred from the adapter)
      and `:prefix` (PostgreSQL schema, default `nil`)

  ## Returns

  `:ok`, or `{:error, reason}` from the first failing statement.
  """
  @spec up(module(), keyword()) :: :ok | {:error, term()}
  def up(repo, opts \\ []) do
    dialect = Keyword.get_lazy(opts, :dialect, fn -> dialect_for(repo) end)
    prefix = Keyword.get(opts, :prefix)

    dialect
    |> statements(prefix)
    |> Enum.reduce_while(:ok, fn sql, :ok ->
      case Ecto.Adapters.SQL.query(repo, sql, []) do
        {:ok, _} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  @doc """
  Drops every Rheo table.

  Destructive: deletes the immutable event log. Intended for migration rollback
  and test teardown only.

  ## Arguments

    * `repo` — an `Ecto.Repo` module
    * `opts` — `:prefix` (PostgreSQL schema, default `nil`)

  ## Returns

  `:ok`, or `{:error, reason}` from the first failing statement.
  """
  @spec down(module(), keyword()) :: :ok | {:error, term()}
  def down(repo, opts \\ []) do
    prefix = Keyword.get(opts, :prefix)

    @tables
    |> Enum.reduce_while(:ok, fn table, :ok ->
      case Ecto.Adapters.SQL.query(repo, "DROP TABLE IF EXISTS #{qualify(table, prefix)}", []) do
        {:ok, _} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  @doc """
  Returns the ordered DDL statements for a dialect.

  ## Arguments

    * `dialect` — `:postgres` or `:sqlite`
    * `prefix` — optional PostgreSQL schema name

  ## Examples

      iex> [first | _] = Rheo.Backend.Ecto.Migrations.statements(:sqlite)
      iex> String.starts_with?(first, "CREATE TABLE IF NOT EXISTS rheo_streams")
      true

  ## Returns

  A list of SQL strings, safe to run repeatedly.
  """
  @spec statements(:postgres | :sqlite, String.t() | nil) :: [String.t()]
  def statements(dialect, prefix \\ nil) when dialect in [:postgres, :sqlite] do
    tables(dialect, prefix) ++ indexes(prefix)
  end

  @doc """
  Maps an `Ecto.Repo` adapter onto a Rheo SQL dialect.

  ## Arguments

    * `repo` — an `Ecto.Repo` module

  ## Returns

  `:postgres` or `:sqlite`.

  ## Errors / raises

  Raises `ArgumentError` for any other adapter (Rheo's Ecto backend is SQL-only).
  """
  @spec dialect_for(module()) :: :postgres | :sqlite
  def dialect_for(repo) when is_atom(repo), do: dialect_for_adapter(repo.__adapter__())

  defp dialect_for_adapter(Ecto.Adapters.Postgres), do: :postgres
  defp dialect_for_adapter(Ecto.Adapters.SQLite3), do: :sqlite

  defp dialect_for_adapter(adapter) do
    raise ArgumentError,
          "Rheo.Backend.Ecto supports Ecto.Adapters.Postgres and Ecto.Adapters.SQLite3, " <>
            "got: #{inspect(adapter)}"
  end

  defp tables(dialect, prefix) do
    json = column_type(dialect, :json)
    ts = column_type(dialect, :timestamp)
    seq = column_type(dialect, :bigint)

    [
      """
      CREATE TABLE IF NOT EXISTS #{qualify("rheo_streams", prefix)} (
        name TEXT NOT NULL PRIMARY KEY,
        partition_count INTEGER NOT NULL DEFAULT 1,
        created_at #{ts} NOT NULL
      )
      """,
      """
      CREATE TABLE IF NOT EXISTS #{qualify("rheo_stream_sequences", prefix)} (
        stream TEXT NOT NULL,
        partition INTEGER NOT NULL,
        next_sequence #{seq} NOT NULL DEFAULT 0,
        PRIMARY KEY (stream, partition)
      )
      """,
      """
      CREATE TABLE IF NOT EXISTS #{qualify("rheo_events", prefix)} (
        id TEXT NOT NULL PRIMARY KEY,
        stream TEXT NOT NULL,
        partition INTEGER NOT NULL DEFAULT 0,
        sequence #{seq} NOT NULL,
        "timestamp" #{ts} NOT NULL,
        "key" TEXT,
        "type" TEXT,
        metadata #{json} NOT NULL,
        payload #{json} NOT NULL
      )
      """,
      """
      CREATE TABLE IF NOT EXISTS #{qualify("rheo_groups", prefix)} (
        stream TEXT NOT NULL,
        name TEXT NOT NULL,
        cursors #{json} NOT NULL,
        frontiers #{json} NOT NULL,
        max_attempts INTEGER NOT NULL DEFAULT 5,
        created_at #{ts} NOT NULL,
        PRIMARY KEY (stream, name)
      )
      """,
      """
      CREATE TABLE IF NOT EXISTS #{qualify("rheo_deliveries", prefix)} (
        stream TEXT NOT NULL,
        group_name TEXT NOT NULL,
        event_id TEXT NOT NULL,
        partition INTEGER NOT NULL DEFAULT 0,
        sequence #{seq} NOT NULL,
        status TEXT NOT NULL,
        attempt INTEGER NOT NULL DEFAULT 0,
        lease_id TEXT,
        consumer_id TEXT,
        reason TEXT,
        leased_at #{ts},
        expires_at #{ts},
        renewed_at #{ts},
        acked_at #{ts},
        retried_at #{ts},
        dead_lettered_at #{ts},
        created_at #{ts} NOT NULL,
        PRIMARY KEY (stream, group_name, event_id)
      )
      """
    ]
    |> Enum.map(&squish/1)
  end

  defp indexes(prefix) do
    [
      {"rheo_events_stream_partition_sequence", "rheo_events", "(stream, partition, sequence)",
       true},
      {"rheo_events_stream_timestamp", "rheo_events", ~s[(stream, "timestamp")], false},
      {"rheo_events_stream_type", "rheo_events", ~s[(stream, "type")], false},
      {"rheo_events_stream_key", "rheo_events", ~s[(stream, "key")], false},
      {"rheo_deliveries_claim", "rheo_deliveries", "(stream, group_name, status, expires_at)",
       false},
      {"rheo_deliveries_frontier", "rheo_deliveries", "(stream, group_name, partition, sequence)",
       false}
    ]
    |> Enum.map(fn {name, table, columns, unique?} ->
      unique = if unique?, do: "UNIQUE ", else: ""
      "CREATE #{unique}INDEX IF NOT EXISTS #{name} ON #{qualify(table, prefix)} #{columns}"
    end)
  end

  defp column_type(:postgres, :json), do: "JSONB"
  defp column_type(:postgres, :timestamp), do: "TIMESTAMPTZ"
  defp column_type(:postgres, :bigint), do: "BIGINT"
  defp column_type(:sqlite, :json), do: "TEXT"
  defp column_type(:sqlite, :timestamp), do: "TEXT"
  defp column_type(:sqlite, :bigint), do: "INTEGER"

  defp qualify(table, nil), do: table
  defp qualify(table, prefix), do: ~s("#{prefix}"."#{table}")

  defp squish(sql) do
    sql
    |> String.split(~r/\s+/, trim: true)
    |> Enum.join(" ")
  end
end
