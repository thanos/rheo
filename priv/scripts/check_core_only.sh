#!/usr/bin/env bash
# Proves that `rheo` compiles and runs with none of its optional integrations
# (ADR 020): a throwaway project depends on this checkout, pulls no
# mongodb_driver / ecto / gen_stage / broadway, and the ETS backend works.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

mkdir -p "$work/core_only/lib"
cat > "$work/core_only/mix.exs" <<EOF
defmodule CoreOnly.MixProject do
  use Mix.Project

  def project do
    [app: :core_only, version: "0.1.0", elixir: "~> 1.17", deps: deps()]
  end

  def application, do: [extra_applications: [:logger]]

  defp deps, do: [{:rheo, path: "$root"}]
end
EOF

cat > "$work/core_only/lib/check.exs" <<'EOF'
present = for mod <- [Mongo, Ecto.Adapters.SQL, GenStage, Broadway], Code.ensure_loaded?(mod), do: mod
if present != [], do: raise("optional dependencies leaked into the core-only build: #{inspect(present)}")

absent = [Rheo.Backend.Mongo, Rheo.Backend.Ecto, Rheo.Producer, Rheo.Broadway, Mix.Tasks.Rheo.Ecto.GenMigration]
for mod <- absent, Code.ensure_loaded?(mod), do: raise("#{inspect(mod)} compiled without its dependency")

{:ok, _} = Rheo.start_link(name: CoreRheo, backend: Rheo.Backend.ETS)
:ok = Rheo.create_stream("s", rheo: CoreRheo)
:ok = Rheo.create_group("s", "g", rheo: CoreRheo)
{:ok, _} = Rheo.append("s", %{type: "e"}, rheo: CoreRheo)
{:ok, [lease]} = Rheo.fetch("s", "g", limit: 1, rheo: CoreRheo)
:ok = Rheo.ack(lease, rheo: CoreRheo)

try do
  Rheo.start_link(name: NoBackend, url: "mongodb://localhost/x")
  raise "expected ArgumentError without Rheo.Backend.Mongo"
rescue
  ArgumentError -> :ok
end

IO.puts("core-only build OK")
EOF

cd "$work/core_only"
MIX_ENV=prod mix deps.get --quiet
MIX_ENV=prod mix compile --warnings-as-errors
MIX_ENV=prod mix run lib/check.exs
