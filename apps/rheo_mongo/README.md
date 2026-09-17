# rheo_mongo

MongoDB backend for [Rheo](https://hex.pm/packages/rheo).

```elixir
{:rheo, "~> 0.8.0"},
{:rheo_mongo, "~> 0.8.0"}
```

```elixir
{Rheo, backend: {Rheo.Backend.Mongo, url: "mongodb://localhost:27017/rheo"}}
```

Developed inside the Rheo monorepo (`apps/rheo_mongo`). See ADR 020.
