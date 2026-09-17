# Architecture and Flow Diagrams

Mermaid diagrams for Rheo architecture and flows. On HexDocs they render via
ExDoc's Mermaid integration; on GitHub they render natively in Markdown preview.

## Overall architecture

```mermaid
flowchart TB
  subgraph app [Application]
    RheoInst[Rheo Instance]
    Risk[RiskConsumer bridge]
    Surv[SurveillanceConsumer bridge]
  end
  RheoInst --> BackendChild[Backend handle]
  RheoInst --> GroupSup[Rheo.GroupSupervisor]
  GroupSup --> GroupRisk[Rheo.Group risk]
  GroupSup --> GroupSurv[Rheo.Group surveillance]
  Risk --> GroupSup
  Surv --> GroupSup
  GroupRisk --> BackendChild
  GroupSurv --> BackendChild
  BackendChild --> DB[(MongoDB)]
```

## OTP supervision

```mermaid
flowchart TB
  AppSup[Application Supervisor]
  AppSup --> RheoInst[Rheo Instance]
  AppSup --> C1[RiskConsumer]
  RheoInst --> Reg[Registry]
  RheoInst --> Mongo[Mongo handle]
  RheoInst --> Inst[Rheo.Instance]
  RheoInst --> Tasks[Task.Supervisor]
  RheoInst --> GS[Rheo.GroupSupervisor]
  C1 --> GS
  GS --> G1[Rheo.Group]
  G1 --> Tasks
```

## Lease lifecycle

```mermaid
stateDiagram-v2
  [*] --> available: materialize
  available --> leased: fetch
  leased --> leased: renew
  leased --> acked: ack
  leased --> available: nack_or_expire
  leased --> rejected: reject_or_max_attempts
  acked --> [*]
  rejected --> [*]
```

## Partitions and ACK frontier (v0.5)

```mermaid
flowchart LR
  append[append key or partition] --> route[phash2]
  route --> seq[per-partition sequence]
  seq --> events[immutable events]
  fetch[fetch assigned partitions] --> mat[materialize by cursor]
  mat --> claim[lease by sequence]
  claim --> ack[ACK or reject]
  ack --> frontier[contiguous frontier]
  frontier --> lag[Rheo.lag HW - frontier]
  events --> mat
```

Ordering is within a partition only. A hole (ACK 1001, inflight 1002, ACK 1003)
keeps the frontier at 1001 until 1002 is terminal.

## Ecto SQL backends (v0.6)

```mermaid
flowchart TB
  app[Host app]
  repo[MyApp.Repo]
  rheo[Rheo instance]
  ecto[Rheo.Backend.Ecto]
  dialect{Dialect}
  pg[PostgreSQL]
  sqlite[SQLite]

  app --> repo
  app --> rheo
  rheo --> ecto
  ecto --> repo
  repo --> dialect
  dialect -->|SKIP_LOCKED optional NOTIFY| pg
  dialect -->|BEGIN IMMEDIATE| sqlite
```

The host owns the Repo. Mongo stays on `Rheo.Backend.Mongo`; Ecto here means
SQL only (see ADR 017).

## Broadway / GenStage interop (v0.7)

```mermaid
flowchart LR
  demand[Broadway_or_GenStage_demand]
  prod[Rheo.Producer]
  fetch[Rheo.fetch]
  backend[Backend_leases]
  proc[Broadway_processors]
  ack[Rheo.Broadway.Acknowledger]
  settle[ack_nack_reject]

  demand --> prod
  prod --> fetch --> backend
  prod -->|Lease| proc
  proc --> ack --> settle --> backend
  ack -->|confirm| prod
  prod -->|renew_inflight| backend
```

The producer owns fetch and renewal; the acknowledger owns settle and reports
back with `Rheo.Producer.confirm/2` so renewal stops and demand is released
(see ADR 018).
