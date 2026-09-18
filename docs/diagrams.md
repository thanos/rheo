# Architecture and Flow Diagrams

Mermaid diagrams for Rheo architecture and flows. On HexDocs they render via
ExDoc's Mermaid integration; on GitHub they render natively in Markdown preview.

## Overall architecture

```mermaid
flowchart TB
  subgraph app [Application]
    RheoInst[Rheo instance]
    Risk[RiskConsumer = Rheo.Group]
    Surv[SurveillanceConsumer = Rheo.Group]
  end
  RheoInst --> BackendChild[Backend handle]
  Risk --> BackendChild
  Surv --> BackendChild
  BackendChild --> DB[(MongoDB / PostgreSQL / SQLite / ETS)]
```

## OTP supervision

```mermaid
flowchart TB
  AppSup[Application Supervisor]
  AppSup --> RheoInst[Rheo instance]
  AppSup --> G1[Rheo.Group risk]
  RheoInst --> Backend[Backend handle]
  RheoInst --> Inst[Rheo.Instance]
  RheoInst --> Tasks[Task.Supervisor]
  RheoInst --> GS[Rheo.GroupSupervisor]
  G1 --> Tasks
```

The host supervisor owns each `Rheo.Group`; `Rheo.GroupSupervisor` only holds
groups started dynamically through `Rheo.GroupSupervisor`.

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
  settle -->|release inflight| prod
  prod -->|renew_inflight| backend
```

The producer owns fetch and renewal; the acknowledger settles each lease with
`Rheo.Producer.ack/3`, `nack/4`, or `reject/4`, which also release the
producer's inflight entry so renewal stops and demand is freed (see ADR 018).

## Settlement failure policy (v0.8)

```mermaid
flowchart TB
  handler[handler returned :ack] --> ack[Rheo.ack]
  ack -->|ok| done[settled]
  ack -->|stale_lease or receipt_mismatch| lost[lease lost: drop, no nack]
  ack -->|backend_unavailable or ambiguous| expire[leave lease to expire]
  ack -->|failed or invalid| nack[nack: immediate redelivery]
  lost --> redeliver[backend redelivers]
  expire --> redeliver
  nack --> redeliver
```

`Rheo.Settle` classifies the error; fencing makes every branch safe under
at-least-once (see ADR 024).

## Native-stream backend shape (design for v0.9)

```mermaid
flowchart LR
  append[append] -->|XADD + logical sequence| stream[(Redis stream)]
  fetch[fetch] -->|XREADGROUP / XAUTOCLAIM| stream
  fetch -->|lease_id + receipt = entry id| lease[Rheo.Lease]
  lease -->|ack: fenced XACK| stream
  lag[lag] -->|XINFO GROUPS| stream
```

The portable `event.sequence` and the frontier stay in Rheo; the entry id
travels in `lease.receipt` (see ADR 021).
