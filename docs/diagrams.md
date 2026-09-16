# Diagrams

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
