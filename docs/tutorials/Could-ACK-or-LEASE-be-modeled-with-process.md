Short answer: **as local coordinators, yes; as the source of truth for leases/ACKs, no** — and Rheo already draws that line.

### What already uses GenServers
`Rheo.Group` is a GenServer that owns:
- demand / fetch scheduling  
- inflight worker refs  
- renew timers  
- drain  

That is the right OTP job: **lifecycle and concurrency on one node**.

The ETS backend also uses a GenServer as a **table owner** so mutations are serialized. The lease/ACK *data* still lives in ETS rows shaped like Mongo deliveries, not in the GenServer’s mailbox state.

### What leases and ACKs actually are
A lease is a **durable fencing token** (`lease_id` on a delivery). ACK/nack/reject succeed only if that token still matches. That has to survive:
- process crash  
- Group restart  
- competing consumers on **other nodes**  

An `Agent` or GenServer map of “who holds what” dies with the process. After a kill you either lose claims or invent ad-hoc recovery that reimplements a database.

### Agent vs GenServer specifically
| Approach | Fine for | Weak for |
|---|---|---|
| **Agent** | Tiny shared handler state | Compare-and-set fencing, multi-node, crash recovery |
| **GenServer (local)** | Inflight, renew, drain (what `Group` does) | Being the only ACK authority across nodes |
| **Backend (Mongo/ETS)** | Durable claim/ACK/redelivery | — |

### Clean model (current design)
```text
Rheo.Group / Agent     →  local OTP coordination
Rheo.Backend           →  leases + ACK truth (fenced mutations)
```

You *could* put lease bookkeeping in Agents for a single-node toy, but then you are not modeling Rheo’s consumer-group contract — you are modeling an in-memory job queue. The second backend (ETS) exists to prove the **same** lease/ACK semantics without pretending OTP memory is durable truth.