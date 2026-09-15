# Why Rheo 0.2 Broke Its 0.1 API

Rheo 0.1 proved durable consumer groups on MongoDB: immutable events, leases,
ACK, competing consumers, and a convenient `Rheo.Consumer` GenServer. That
shape was good enough to ship — and sharp enough to paint us into a corner.

## Process topology is not durable truth

In 0.1, each consumer process polled the backend and ACKed inline. It felt like
the consumer *was* the group. Crashes were “handled” by lease expiry, but the
OTP tree and the durable group were casually conflated.

In 0.2, a local `Rheo.Group` is an **OTP lifecycle boundary**: demand, workers,
renewals, drain. The backend remains the authority for leases and ACKs. Multiple
nodes may run Groups for the same durable group; Mongo arbitrates. That split is
why the Consumer API changed from “I am a poll loop” to “I join a Group with a
handler module.”

## Concurrency was a lie

README advertised `concurrency`. The GenServer processed leases sequentially.
Once you introduce real worker Tasks, shared mutable handler state, renewals,
and failed ACKs stop being theoretical — they need a coordinator. Group is that
coordinator.

## Handles beat global topology

`Application.get_env(:rheo, :topology)` made a second Rheo instance awkward and
told every future backend it must look like Mongo’s process. Opaque
`Rheo.Backend.handle` plus named `Rheo.Instance` processes fix that before ETS
and friends arrive.

## Queries should travel

A public `:sort` map is a Mongo document in disguise. `%Rheo.Query{}` is small
on purpose. Backends translate; escape hatches can come later without teaching
every app Mongo’s sort dialect.

## Breaking the API on purpose

0.2 breaks supervision opts, Consumer startup, query sort, and Event decoding
so 0.3+ can add backends without carrying 0.1’s accidents. See
[docs/migrations/0.1-to-0.2.md](../migrations/0.1-to-0.2.md).
