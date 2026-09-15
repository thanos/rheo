# Article 3 — Why ACK Is Harder Than It Looks

Happy path ACK is easy. Failure timelines are not.

## Timeline

```text
t0  worker A fetches event E → lease L1
t1  worker A crashes before ACK
t2  lease L1 expires
t3  worker B fetches event E → lease L2
t4  worker A (restarted, delayed) tries ACK L1 → rejected (:stale_lease)
t5  worker B ACK L2 → durable progress
```

Rheo fences ACKs with opaque `lease_id` tokens. Only the current lease holder can
acknowledge.

## At-least-once

Between t1 and t5, E may be processed twice. That is intentional. Exactly-once
processing requires application idempotency (use `event.id`).

## Tests

`test/rheo/lease_test.exs` covers stale ACK, expiry, retry, max attempts, and
reject using `Rheo.Clock.Frozen` instead of sleeps.
