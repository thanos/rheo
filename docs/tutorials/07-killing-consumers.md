# Article 7 — Killing Consumers on Purpose

Messaging infrastructure that cannot survive killed workers is not infrastructure.

Demo flow (`mix rheo.demo`):

1. fetch leases for `risk`
2. kill the consumer process before ACK
3. advance lease time / wait for expiry
4. another consumer obtains the same event
5. ACK succeeds

The regression suite encodes the same invariant without relying on wall-clock
sleeps (`Rheo.Clock.Frozen`).
