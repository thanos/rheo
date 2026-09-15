# Article 1 — Why Put Consumer Groups in Front of a Database?

> Prefer a hands-on pass first? Open the
> [Livebook demo](../../notebooks/rheo_demo.livemd).

Message brokers excel at delivery: competing consumers, acknowledgements, retries.
They are usually weak at answering: “what happened to EUR-EURIBOR-6M at 13:04?”

Databases excel at durable, indexed history. They are usually weak at broker-like
consumer-group coordination.

Rheo exists for workloads that need both:

- deliver the next event to a consumer group
- find the event that explains what happened

## Brokers vs databases

| System | Delivery | Historical search |
|---|---|---|
| RabbitMQ | Strong queues/ACKs | Weak long-term query |
| NATS / JetStream | Fast streams | Limited investigative query |
| Kafka | Partitioned log + groups | Possible but operationally heavy |
| MongoDB alone | DIY | Strong indexes/query |
| Rheo + MongoDB | Leases/groups in OTP + DB | First-class `Rheo.query/2` |

Brokers remain superior for pure fan-out, ultra-low-latency messaging, and
ecosystems already standardized on them. Rheo does not claim databases replace
brokers universally.

## Rheo's split

Immutable events stay in MongoDB forever (until retention policy says otherwise).
Consumer progress lives in separate delivery state. Consumption never means
deletion — that is the point.
