# Tutorials

Medium-oriented tutorial notes for Rheo **v0.5.0**:

1. [Why put consumer groups in front of a database?](tutorials/01-why-consumer-groups-on-a-database.md)
2. [What is a consumer group?](tutorials/02-what-is-a-consumer-group.md)
3. [Why ACK is harder than it looks](tutorials/03-why-ack-is-harder.md)
4. [Building Rheo as an Elixir/OTP library](tutorials/04-rheo-as-otp-library.md)
5. [Demand, backpressure, and database consumers](tutorials/05-demand-and-backpressure.md)
6. [MongoDB as a searchable event log](tutorials/06-mongodb-searchable-event-log.md)
7. [Killing consumers on purpose](tutorials/07-killing-consumers.md)
8. [Searching the stream](tutorials/08-searching-the-stream.md)
9. [Why Rheo 0.2 broke its 0.1 API](tutorials/09-why-rheo-0-2-broke-its-0-1-api.md)
10. [If Rheo really is database-agnostic, prove it with ETS](tutorials/10-if-rheo-is-database-agnostic-prove-it-with-ets.md)
11. [Search and replay the event history](tutorials/11-search-and-replay-the-event-history.md)
12. [ACKs are not a cursor](tutorials/12-acks-are-not-a-cursor.md) — partitions, frontier, lag

Hands-on walkthrough: [Livebook demo](https://hexdocs.pm/rheo/rheo_demo.html)
([source](https://github.com/thanos/rheo/blob/main/notebooks/rheo_demo.livemd);
ETS by default). Upgrade notes:
[0.4 → 0.5](https://hexdocs.pm/rheo/0-4-to-0-5.html).
