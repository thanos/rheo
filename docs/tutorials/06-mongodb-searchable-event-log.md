# Article 6 — MongoDB as a Searchable Event Log

## Event document

```javascript
{
  _id: "…",
  stream: "market-events",
  partition: 0,
  sequence: 12345,
  timestamp: ISODate("…"),
  type: "curve_update",
  key: null,
  metadata: { correlation_id: "abc", producer: "pricing-service-v3" },
  payload: { currency: "EUR", curve: "EUR-EURIBOR-6M", price: 2.913 }
}
```

## Sequences

`streams.next_sequence` is atomically incremented on append. A unique index on
`(stream, partition, sequence)` preserves ordering integrity.

## Leases

`deliveries` documents track per-group status: `available`, `leased`, `acked`,
`rejected`. Claim uses `findOneAndUpdate` with status/expiry predicates.

## Queries

`Rheo.query/2` is intentional product surface, not an admin afterthought.
