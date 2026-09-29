# Contract: `ankka.graph-delta.v1`

The JSON a mapping streamlet writes and the merge sink reads. One object per Kafka record. Declared
by an outlet as `JsonOutlet("deltas", schema_name="ankka.graph-delta.v1")` in the Python SDK, or by
any language as the contract `{"format": "json", "schema_name": "ankka.graph-delta.v1"}` with the
fingerprint `Base64(SHA-256("ankka.graph-delta.v1"))`.

## Shape

```json
{"kind": "node", "id": "cart:cart-1", "version": 1790627790360,
 "labels": ["Cart"], "properties": {"cartId": "cart-1"}}

{"kind": "edge", "id": "checked-out:cart-1:1790627790360", "version": 1790627790360,
 "type": "CHECKED_OUT", "from": "cart:cart-1", "to": "checkout:cart-1:1790627790360",
 "properties": {}}

{"kind": "tombstone", "element": "node", "id": "cart:cart-1", "version": 1790627800000}

{"kind": "tombstone", "element": "edge", "id": "checked-out:cart-1:1790627790360", "version": 1790627800000,
 "type": "CHECKED_OUT", "from": "cart:cart-1", "to": "checkout:cart-1:1790627790360"}
```

## Rules a writer keeps

1. **State, not change.** A node or edge delta carries the element's whole state. Nothing in a
   delta means "add to", "increment" or "remove this one property"; to remove a property, send the
   state without it.
2. **One writer per element, and its version rises.** `version` is the source entity's own
   sequence number (or any integer that rises with that entity's history, such as an event time in
   milliseconds when one event per millisecond is guaranteed). Two source entities never write the
   same element id.
3. **Ids are global and stable.** Prefix them by kind of thing (`cart:`, `checkout:`) so ids from
   different entities cannot collide; nodes and edges are separate id spaces.
4. **Key the record by the element id**, so all deltas for one element are on one partition and in
   order. An edge and its endpoints are usually on different partitions, and that is fine: the sink
   creates a placeholder for an endpoint it has not seen.
5. **Property values are plain.** A string, a number, a boolean, or a non-empty array of one of
   those. No `null`, no nested objects, no mixed arrays. The keys `id`, `_version` and `_deleted`
   are the sink's.
6. **Labels and types are identifiers**: `[A-Za-z_][A-Za-z0-9_]*`. A node may have any number of
   labels, including none; an edge has exactly one type.
7. **A tombstone marks; it does not remove.** The element stays, with `_deleted: true`, so a
   straggling older delta cannot resurrect it. An edge tombstone names its `type`, `from` and `to`.
8. **A new contract version is a new schema name.** A change to these rules that a reader of `v1`
   could not accept is `ankka.graph-delta.v2`, and a sink that reads it declares that name.

## What the sink does with it

Applied only when `version` is strictly greater than the element's stored `_version`; otherwise
counted as stale and ignored. Within one batch, one delta per element id survives (the highest
version; the first on a tie). A record that violates the shape above fails its batch: the partition
stalls, the sink's log names the offset and the problem, and nothing is skipped
([neo4j-merge-sink.md](./neo4j-merge-sink.md)).

## Validation, as the sink checks it

| check | on failure |
|---|---|
| the value parses as JSON and is an object | `offset N: not a JSON object` |
| `kind` is `node`, `edge` or `tombstone` | `offset N: unknown kind 'x'` |
| `id` is a non-empty string | `offset N: id missing or empty` |
| `version` is a JSON integer ≥ 0 that fits 64 bits | `offset N: version is not a non-negative integer` |
| node: `labels` is an array of identifiers (absent ⇒ empty) | `offset N: labels must be an array of identifiers` |
| edge: `type` is an identifier; `from` and `to` non-empty strings | `offset N: edge needs type, from and to` |
| tombstone: `element` is `node` or `edge`; an edge tombstone has `type`, `from`, `to` | `offset N: tombstone needs element …` |
| `properties` absent, or an object whose values are scalars or homogeneous non-empty arrays of scalars, keys not reserved | `offset N: property 'p' is not a scalar or array of scalars` / `… is reserved` |
| unknown top-level fields | ignored (forward compatibility within `v1`) |
