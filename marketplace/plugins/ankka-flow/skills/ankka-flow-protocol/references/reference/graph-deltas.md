# Graph deltas

> The ankka.graph-delta.v1 contract a mapping streamlet writes and the Neo4j merge sink reads — node merges, edge merges and tombstones, each a versioned statement of state.

Source: https://flow.ankka.cloud/reference/graph-deltas/
A graph delta is one JSON object per Kafka record, under the contract `ankka.graph-delta.v1`. It says
what one element of a graph — a node or an edge — looks like now, or that it is gone. The
[Neo4j merge sink](neo4j-merge-sink.md) reads deltas and merges them into a graph; a streamlet in any
language writes them, usually by mapping a service's events.

An outlet declares the contract like any JSON contract. In the Python SDK:

```python
deltas = JsonOutlet("deltas", schema_name="ankka.graph-delta.v1")
```

In any other language, the contract is `{"format": "json", "schema_name": "ankka.graph-delta.v1"}`
with the fingerprint `Base64(SHA-256("ankka.graph-delta.v1"))`.

## The three kinds

A node merge carries the node's labels and properties:

```json
{"kind": "node", "id": "cart:cart-1", "version": 1790627790360,
 "labels": ["Cart"], "properties": {"cartId": "cart-1"}}
```

An edge merge carries its type, its endpoints and its properties. Its direction is `from` → `to`:

```json
{"kind": "edge", "id": "checked-out:cart-1:1790627790360", "version": 1790627790360,
 "type": "CHECKED_OUT", "from": "cart:cart-1", "to": "checkout:cart-1:1790627790360",
 "properties": {}}
```

A tombstone marks an element deleted. An edge tombstone names the edge's type and endpoints, so the
edge is found without scanning the graph:

```json
{"kind": "tombstone", "element": "node", "id": "cart:cart-1", "version": 1790627800000}

{"kind": "tombstone", "element": "edge", "id": "checked-out:cart-1:1790627790360", "version": 1790627800000,
 "type": "CHECKED_OUT", "from": "cart:cart-1", "to": "checkout:cart-1:1790627790360"}
```

## Fields

| Field | Type | On | Rule |
|---|---|---|---|
| `kind` | `node`, `edge` or `tombstone` | all | anything else fails the batch |
| `id` | non-empty string | all | global; nodes and edges are separate id spaces |
| `version` | whole number ≥ 0, within 64 bits | all | rises with the source entity's own history |
| `labels` | array of identifiers | node | may be empty or absent |
| `type` | identifier | edge, edge tombstone | one type per edge |
| `from`, `to` | non-empty strings | edge, edge tombstone | node ids |
| `properties` | object | node, edge | may be empty or absent |
| `element` | `node` or `edge` | tombstone | which id space the tombstone marks |

An identifier matches `[A-Za-z_][A-Za-z0-9_]*`. A property value is a string, a number, a boolean, or
a non-empty array of values of one of those kinds. A whole number becomes a 64-bit integer in the
graph and any other number a float. Top-level fields the contract does not name are ignored.

## Rules a writer keeps

1. **State, not change.** A node or edge delta carries the element's whole state. To remove a
   property, send the state without it; nothing means "add to" or "increment".
2. **One writer per element, and its version rises.** `version` is the source entity's sequence
   number, or any integer that rises with that entity's history, such as an event time in
   milliseconds when one event per millisecond is guaranteed. Two source entities never write the
   same element id.
3. **Ids are global and stable.** Prefix them by kind of thing (`cart:`, `checkout:`) so ids from
   different entities cannot collide.
4. **Key the record by the element id.** All deltas for one element then land on one partition and
   are applied in order. An edge and its endpoints are usually on different partitions, and that is
   fine: the sink creates a placeholder for an endpoint it has not seen yet.
5. **Property values are plain.** No `null`, no nested objects, no mixed arrays. The keys `id`,
   `_version` and `_deleted` belong to the sink.
6. **A tombstone marks; it does not remove.** The element stays, marked, so a straggling older delta
   cannot bring it back.
7. **A new contract version is a new schema name.** A change a reader of `v1` could not accept is
   `ankka.graph-delta.v2`.

## What the sink does with a delta

The sink applies a delta only when its `version` is greater than the version the graph holds for that
element; an equal or lower version is counted as stale and changes nothing. Within one batch it keeps
one delta per element — the highest version, the first on a tie. So a delta applied twice, or an old
one arriving after a newer one, leaves the graph as it was.

## Validation

A record that breaks the contract fails its whole batch: the partition stalls, nothing in the batch is
written, and the sink's log names the record's offset and the problem. Nothing is skipped; the fix is
in the streamlet that wrote the record.

| Check | Message |
|---|---|
| the value is a JSON object | `offset N: not a JSON object` |
| `kind` is present | `offset N: kind missing` |
| `kind` is known | `offset N: unknown kind 'x'` |
| `id` is a non-empty string | `offset N: id missing or empty` |
| `version` is a whole number ≥ 0 within 64 bits | `offset N: version is not a non-negative integer` |
| `labels` is an array of identifiers | `offset N: labels must be an array of identifiers` |
| an edge has `type`, `from` and `to` | `offset N: edge needs type, from and to` |
| a tombstone names its element | `offset N: tombstone needs element 'node' or 'edge'` |
| an edge tombstone has `type`, `from` and `to` | `offset N: tombstone of an edge needs type, from and to` |
| `properties` is an object | `offset N: properties must be an object` |
| a property is a scalar or an array of one scalar kind | `offset N: property 'p' is not a scalar or array of scalars` |
| a property key is not the sink's | `offset N: property 'p' is reserved` |
