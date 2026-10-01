# Graph deltas

> The ankka.graph-delta.v1 contract a mapping streamlet writes and the Neo4j merge sink reads — node merges, edge merges and tombstones, each a versioned statement of state under its element key.

Source: https://flow.ankka.cloud/reference/graph-deltas/
A graph delta is one JSON object per Kafka record, under the contract `ankka.graph-delta.v1`. It says
what one element of a graph — a node or an edge — looks like now, or that it is gone. The
[Neo4j merge sink](neo4j-merge-sink.md) reads deltas and merges them into a graph; a streamlet in any
language writes them, usually by mapping a service's events.

In the Python SDK an outlet of deltas is a `GraphDeltaOutlet`, which owns the contract and builds each
record with its key; see [Python SDK](python-sdk.md#graph-deltas):

```python
deltas = GraphDeltaOutlet("deltas")
```

In any other language, the contract is `{"format": "json", "schema_name": "ankka.graph-delta.v1"}`
with the fingerprint `Base64(SHA-256("ankka.graph-delta.v1"))`, and the writer sets each record's key
itself, by the rule in [The record key](#the-record-key).

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

## The record key

A delta's Kafka record key is its **element key**: the element's kind and its id, as UTF-8 text.

| Delta | Key |
|---|---|
| a node merge for id `X` | `node:X` |
| a tombstone with `"element": "node"` for id `X` | `node:X` |
| an edge merge for id `X` | `edge:X` |
| a tombstone with `"element": "edge"` for id `X` | `edge:X` |

The id follows the first colon verbatim, so an id that contains colons is unambiguous: the node
`cart:cart-1` has the key `node:cart:cart-1`. Two records have the same key exactly when they describe
the same element. Nodes and edges are separate id spaces, so a node and an edge that share an id have
different keys.

The key is a requirement, not advice. A delta topic is compacted by default (see
[Topics and Kafka clusters](../concepts/topics.md#delta-topics)), and a compacted topic keeps the last
record per key: a delta under any other key would replace, or be replaced by, another element's
record. So the sink refuses a delta whose key is missing or is not its own element key, on every
delta topic, compacted or not. The batch fails, nothing of it is written, and the message gives the
key the delta should have had:

```text
neo4j merge failed for inlet 'in' partition 1: offset 42: key 'cart-1' is not this delta's element key 'node:cart:cart-1'
neo4j merge failed for inlet 'in' partition 1: offset 43: no key; this delta's element key is 'edge:checked-out:cart-1:1790627790360'
```

[`protocol/fixtures/graph-deltas/keys.json`](https://github.com/thinkmorestupidless/ankka-flow/blob/main/protocol/fixtures/graph-deltas/keys.json)
lists deltas with their keys, including an id with colons and one with non-ASCII characters; the sink
and the Python SDK are both tested against it.

## Delete markers

A record with a key and no value is a **delete marker**: Kafka's way of removing a key from a
compacted topic. It is not a delta. The sink applies nothing for it, counts it, and acknowledges it
with its batch; a record with an empty value is read the same way. A marker never stalls a partition.

The platform writes no delete markers. A tombstone stays in the topic as its element's last record, so
the topic is bounded by the elements ever created and a graph rebuilt from it has deletions marked. A
writer that wants a tombstoned element's record gone from the topic sends the marker itself, under the
element's key, after the tombstone. The graph that already holds the element is unchanged; once the
broker has compacted the key away, a graph [rebuilt from the topic](../deploy/rebuild-a-graph.md) no
longer has it.

A marker written for an element that was never tombstoned is a mistake to avoid: the element is still
live in the graph that was built as the records arrived, live in a rebuild until the broker compacts
its key, and absent from every rebuild after that.

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
4. **Key the record by its element key**, `node:<id>` or `edge:<id>`. All deltas for one element then
   land on one partition and are applied in order, and compaction keeps the element's latest. An
   edge and its endpoints are usually on different partitions, and that is fine: the sink creates a
   placeholder for an endpoint it has not seen yet.
5. **Property values are plain.** No `null`, no nested objects, no mixed arrays. The keys `id`,
   `_version` and `_deleted` belong to the sink.
6. **A tombstone marks; it does not remove.** The element stays, marked, so a straggling older delta
   cannot bring it back. A tombstone's key is the key of the element it marks.
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
| the record's key is the delta's element key | `offset N: key 'k' is not this delta's element key 'node:x'` |
| the record has a key | `offset N: no key; this delta's element key is 'node:x'` |

A record with no value, or an empty one, is not checked at all: it is a
[delete marker](#delete-markers).

## For a writer built before the key rule

The contract's name is unchanged, so a blueprint whose writer keys its deltas some other way — by the
bare id, or by its input's key — still passes `flow verify`. The sink refuses that writer's first
delta at run time, the partition stalls, and the message gives the key expected. A delta topic that
already holds records keyed the earlier way is refused at the first of them when read from the start.

To bring a pipeline across:

1. **Scale the writer and the sink to zero.** Set `replicas = 0` for both in a `--conf` file,
   generate and apply.
2. **Delete the delta topic.** The platform never alters or deletes a topic; it recreates a managed
   one on the next reconcile, compacted.
3. **Deploy the writer that keys its deltas** `node:<id>` and `edge:<id>` — in Python, one that
   declares a `GraphDeltaOutlet`.
4. **Reset the pipeline**: `flow reset <pipeline>`.
5. **Scale both back up.** The writer re-emits its history under the new keys, and the sink finds
   what the graph already holds stale.
