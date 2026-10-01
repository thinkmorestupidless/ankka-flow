# Contract: element keys and delete markers (an addition to `ankka.graph-delta.v1`)

The delta contract's value is unchanged. This adds what its records' **keys** are, and what a
record with no value means.

## The rule

1. **A delta's record key is its element key**: `node:<id>` for a node merge or a node tombstone,
   `edge:<id>` for an edge merge or an edge tombstone, as UTF-8 text, the id verbatim.
2. **The sink refuses any other key**, and a missing one, on every delta topic: the batch fails,
   nothing of it is applied or committed, and the message names the offset, the key found and the
   key expected.
3. **A record with no value, or an empty value, is a delete marker**, whatever its key and even
   with none. It is not a delta. The sink
   applies nothing for it, counts it, and acknowledges it. On a compacted topic it removes the
   key's earlier records once the broker compacts.
4. **The platform writes no delete markers.** A tombstone stays its element's last record. A writer
   that wants a tombstoned element's record gone from the topic sends the marker itself, after the
   tombstone.

## Messages

```text
neo4j merge failed for inlet 'in' partition 1: offset 42: key 'cart-1' is not this delta's element key 'node:cart:cart-1'
neo4j merge failed for inlet 'in' partition 1: offset 43: no key; this delta's element key is 'edge:checked-out:cart-1:1790627790360'
```

## Why the key includes the kind

Nodes and edges are separate id spaces: a node `x` and an edge `x` are different elements. Under
compaction one key keeps one record, so the two must not share a key.

## For a writer built before this rule

The contract's name has not changed, so a blueprint with such a writer still verifies. Its first
delta is refused by the sink with the message above. To bring a pipeline across:

1. Scale the writer and the sink to zero.
2. Delete the delta topic. The platform never alters or deletes a topic; it recreates a managed one
   on the next reconcile, compacted.
3. Deploy the writer built with the keys of this contract (the SDK's `GraphDeltaOutlet` in Python).
4. Reset the writer and the sink (`flow reset <pipeline>`), and scale them up. The writer re-emits
   its history under the new keys; the sink finds what it already holds stale.

## The shared fixture

`protocol/fixtures/graph-deltas/keys.json`: an array of `{"delta": <object>, "key": <string>}`
covering each kind, an id containing colons, and an id with non-ASCII characters. The sink's suite
asserts `Deltas.key(parse(delta)) == key`; the SDK's tests assert the helper produces `key` for the
same delta.
