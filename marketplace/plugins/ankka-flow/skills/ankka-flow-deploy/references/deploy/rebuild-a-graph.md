# Rebuild a graph from its delta topic

> Fill an empty Neo4j database from a compacted topic of graph deltas by resetting only the merge sink, with no mapper running and no upstream topic read.

Source: https://flow.ankka.cloud/deploy/rebuild-a-graph/
A topic of [graph deltas](../reference/graph-deltas.md) holds the graph: every delta is one element's
whole state, keyed by that element, and a compacted topic keeps the latest record under every key. So a
lost database, or a second one, is filled from the delta topic alone. Only the
[Neo4j merge sink](../reference/neo4j-merge-sink.md) is reset; the streamlets that write the deltas are
not touched, and no upstream service's history is replayed.

This is different from [Rebuild from the start](reset.md), which resets every streamlet and maps every
upstream event again. That always works, and is as slow as the whole history is long. Rebuilding from
the delta topic reads about one record per element.

The steps use a pipeline `checkouts-graph` in namespace `shop` whose sink streamlet is named `graph`.
The mappers in front of it may be stopped or running throughout.

## 1. Stop the sink

Set the sink's `replicas` to 0 through deploy-time configuration, and apply the regenerated resource:

```hocon
# stop-sink.conf
flow.streamlets.graph { replicas = 0 }
```

```bash
flow generate blueprint.conf --descriptors flow --conf prod.conf --conf stop-sink.conf \
  --image mapper=registry.example.com/checkout-graph:1.2 -n shop | kubectl apply -f -
kubectl -n shop get pods \
  -l flow.ankka.thinkmorestupidless.com/pipeline=checkouts-graph,flow.ankka.thinkmorestupidless.com/streamlet=graph
```

Wait until the sink's pod is gone. Scaling the Deployment directly does not work: the operator restores
the resource's `replicas`.

## 2. Give it an empty database

Either empty the database the sink's connection Secret names, or point the Secret at an empty one. The
sink is rolled when its Secret changes, and reads its credentials again on every connection attempt.

To empty a Neo4j database:

```cypher
MATCH (n) CALL (n) { DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS
```

Run it in `cypher-shell`, or prefix it with `:auto` in Neo4j Browser: it commits in batches, so it
cannot run inside an explicit transaction. The statement leaves the uniqueness constraint in place, and
the sink creates it when it opens if it is missing. A rebuild into a database that is not
empty is safe but is not an exact copy: every delta is applied by the usual version rule, so elements
already at or above their topic version are left as they are.

## 3. Reset the sink alone

```bash
flow reset checkouts-graph --streamlet graph -n shop
```

```text
reset requested for 'checkouts-graph': 1b7c2f0e-9a4d-4d3e-8f55-2f1c0b6e9a71
```

`--streamlet` limits the reset to the sink's consumer group, and only the sink has to be stopped for
it. The operator records one `ResetOffsets` event, for the group `checkouts-graph.graph.in`:

```bash
kubectl -n shop get events --field-selector involvedObject.kind=AnkkaFlow,reason=ResetOffsets
```

## 4. Start the sink

Generate without the override and apply:

```bash
flow generate blueprint.conf --descriptors flow --conf prod.conf \
  --image mapper=registry.example.com/checkout-graph:1.2 -n shop | kubectl apply -f -
```

The sink reads the delta topic from the start. Deltas that arrive while it rebuilds are applied in
their place, so the graph ends where the original is.

## Watch it

The rebuild is done when the sink's lag reaches zero. Its pod exports the lag under
`client_id="checkouts-graph.graph.in"`, and `ankka_flow_stage_deltas_written_total` counts the deltas
applied: into an empty database nearly every delta read is written, where a replay over an existing
graph shows as stale deltas instead. See [Observe a pipeline](observe.md#a-built-in-stage).

## What the rebuilt graph contains

- Every **live** element — every node and edge not marked deleted — with its latest labels, properties
  and version: identical to the original.
- Every tombstoned element whose tombstone is still in the topic, marked deleted. The platform writes
  no delete markers, so a tombstone stays as its element's last record unless a writer removes it.
- Placeholders for edge endpoints that no delta ever described, created again by their edges.
- **Not** tombstoned elements whose records a writer removed with a
  [delete marker](../reference/graph-deltas.md#delete-markers) and the broker has since compacted away.

The rebuild does not depend on the broker having compacted. A topic that has not been compacted yet, or
only partly, rebuilds the same live graph; the sink reads more records to get there. A compacted topic
of two thousand elements written ten times each holds about two thousand records, and a rebuild reads
about that many deltas rather than twenty thousand.

## What the topic must be

- **Keyed by element.** Every delta's record key is its element key, `node:<id>` or `edge:<id>`. The
  sink refuses anything else, so a topic the sink has read to the end is keyed correctly.
- **Compacted.** A managed delta topic is compacted by default. With `cleanup.policy = "compact,delete"`
  elements untouched for longer than the topic's retention are missing from a rebuild; with `delete`
  alone the topic holds only recent history and cannot rebuild the graph. `flow verify` says which a
  blueprint has, and the operator records `TopicNotCompacted` for an existing topic that is not
  compacted; see [Blueprint](../reference/blueprint.md#delta-topics).
- **Delete markers only after tombstones.** A marker written for a live element makes rebuilds differ
  from the original: the element is rebuilt live until the broker compacts its key, and is absent
  afterwards.

A delta topic the pipeline does not own is its owner's to keep compacted; the platform never alters a
topic.
