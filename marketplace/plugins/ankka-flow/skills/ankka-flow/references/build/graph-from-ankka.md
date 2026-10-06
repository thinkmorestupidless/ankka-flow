# Fill a graph from an ankka service

> Keep a Neo4j graph in step with an ankka service that publishes its own graph deltas, with a pipeline that is the built-in merge sink and nothing else.

Source: https://flow.ankka.cloud/build/graph-from-ankka/
An ankka service can publish its entities as a graph itself: a **graph consumer** in the service says
which nodes and edges each change leaves in which state, and ankka publishes each one to a topic as a
[graph delta](../reference/graph-deltas.md), keyed by its element and versioned by the entity's own
history. How to write one is on ankka's
[Publish a graph](https://docs.ankka.cloud/build/graph/) page, in Scala, Python, TypeScript and Rust.

What is left for a pipeline is the part that is the same for every graph: applying the deltas to
Neo4j. That is the built-in [Neo4j merge sink](../reference/neo4j-merge-sink.md), so the pipeline has
one streamlet and no image of yours.

```text
ankka service (graph consumer) ──► cart-graph (compacted) ──► graph (Neo4j merge sink) ──► Neo4j
```

The [mapper route](graph-sink.md) is for a graph fed by topics services already publish, or by several
services at once; its mapper is a streamlet of yours, written with either SDK's graph delta outlet
(`GraphDeltaOutlet` in Python, `graphDeltaOutlet` in [Scala](scala-streamlet.md), with `node`, `edge`,
`tombstoneNode` and `tombstoneEdge`). When one service owns the graph, this is the shorter road.

## The blueprint

```hocon
blueprint {
  name = cart-graph
  streamlets {
    graph = builtin/neo4j-merge-sink
  }
  topics {
    cart-graph {
      topic.name = "cart-graph"
      consumers  = [graph.in]
      partitions = 3
      replicas   = 1
      consumer-config { auto.offset.reset = earliest }
    }
  }
}
```

The topic is declared as the pipeline's own, under the name the service publishes to. Two things
follow from that:

- **ankka-flow creates it compacted.** A topic with a port of the delta contract gets
  `cleanup.policy = compact` unless the blueprint says otherwise, and `flow verify` says so. A compacted
  topic keeps the latest delta under every element's key, which is what makes it
  [the graph itself](../deploy/rebuild-a-graph.md).
- **Deploy the pipeline before the service.** ankka creates no topics. On a broker that creates topics
  on first use, a service that publishes first gets an uncompacted topic, which the operator then
  reports as `TopicNotCompacted` and leaves as it is.

```bash
flow verify blueprint.conf --conf prod.conf
flow generate blueprint.conf --conf prod.conf -n shop | kubectl apply -f -
```

```text
note: Topic 'cart-graph' carries graph deltas and is compacted (cleanup.policy = compact).
verified: 1 streamlets, 1 topics
```

`prod.conf` names the sink's connection Secret, as for any [merge sink](../reference/neo4j-merge-sink.md):

```hocon
flow.streamlets.graph.config { secret = neo4j-prod }
```

A pipeline with no streamlet of yours needs no `--descriptors` and no `--image`.

## The service

The service needs a broker to publish to: `ANKKA_KAFKA_BOOTSTRAP_SERVERS` in its descriptor's `env`,
naming the same Kafka the pipeline's `default` cluster Secret points at. With it, the service's graph
consumers are registered and every change to an entity publishes its elements.

What arrives on the topic needs nothing from you. Every record is already what the sink demands:

```text
key      node:cart:c1
headers  ce-type: ankka.graph-delta.v1, ce-subject: c1, content-type: application/json

{"kind":"node","id":"cart:c1","version":4,"labels":["Cart"],"properties":{"cartId":"c1","checkedOut":true}}
```

The key is the element key, the version is the entity's sequence number, and a deleted entity's
elements arrive as tombstones. A service's change delivered twice publishes the same deltas at the same
versions, which the sink counts as stale, so the graph does not move.

## Watching it, and rebuilding it

The sink's counters say what it did with the deltas:
[`ankka_flow_stage_deltas_written_total`](../deploy/observe.md#a-built-in-stage) for the ones applied
and `ankka_flow_stage_deltas_stale_total` for the ones an older or repeated version left as they were.
A stall names the record and the problem, as on any delta topic; a wrong key cannot come from an
ankka graph consumer, which has no way to set one.

To fill an empty database, or a second one, reset only the sink: the service is not involved, and
nothing upstream of the topic is replayed. The steps are in
[Rebuild a graph from its delta topic](../deploy/rebuild-a-graph.md).

## What is not here

- ankka does not create, inspect or alter the topic. Compaction is the pipeline's to declare.
- A graph fed by more than one service is several graph consumers publishing to one topic, each
  writing its own elements; the sink needs nothing more. Two services writing the same element is a
  mistake nothing detects.
- A graph built from events a service publishes for other reasons — a checkout notice, say — is the
  [mapper route](graph-sink.md).
