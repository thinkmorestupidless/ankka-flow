# Blueprint

> Every key of the blueprint file, the HOCON that names a pipeline's streamlets and the topics connecting their ports, and every rule flow verify checks it against.

Source: https://flow.ankka.cloud/reference/blueprint/
A blueprint is a HOCON file, conventionally `blueprint.conf`, that names the streamlets a pipeline
uses and the topics that connect their ports. `flow verify` and `flow generate` read it together with a
directory of streamlet descriptors.

```hocon
blueprint {
  name = cart
  streamlets {
    router = cart-router
    sink   = sink
  }
  topics {
    cart-events {
      managed    = false
      topic.name = "shop.cart-events.v1"
      cluster    = shop
      consumers  = [router.in]
      consumer-config { auto.offset.reset = earliest }
    }
    valid-carts {
      producers  = [router.valid]
      consumers  = [sink.in]
      partitions = 6
      replicas   = 1
      topic { retention.ms = 86400000 }
    }
    review-carts {
      producers = [router.review]
    }
  }
}
```

## `blueprint.name`

Optional. The pipeline id when `flow generate` is given no `--pipeline`; without either, the pipeline
id is the blueprint file's name up to its first dot. A pipeline id is 1 to 40 characters of `a-z`,
`0-9` and `-`, not starting or ending with `-`. It prefixes managed topics' Kafka names, consumer
groups, client ids and the names of everything the operator creates.

## `blueprint.streamlets`

Required, and must name at least one streamlet. Each key is a streamlet's name in this pipeline; each
value is the name of the descriptor it runs, the `streamlet.name` in one of the descriptors given with
`--descriptors`.

```hocon
streamlets {
  router    = cart-router
  router-eu = cart-router     # the same descriptor, a second streamlet
}
```

A streamlet name is a DNS label: at most 63 characters of `a-z`, `0-9` and `-`, not starting or ending
with `-`. Two streamlets may not share a name.

A value of the form `builtin/<name>` names a descriptor the platform ships instead of one an SDK
wrote. Its logic runs inside the sidecar, so it needs no descriptor file, no image and no process
container. The built-ins are:

| Built-in | What it does |
|---|---|
| `builtin/neo4j-merge-sink` | merges [graph deltas](graph-deltas.md) into Neo4j; see [Neo4j merge sink](neo4j-merge-sink.md) |

```hocon
streamlets {
  mapper = checkout-graph
  graph  = builtin/neo4j-merge-sink
}
```

A built-in is found only by its `builtin/` name, and a descriptor file only by its bare name, so a
descriptor file named `neo4j-merge-sink` neither replaces the built-in nor is replaced by it. A
built-in that this version of the CLI does not have is refused:
`Streamlet 'graph' names built-in descriptor 'builtin/nope', which this version does not have; the built-ins are: neo4j-merge-sink.`

## `blueprint.topics`

Each key is a topic id. Each value is an object with these keys, all optional:

| Key | Type | Meaning |
|---|---|---|
| `producers` | list of port paths | outlets that write to the topic |
| `consumers` | list of port paths | inlets that read from the topic |
| `managed` | boolean, default `true` | `false`: the topic belongs to something else and is only read |
| `topic.name` | string | the Kafka topic name; defaults to `<pipeline>.<id>` for a managed topic and `<id>` for an unmanaged one |
| `cluster` | string | the Kafka cluster, a `kafka-cluster-<name>` Secret beside the operator |
| `bootstrap.servers` | string | brokers, instead of or over the cluster's |
| `partitions` | integer | partitions of a managed topic when the operator creates it |
| `replicas` | integer | replication factor of a managed topic when the operator creates it |
| `topic` | object | Kafka topic configuration for a managed topic, such as `retention.ms` |
| `connection-config` | object | Kafka client properties for every client of the topic, such as `security.protocol` |
| `producer-config` | object | Kafka producer properties for the topic's producers |
| `consumer-config` | object | Kafka consumer properties for the topic's consumers, and the batch limits |

A port path is `<streamlet>.<port>`: `router.valid` is the `valid` port of the streamlet named `router`
in `blueprint.streamlets`.

Nested objects are flattened into Kafka property names, so `topic { retention.ms = 86400000 }` and
`topic.retention.ms = 86400000` are the same setting. How settings left out here are filled from a
Kafka cluster is on [Topics and Kafka clusters](../concepts/topics.md).

A topic with no producers and no consumers is ignored: it does not reach the resource and no Kafka
topic is created for it.

### Delta topics

A **delta topic** is a topic with at least one port, producing or consuming, of the graph delta
contract `ankka.graph-delta.v1`. Its latest record per element is the graph, so a managed delta topic
is compacted by default: when neither the blueprint nor a `--conf` file sets `cleanup.policy` for it,
`flow generate` writes `cleanup.policy: compact` into the topic's configuration in the resource, and
the operator creates it compacted. `flow verify` and `flow generate` say what was decided in a note
on stderr, which does not change the exit code:

| Topic | Note |
|---|---|
| managed, no `cleanup.policy` set, or set to `compact` | `note: Topic '<id>' carries graph deltas and is compacted (cleanup.policy = compact).` |
| managed, a policy with both `compact` and `delete` | `note: Topic '<id>' carries graph deltas and sets cleanup.policy = compact,delete; records older than its retention are gone from a rebuild.` |
| managed, a policy without `compact` | `note: Topic '<id>' carries graph deltas and sets cleanup.policy = <policy>; it will not hold the whole graph and cannot be relied on to rebuild it.` |
| unmanaged | `note: Topic '<id>' carries graph deltas and is not managed; whether it is compacted is its owner's.` |

A blueprint sets a policy like any topic setting, and a policy that names both must be quoted, because
an unquoted comma ends a HOCON value:

```hocon
graph-deltas {
  producers = [mapper.deltas]
  consumers = [graph.in]
  topic { cleanup.policy = "compact,delete", retention.ms = 2592000000 }
}
```

A `--conf` file overrides it: `flow.topics.graph-deltas { topic { cleanup.policy = delete } }`. A
topic with no port of the delta contract is written exactly as the blueprint says, with no default
added. What a compacted delta topic makes possible is on
[Rebuild a graph from its delta topic](../deploy/rebuild-a-graph.md).

### Batch limits

The sidecar sends an inlet's records to the process in batches: whatever arrived on one partition
while that partition's previous batch was in flight, capped by two limits set under the topic's
`consumer-config`:

```hocon
consumer-config {
  flow.batch.max-records = 100      # the default
  flow.batch.max-bytes   = 1 MiB    # the default
}
```

The `flow.` keys are the platform's and never reach Kafka; every other `consumer-config` key is passed
to the consumer.

## Deploy-time overrides

Everything under `blueprint.topics.<id>` except `producers`, `consumers`, `managed` and `topic.name`
can be overridden at deploy time under `flow.topics.<id>` in a `--conf` file, and a streamlet's
replicas and parameters under `flow.streamlets.<name>`.
[Configure at deploy time](../deploy/configuration.md) describes them.

## Verification rules

`flow verify` and `flow generate` report every problem in one pass and refuse when there is any. The
blueprint is refused when:

- the file is not valid HOCON, has no `blueprint.streamlets` section, or names no streamlets;
- it has a `blueprint.connections` section, an older format: connect ports through
  `blueprint.topics`;
- the descriptor directory holds no descriptors, a descriptor is invalid, or two descriptors declare
  the same streamlet;
- a streamlet name is not a DNS label, or two streamlets share a name;
- a streamlet names a descriptor that no descriptor declares;
- a port path is not of the form `<streamlet>.<port>`;
- a port path names a streamlet or port that does not exist (the message suggests the ports the
  streamlet has);
- a producer is an inlet, or a consumer is an outlet;
- a port is bound to more than one topic;
- an inlet is connected to no topic;
- two ports on one topic have different contracts (the message names both ports and both contracts);
- a port's contract has a format other than `json`;
- a topic's Kafka name has characters other than `a-z`, `A-Z`, `0-9`, `.`, `_` and `-`, or is longer
  than 249 characters;
- a cluster name is not a DNS label;
- an unmanaged topic has producers, or names neither `bootstrap.servers` nor `cluster`;
- a parameter has no default in its descriptor and no value in `--conf`, or a value that is not its
  type;
- a `--conf` file names a topic or streamlet the blueprint does not, or a parameter the streamlet does
  not declare, or gives a replica count that is negative or not a number.

`flow generate` also refuses when a streamlet has no image or the pipeline id is not valid.

An outlet connected to no topic is allowed: `flow verify` prints it as a note and carries on. A
[delta topic](#delta-topics) gets a note saying whether it is compacted.

```text
note: Outlet router.review is not connected.
note: Topic 'graph-deltas' carries graph deltas and is compacted (cleanup.policy = compact).
```
