# Glossary

> Every term ankka-flow uses — streamlet, port, blueprint, contract, descriptor, sidecar, conversation, managed topic and the rest — each defined in a sentence with the page that explains it.

Source: https://flow.ankka.cloud/reference/glossary/
The documentation uses each of these terms in exactly this sense.

### Ack (acknowledgement)

The process's answer that it has finished a batch, sent after every emit for that batch. On an ack the
sidecar writes the emits and then commits the batch's offsets. See
[Delivery and failure](../concepts/delivery.md).

### AnkkaFlow

The Kubernetes custom resource that describes one deployed pipeline: its streamlets, images,
descriptors, bindings and topics. `flow generate` writes it; the operator runs it. See
[AnkkaFlow resource](../reference/resource.md).

### At least once

The delivery guarantee: every record is processed, and a record may be processed more than once after a
failure or a rebalance. See [Delivery and failure](../concepts/delivery.md).

### Batch

The records of one inlet partition that the sidecar sends to the process together, in offset order. At
most one batch per inlet partition is in flight. See [The sidecar](../concepts/sidecar.md).

### Blueprint

The HOCON file that names a pipeline's streamlets and the topics connecting their ports. See
[Write a blueprint](../build/blueprints.md) and [Blueprint](../reference/blueprint.md).

### Built-in streamlet

A streamlet whose descriptor ships with the platform, named `builtin/<name>` in a blueprint. It needs no
image or descriptor file, and its pod has only the sidecar, which runs its logic as a stage. See
[Blueprint](blueprint.md).

### Client id

The Kafka client id of one port, `<pipeline>.<streamlet>.<port>`, which labels its lag and rates in the
sidecar's metrics. See [Topics and Kafka clusters](../concepts/topics.md).

### Commit after the write

The rule that an inlet's offsets are committed only once every emit for them is confirmed by the broker.
See [Delivery and failure](../concepts/delivery.md).

### Conformance suite

The suite of scripted conversations an SDK is run against to prove it implements the streamlet protocol
correctly. See [Adding a language SDK](../contributing/language-sdks.md).

### Consumer group

The Kafka consumer group of one inlet, `<pipeline>.<streamlet>.<inlet>`, shared by the streamlet's
replicas and reset by `flow reset`. See [Topics and Kafka clusters](../concepts/topics.md).

### Contract

A port's format and fingerprint. Two ports on one topic connect only when their contracts are equal. See
[Contracts](../concepts/contracts.md).

### Conversation

One `Run` stream between the sidecar and the process, opened with `Start` and carrying every batch until
it stops or fails. See [Streamlet protocol](../reference/protocol.md).

### Delete marker

A Kafka record with a key and no value, which removes that key from a compacted topic. It is not a graph
delta: the Neo4j merge sink passes over it and counts it, and the platform never writes one. See
[Graph deltas](graph-deltas.md#delete-markers).

### Delta topic

A topic with any port of the graph delta contract. A managed one is compacted by default, so it keeps
the latest delta of every element and the graph can be rebuilt from it. See
[Topics and Kafka clusters](../concepts/topics.md#delta-topics).

### Descriptor

The streamlet as the platform sees it — name, ports, contracts and parameters — written as canonical
JSON by the streamlet's SDK. See [Descriptor](../reference/descriptor.md).

### Discovery

The sidecar asking the process to describe itself and comparing the answer with the deployed descriptor
before any record flows. See [The sidecar](../concepts/sidecar.md).

### Element

A node or an edge of a graph the Neo4j merge sink writes, found by its global id and holding the version
of the last delta applied to it. See [Neo4j merge sink](neo4j-merge-sink.md).

### Element key

The Kafka record key every graph delta carries: `node:<id>` or `edge:<id>`, the element's kind and its
id. The sink refuses a delta under any other key. See [Graph deltas](graph-deltas.md#the-record-key).

### Emit

A record the process sends to one of its outlets while handling a batch. See
[Streamlet protocol](../reference/protocol.md).

### Fail

The process's answer that it could not handle a batch. It fails the stream, and the batch is redelivered
from the last commit. See [Delivery and failure](../concepts/delivery.md).

### Fingerprint

The part of a contract compared between ports. For a JSON contract it is the Base64 of the SHA-256 of
the schema name. See [Contracts](../concepts/contracts.md).

### Graph delta

One record of the `ankka.graph-delta.v1` contract: a node merge, an edge merge or a tombstone, stating an
element's whole state at a version. See [Graph deltas](graph-deltas.md).

### Inlet

A port a streamlet reads records from. Every inlet is connected to exactly one topic. See
[Pipelines and streamlets](../concepts/pipelines.md).

### Kafka cluster

A named set of brokers and client settings, held in a `kafka-cluster-<name>` Secret beside the operator,
that topics resolve their connection against. See [Topics and Kafka clusters](../concepts/topics.md).

### Managed topic

A topic the pipeline owns, created by the operator with its declared partitions and replication and
named `<pipeline>.<id>` by default. See [Topics and Kafka clusters](../concepts/topics.md).

### Operator

The Kubernetes controller that turns an `AnkkaFlow` into topics, Secrets and Deployments and reports the
pipeline's status. See [Operator](../reference/operator.md).

### Outlet

A port a streamlet writes records to. An outlet is connected to at most one topic. See
[Pipelines and streamlets](../concepts/pipelines.md).

### Parameter

A typed configuration value a streamlet declares, with an optional default, set per deployment. See
[Configure at deploy time](../deploy/configuration.md).

### Pipeline

A graph of streamlets connected by Kafka topics, described by a blueprint and deployed as one
`AnkkaFlow`. See [Pipelines and streamlets](../concepts/pipelines.md).

### Placeholder

A node an edge names before the node's own delta has arrived, holding only its id; the node's first delta
replaces it. See [Neo4j merge sink](neo4j-merge-sink.md).

### Port

An inlet or an outlet, named and carrying a contract. A blueprint addresses it as `<streamlet>.<port>`.
See [Blueprint](../reference/blueprint.md).

### Process

The streamlet's own container and the program in it, serving the streamlet protocol on
`127.0.0.1:$FLOW_PROCESS_PORT`. See [The sidecar](../concepts/sidecar.md).

### Reset

Moving a pipeline's consumer groups back to the earliest offset so its streamlets reread their inputs
from the start. See [Rebuild from the start](../deploy/reset.md). Resetting the merge sink alone
rebuilds a graph from its delta topic; see
[Rebuild a graph from its delta topic](../deploy/rebuild-a-graph.md).

### Sidecar

The platform's container in every streamlet pod, which owns everything Kafka and drives the process over
the streamlet protocol. See [The sidecar](../concepts/sidecar.md).

### Skip

Acknowledging a batch without emitting for a record. Only the streamlet skips; the platform never does.
See [Delivery and failure](../concepts/delivery.md).

### Stage

Logic that runs inside the sidecar instead of in a process: the logic of a built-in streamlet. Its write
is the write the sidecar commits after. See [The sidecar](../concepts/sidecar.md).

### Stall

A partition that makes no progress because one of its batches fails every time. It shows as lag, a
metric and a `PartitionStalled` event. See [Delivery and failure](../concepts/delivery.md).

### Streamlet

A pipeline stage: code in any language, in its own image, with declared ports and parameters and a
function that turns batches into emits. See [Pipelines and streamlets](../concepts/pipelines.md).

### Streamlet protocol

The gRPC services, `ankka.flow.v1`, between the sidecar and the process. See
[Streamlet protocol](../reference/protocol.md).

### Tombstone

A graph delta that marks an element deleted at a version rather than removing it, so an older delta
cannot bring the element back. It has a value and its element's key, which makes it a different thing
from a delete marker. See [Graph deltas](graph-deltas.md).

### Unmanaged topic

A topic that belongs to something else. The pipeline only reads it and never creates, alters, deletes or
produces to it. See [Topics and Kafka clusters](../concepts/topics.md).