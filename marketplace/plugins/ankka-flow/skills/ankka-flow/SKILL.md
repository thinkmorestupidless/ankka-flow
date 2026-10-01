---
name: ankka-flow
description: What ankka-flow is and how a pipeline behaves — streamlets with typed inlets and outlets wired by a blueprint over Kafka topics, the sidecar that owns everything Kafka in every pod, JSON contracts matched by schema name and fingerprint, managed and unmanaged topics, commit after the write, at-least-once delivery, never skipping, stalled partitions, and when a design is an ankka consumer rather than a flow. Use for designing a pipeline, choosing between ankka and ankka-flow, writing or reviewing a blueprint, or any question about ankka-flow that is not specifically writing a Python streamlet, deploying, or implementing the protocol; load it first when unsure which skill applies. Also building a graph from events with graph deltas keyed by element, compacted delta topics, and the built-in Neo4j merge sink.
---

# ankka-flow

ankka-flow runs streaming pipelines beside ankka. A pipeline is a graph of **streamlets**, each with
named inlets and outlets, connected by Kafka **topics** according to a **blueprint**. A streamlet's
logic is written in any language and shipped as an image that holds only that code. A **sidecar** the
platform adds to every pod owns everything Kafka — subscribing, batching, producing, committing,
consumer groups, lag and resets — and talks to the streamlet's process over a gRPC protocol on the
pod's loopback interface.

## Rules

1. **A single reaction to a topic is an ankka consumer, not a flow.** Reach for ankka-flow when a
   design needs several of: a graph of stages whose contracts are checked against each other before
   anything runs, several typed outlets per stage, topics the platform creates and owns, per-key
   ordering through a chain, a rebuild from the start of the inputs, lag per stage, or sinks that
   commit only after writing elsewhere.
2. **Ports connect by contract, checked before anything runs.** A contract is a format (only `json`)
   and a schema name such as `cart-events.v1`; the fingerprint is Base64 of the SHA-256 of that name.
   An outlet and an inlet on one topic must carry equal contracts. A new version is a new name.
3. **The sidecar never decodes for a process.** A record's value is bytes from Kafka to the process
   and back. Decoding, and deciding what to do with a record that will not decode, is the streamlet's
   job. A built-in streamlet (`builtin/<name>`, no image, a pod with only the sidecar) decodes its own
   contract and nothing else.
4. **Delivery is at least once.** Offsets are committed only after every emit of the batch is
   confirmed by the broker. A crash between the write and the commit delivers the record again, so
   streamlet logic must tolerate repeats. There is no exactly-once.
5. **Nothing is ever skipped by the platform.** A failed batch is redelivered from the last commit,
   indefinitely; a batch that always fails stalls its partition and shows as lag, a metric and a
   `PartitionStalled` event. Skipping is the streamlet's decision: acknowledge without emitting.
6. **Order is per partition.** At most one batch per inlet partition is in flight, so records with the
   same key stay in order through a chain of streamlets that keep the key. Batches of different
   partitions interleave, and the assignment changes on every rebalance: never keep per-partition
   state in the process.
7. **Managed topics are the pipeline's; unmanaged topics are someone else's.** The operator creates a
   managed topic (named `<pipeline>.<topic id>` unless `topic.name` says otherwise) and never changes
   an existing one. An unmanaged topic is only read: never created, altered, deleted or produced to;
   it is named by `topic.name`, else its bare topic id. Neither `managed` nor `topic.name` can be
   overridden at deploy time.
8. **The resource says what runs.** `flow generate` merges deploy-time configuration into the
   `AnkkaFlow` resource; the operator adds only the sidecar image and Kafka cluster secrets.
9. **A graph is built from state-shaped, versioned deltas.** Map events to `ankka.graph-delta.v1`
   in an ordinary streamlet and wire `builtin/neo4j-merge-sink` behind it. Each delta is an element's
   whole state with a global id and a version that rises with its one source entity. Redelivery,
   reordering and a replay from the start then leave the same graph. No increments, no Cypher from
   the pipeline; tombstones mark rather than delete.
10. **A delta's record key is its element key, and the sink enforces it.** `node:<id>` for a node
   merge or node tombstone, `edge:<id>` for an edge merge or edge tombstone, the id verbatim. The
   sink fails the batch for a delta with no key or another key, naming the key expected. In Python,
   `GraphDeltaOutlet` builds the key; in any other language the writer sets it.
11. **A managed delta topic is compacted by default.** A topic with any port of the delta contract
   gets `cleanup.policy = compact` from `flow generate` unless the blueprint or `--conf` sets a
   policy, and `flow verify` says so in a note. It then holds the latest delta per element, so an
   empty database is filled by resetting the sink alone. The platform writes no delete markers and
   passes over any it reads; a tombstone stays in the topic.

## Before answering

- Is this a pipeline at all, or one ankka consumer? Apply rule 1 first.
- Which topics are managed and which belong to something else? Unmanaged topics may have no producers
  in the blueprint.
- Does every inlet's contract equal the contract of every outlet on its topic?
- Can the logic tolerate a record twice, and is anything relying on per-partition state?
- Does a limitation apply? Check `references/reference/limitations.md` before promising a feature.

## Mistakes to check for

- Proposing Avro, Protobuf or a schema registry for a contract; only JSON by schema name exists.
- A dead-letter topic or a "skip after N retries" setting; neither exists, by design.
- A blueprint that produces to an unmanaged topic, or leaves an inlet connected to nothing.
- Kafka settings or credentials in the streamlet's own container; they belong to the sidecar.
- An HTTP or gRPC ingress into a pipeline; records enter through a Kafka topic.
- A graph pipeline whose mapper writes to Neo4j itself, or emits increments rather than whole state.
- A delta keyed by the bare element id, by the input record's key, or not at all; the key is
  `node:<id>` or `edge:<id>`.
- `cleanup.policy = delete` on a delta topic that is expected to rebuild the graph, or a delete
  marker written for an element that was never tombstoned.

## Reference files

Open the one a task needs; each is one topic and stands alone.

### Start here

- `references/index.md` — Streaming pipelines beside ankka — streamlets in any language, wired by a blueprint over Kafka topics, with a sidecar in every pod that owns everything Kafka.

### Get started

- `references/get-started/install.md` — Install what ankka-flow's build needs, then build the flow CLI, the sidecar and operator images, and the sample streamlet's image from source.
- `references/get-started/first-streamlet.md` — Run the sample cart router on a laptop — test it with the harness, check its descriptor, start Kafka and the sidecar in containers, and watch records flow through it and survive a restart.
- `references/get-started/deploy-locally.md` — Install the operator and a development Kafka on a kind cluster, deploy the sample cart router as a pipeline with flow generate and kubectl, and watch it become Ready.
- `references/get-started/coding-agents.md` — Give a coding agent this documentation as skills from the ankka marketplace, or as llms.txt and Markdown pages, and know what each skill carries.

### Concepts

- `references/concepts/pipelines.md` — What a pipeline is — streamlets with typed inlets and outlets, wired by a blueprint over Kafka topics — and when a design needs one rather than an ankka consumer.
- `references/concepts/sidecar.md` — The container the platform runs beside every streamlet, which owns everything Kafka and drives the streamlet's process over a gRPC protocol on the pod's loopback interface.
- `references/concepts/delivery.md` — How the sidecar delivers records — commit only after the write, at least once, never skipping — and what happens when a batch fails, a partition stalls or a rebalance moves a partition.
- `references/concepts/contracts.md` — What a port's contract is — a format and a fingerprint — how two ports are matched by it before anything runs, and why the sidecar never decodes a record.
- `references/concepts/topics.md` — Managed and unmanaged topics, how a topic's Kafka name is chosen, how its brokers and settings resolve from the blueprint and a Kafka cluster Secret, and how consumer groups and client ids are named.

### Build

- `references/build/blueprints.md` — Write the blueprint that wires a pipeline's streamlets together over Kafka topics, from naming the streamlets to checking the result with flow verify.
- `references/build/ankka-topics.md` — Build a pipeline on the messages an ankka service publishes — give the service a broker in its descriptor, declare its topic unmanaged in the blueprint, and decode ankka's CloudEvents in a streamlet.
- `references/build/graph-sink.md` — Turn a service's events into a Neo4j graph — choose ids and versions, map events to keyed graph deltas in a streamlet, and wire the built-in Neo4j merge sink behind it.

### Run and operate

- `references/deploy/rebuild-a-graph.md` — Fill an empty Neo4j database from a compacted topic of graph deltas by resetting only the merge sink, with no mapper running and no upstream topic read.

### Reference

- `references/reference/blueprint.md` — Every key of the blueprint file, the HOCON that names a pipeline's streamlets and the topics connecting their ports, and every rule flow verify checks it against.
- `references/reference/graph-deltas.md` — The ankka.graph-delta.v1 contract a mapping streamlet writes and the Neo4j merge sink reads — node merges, edge merges and tombstones, each a versioned statement of state under its element key.
- `references/reference/limitations.md` — What ankka-flow does not do — delivery guarantees, contract formats, SDKs, ingress, brokers and tooling — stated plainly so a design does not depend on it.
- `references/reference/glossary.md` — Every term ankka-flow uses — streamlet, port, blueprint, contract, descriptor, sidecar, conversation, managed topic and the rest — each defined in a sentence with the page that explains it.
