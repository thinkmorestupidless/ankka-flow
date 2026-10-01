# The sidecar

> The container the platform runs beside every streamlet, which owns everything Kafka and drives the streamlet's process over a gRPC protocol on the pod's loopback interface.

Source: https://flow.ankka.cloud/concepts/sidecar/
Every streamlet pod has two containers. The streamlet's container holds only its code. The
**sidecar** holds everything to do with Kafka: subscribing, batching, producing, committing, consumer
groups, lag and resets. The two talk over a small gRPC protocol on the pod's loopback interface.

| Container | Owns |
|---|---|
| `process` (the streamlet's image) | the streamlet's logic. It listens on `127.0.0.1:$FLOW_PROCESS_PORT` and has no ports, probes, mounts or secrets. |
| `sidecar` (the platform's image) | Kafka, the descriptor check, readiness and liveness, the Prometheus metrics, the Kafka credentials. |

Because the sidecar never decodes a record for a process, one sidecar serves every language. Nothing in a pipeline
names the sidecar's image: the operator knows it from its own configuration, so upgrading the platform
upgrades every pipeline's sidecar on its next rollout.

## A pod with only the sidecar

A streamlet whose descriptor is built into the platform — named `builtin/<name>` in a blueprint — has
no process container at all. Its logic runs inside the sidecar as a **stage**: batches from Kafka go
to the stage instead of across the protocol, and the offsets are committed only once the stage's own
write has completed. A built-in stage is the one thing in the sidecar that decodes records, and it
decodes only its own contract. The [Neo4j merge sink](../reference/neo4j-merge-sink.md) is the one
built-in stage; it merges [graph deltas](../reference/graph-deltas.md) into Neo4j.

## Start-up

1. The sidecar reads the deployed descriptor and its `streamlet.conf` from its configuration
   directory. A file that does not parse, or ports that do not match the descriptor's, refuse start-up.
2. It asks the process to describe itself (`Discover`), retrying with a backoff from 500 ms to 10 s
   for as long as the process does not answer. The process may start before or after the sidecar.
3. It compares the answer with the deployed descriptor. A difference, an incompatible protocol
   version, or an invalid descriptor refuses start-up: the sidecar sends every problem to the process
   (`ReportError`), so it appears in the streamlet's own log, and exits.
4. It opens one conversation (`Run`), sends `Start` with the streamlet's resolved parameters, and
   subscribes to every inlet. The pod is ready when every inlet has joined its consumer group and every
   inlet's topic exists.

## The conversation

The sidecar sends batches of records, each record as bytes with its key and headers. A batch is
whatever arrived on one inlet partition while the previous batch of that partition was with the
process, capped at a number of records and a number of bytes. At most one batch per inlet partition is
in flight, so each partition's records arrive in order; batches of different partitions interleave.

The process answers each batch with any number of emits, each naming an outlet, and then one
acknowledgement, or one failure instead. To skip a record, the process acknowledges the batch without
emitting for it.

What the sidecar does with an acknowledgement — produce every emit, wait for the broker, then
commit — and what it does when anything fails is on [Delivery and failure](delivery.md). The messages
themselves are on [Streamlet protocol](../reference/protocol.md).

## Readiness and liveness

The sidecar is the only container with probes. It is ready while its conversation runs, every inlet
is subscribed and every inlet's topic exists, and not ready from the moment the process becomes
unreachable or the conversation is being rebuilt. Its liveness reflects its own loop, not the
process's health: a process that fails every batch leaves the pod alive but its partitions stalled.

## Metrics

Each sidecar serves Prometheus metrics on port 2050: Kafka's own consumer lag and producer rates, and
two gauges of its own for batches in flight and how long a partition has gone without a commit.
Kafka's metrics are labelled with a client id of `<pipeline>.<streamlet>.<port>`, so lag is
attributable to one streamlet's inlet. The full list is on [Sidecar](../reference/sidecar.md).
