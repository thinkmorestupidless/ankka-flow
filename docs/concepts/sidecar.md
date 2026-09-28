# The sidecar

Every streamlet pod has two containers. Your container holds only your code. The **sidecar** holds
everything to do with Kafka: subscribing, batching, producing, committing, consumer groups, lag and
resets. The two talk over a small gRPC protocol on the pod's loopback interface.

| container | owns |
|---|---|
| `process` (your image) | the streamlet's logic. It listens on `127.0.0.1:$FLOW_PROCESS_PORT` and has no ports, probes, mounts or secrets. |
| `sidecar` (the platform's image) | Kafka, the descriptor check, readiness and liveness, the Prometheus metrics, the Kafka credentials. |

Nothing in a pipeline names the sidecar's image. The operator knows it from its own configuration,
so upgrading the platform upgrades every pipeline's sidecar on its next rollout.

## Start-up

1. The sidecar asks your process to describe itself (`Discover`), retrying until it answers.
2. It compares the answer with the descriptor it was deployed with. Any difference, an unsupported
   protocol major, or an invalid descriptor refuses start-up. Every problem is sent to your process
   first, so it appears in your own log.
3. It opens one conversation (`Run`) and subscribes to every inlet. The pod is ready when every inlet
   is subscribed and its topic exists.

## The conversation

The sidecar sends batches of records, as bytes with their keys and headers. At most one batch per
inlet partition is in flight, so each partition's records arrive in order. Batches of different
partitions interleave.

Your process answers each batch with any number of emits, each naming an outlet, and then one
acknowledgement. To skip a record, acknowledge the batch without emitting for it.

## Commit after the write

When a batch is acknowledged, the sidecar produces every emit and waits for the broker to confirm
each one. Only then does it commit the batch's offsets. Delivery is **at least once**: a record
whose emits were written but whose offsets were not yet committed is delivered again after a crash.
Your logic should tolerate seeing a record twice.

## When something fails

A failed batch, a protocol violation, a failed write, or your process going away all fail the
stream. The sidecar then:

1. discards every batch in flight, committing nothing for them;
2. reports the pod not ready;
3. waits (500 ms, doubling to 30 s), repeats discovery, and starts a new conversation;
4. resumes from the last committed offsets.

It does this indefinitely. The sidecar never skips a record and never sends one to a dead-letter
topic. A batch that fails every time stalls its partition; its lag grows, and after
`FLOW_STALL_WARNING_AFTER` (default five minutes) the sidecar records a `PartitionStalled` warning
event on its pod.

A partition taken away by a rebalance while its batch is in flight is not a failure. That batch's
acknowledgement is discarded, nothing is committed for it, and the new owner reads it again.

## Rebuilding from the start

To reprocess a pipeline's inputs from the beginning: scale its streamlets to zero, run
`flow reset <pipeline>`, and scale them back up. The operator moves each inlet's consumer group to
the earliest offset and records one event per group. See the [CLI reference](../reference/cli.md).

## Metrics

Each sidecar serves Prometheus metrics on port 2050. Kafka's own consumer lag and producer rates
are labelled with a client id of `<pipeline>.<streamlet>.<port>`, so lag is attributable to a
streamlet's inlet. See the [sidecar reference](../reference/sidecar.md).
