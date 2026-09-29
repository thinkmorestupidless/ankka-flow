# Delivery and failure

> How the sidecar delivers records — commit only after the write, at least once, never skipping — and what happens when a batch fails, a partition stalls or a rebalance moves a partition.

Source: https://flow.ankka.cloud/concepts/delivery/
The sidecar gives one guarantee and keeps it without exception: an inlet's offsets are committed only
after everything the process emitted for those records has been written to Kafka and confirmed by the
broker. Delivery is therefore **at least once**, nothing is ever skipped by the platform, and a record
is only ever skipped because the streamlet decided to skip it.

## Commit after the write

When the process acknowledges a batch, the sidecar:

1. produces every emit of the batch to its outlet's topic;
2. waits for the broker to confirm every one of those writes;
3. only then commits the batch's offsets on the inlet's consumer group.

Batches of one partition are written and committed in the order they were read, one after another. A
batch whose writes fail is never committed, and nothing read after it on that partition is committed
either.

## At least once

A crash, a restart or a failure between step 2 and step 3 leaves records whose emits were written but
whose offsets were not committed. The next reader of the partition starts from the last committed
offset and delivers those records again, and the streamlet emits for them again. Streamlet logic must
tolerate seeing a record twice, and a sink that writes elsewhere must tolerate writing it twice. There
is no exactly-once mode.

A repeat never reorders a key: redelivery starts from the last commit and proceeds in offset order.

## Ordering

At most one batch per inlet partition is with the process at a time, and a batch's records are in
offset order. Records with the same key land on the same partition, so they reach the process in the
order they were produced. A streamlet that keeps each record's key when it emits keeps that order
through the next topic, and so through a chain of streamlets.

Batches of different partitions interleave and may be processed concurrently. Which partitions a pod
holds changes on every rebalance, so a streamlet must never keep state per partition in memory.

## Skipping is the streamlet's decision

The sidecar never skips a record and never sends one to a dead-letter topic. A streamlet that does
not want a record — including one it cannot decode — acknowledges the batch without emitting for that
record. The offset is committed with the rest of the batch and the record is gone from the pipeline's
point of view.

## When something fails

Any of these fails the stream:

- the process fails a batch;
- the process breaks the protocol: an emit to an outlet it did not declare, an emit after its batch's
  acknowledgement, an acknowledgement for a batch that is not in flight;
- a write to Kafka fails;
- the process becomes unreachable or closes the conversation;
- a record is larger than the protocol's per-record limit (just under 4 MiB).

The sidecar then:

1. discards every batch in flight, committing nothing for them;
2. reports the pod not ready;
3. waits, starting at 500 ms and doubling up to `FLOW_RECONNECT_MAX_BACKOFF` (30 s by default);
4. repeats discovery, opens a new conversation and rebuilds its consumers and producers;
5. resumes every partition from its last committed offset.

It does this indefinitely; it never gives up and never exits for a failed batch. A conversation that
then runs for longer than twice the maximum backoff resets the wait to 500 ms. The only thing that
makes the sidecar exit is a discovery refusal: a process whose descriptor differs from the deployed
one.

## Stalled partitions

A batch that fails every time is redelivered every time, so its partition makes no progress: it
**stalls**. Other partitions of the same inlet carry on. A stall is visible three ways:

- the inlet's consumer lag on that partition grows;
- the sidecar's `ankka_flow_sidecar_stalled_seconds` gauge reports the age of the oldest uncommitted
  batch of that partition;
- once the stall passes `FLOW_STALL_WARNING_AFTER` (five minutes by default), the sidecar records one
  `PartitionStalled` warning event on its pod, naming the pipeline, streamlet, inlet, partition and the
  last error. Outside Kubernetes it logs the same warning.

The stall is measured from the first attempt at the batch. Each failure tears the stream down and the
batch is read again after the reconnect, but the clock keeps running across those reconnects; it
resets only when the partition is revoked and moves to another pod.

The stall clears when a batch of that partition commits. Clearing it is a change to the streamlet's
code or configuration, or to the data it depends on, not to the platform: the platform will not move
past the record for it.

## Rebalances

When a rebalance takes a partition away from a pod while one of its batches is in flight, that is not
a failure. The batch's acknowledgement and emits are discarded, nothing more is committed for the
partition on that pod, and the new owner reads it again from the last commit. Emits the old owner had
already written are written again by the new owner: one more case of at least once.
