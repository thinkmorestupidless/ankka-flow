# Delivery and failure

> What each engine promises about delivery — a Pekko Streams streamlet at least once, committing only after the write; a Flink streamlet once between managed topics — and what happens when a streamlet fails.

Source: https://flow.ankka.cloud/concepts/delivery/
Every streamlet reads its inlets as a Kafka consumer group and writes its outlets as a Kafka
producer. What it promises when something fails depends on its engine.

| Streamlet | Outlet on | Delivery |
|---|---|---|
| Pekko Streams | any topic | at least once |
| Flink | a topic the pipeline manages | each record counted once |
| Flink | a topic it does not manage, or an ankka topic | at least once |

Nothing is skipped by the platform. A record is only ever dropped because the streamlet decided to
drop it, by emitting nothing for it.

## A Pekko Streams streamlet: commit after the write

An inlet's offset is committed only after everything the streamlet produced from that record, and
from every record before it on the same partition, has been written to Kafka and acknowledged by
the broker. A record turned into several emits is committed after the last of them.

A failure anywhere in an inlet's stream (a record the streamlet's code throws on, a write the broker
refuses, a lost connection) stops that inlet's stream and starts it again from the last commit,
after a backoff that grows from one second to thirty, indefinitely. Records whose results were
written but not yet committed are read and processed again, so a reader downstream may see a result
twice. A rebalance that moves a partition to another pod is the same: its new owner starts from the
last commit.

A record that fails every time is read again every time, and its partition makes no progress. The
inlet's lag on the pipeline's status grows while the other partitions carry on. Clearing it is a
change to the streamlet's code, configuration or data, never to the platform.

Per key, order is kept: records with one key are on one partition, read in offset order, and a
streamlet that keeps each record's key when it emits keeps that order through the next topic.

## A Flink streamlet: once between managed topics

A Flink streamlet's job takes a checkpoint every checkpoint interval (ten seconds unless the
deploy-time configuration sets `flow.streamlets.<name>.flink.checkpoint-interval`): its state, and the offset of every
inlet partition, written together to the state store. An outlet on a topic the pipeline manages
writes in a Kafka transaction that commits with the checkpoint. When a pod fails, the job restarts
from the last completed checkpoint: the state as it was, the inlets read from the offsets it
recorded, and every transaction opened since aborted. A window is written once.

Every inlet on a managed topic, of either engine, reads with `isolation.level=read_committed`, so a
reader downstream of a Flink streamlet sees only committed records. It sees them one checkpoint
interval after the Flink streamlet wrote them.

An outlet on a topic the pipeline does not manage, or on an ankka topic, is written at least once: a
transaction there needs rights the pipeline is not given, so after a restart it may write again what
it wrote after the last checkpoint.

The records a Flink streamlet writes carry the timestamp its job gives them: for a window, the
window's last millisecond, not the time it was written.

The inlet's consumer group is committed on each checkpoint, so the lag the status reports for a
Flink streamlet trails by up to one checkpoint interval.

## When the state cannot be read

A new image of a Flink streamlet resumes from the state the old one left. When it cannot read that
state, its job fails, the pipeline is `Degraded` with the condition `StateUnreadable`, and the state
store is left as it was. Deploy the previous image again, or [reset](../deploy/reset.md) the
streamlet, which starts it at a new state epoch with no state.
