---
title: Rebuild from the start
description: Reprocess a pipeline's inputs from the earliest offsets by stopping its streamlets, requesting a reset with flow reset, and starting them again; a Flink streamlet starts again with no state.
kind: guide
related: [reference/cli.md, reference/resource.md, deploy/configuration.md, concepts/delivery.md]
---

# Rebuild from the start

A pipeline is rebuilt by moving its streamlets' consumer groups back to the earliest offset of every
topic they read, so they read their inputs again from the beginning. Kafka refuses to move a group that
still has members, so the streamlets are stopped first, reset, and started again. The operator does the
Kafka work, over the same connection each streamlet uses.

Each inlet has its own consumer group, `<pipeline>.<streamlet>.<inlet>`, so a reset can target some
streamlets and leave the rest where they are. Resetting reprocesses records; it does not delete what
downstream topics already hold, so downstream streamlets see those records again.

A Flink streamlet's state goes with its offsets: a reset streamlet starts with no state, at a new
**state epoch**. Every path it checkpoints to names the epoch, and so does the prefix of its
transactions, so nothing of the earlier epoch is read again. The earlier epoch's files stay in the
state store until the store's own lifecycle rules remove them; the operator deletes nothing there.

## Stop the streamlets

Set `replicas` to 0 for every streamlet to reset, through deploy-time configuration, and apply the
regenerated resource:

```hocon
# stop.conf
flow.streamlets.cleaner { replicas = 0 }
flow.streamlets.counter { replicas = 0 }
```

```bash
flow generate blueprint.conf --descriptors flow --conf prod.conf --conf stop.conf \
  --images images.conf -n shop | kubectl apply -f -
kubectl -n shop get pods -l flow.ankka.thinkmorestupidless.com/pipeline=carts
```

Wait until the Pekko Streams streamlets' pods are gone and each Flink streamlet's status says
`suspended`: `replicas = 0` suspends its job, keeping its state, and its job manager stops. Scaling a
Deployment directly does not work: the operator restores the resource's `replicas`, and `flow reset`
reads `replicas` from the resource.

## Request the reset

```bash
flow reset carts -n shop
```

```text
reset requested for 'carts': 1b7c2f0e-9a4d-4d3e-8f55-2f1c0b6e9a71
```

With no `--streamlet`, every streamlet with an inlet is reset. `--streamlet counter`, repeatable, limits
it to the named ones. `flow reset` refuses, and changes nothing, when a target still has `replicas` other
than 0 or has pods left, when a named streamlet does not exist or has no inlets, or when the pipeline is
not found. Every refusal is listed in [the CLI reference](../reference/cli.md#flow-reset).

The request is an annotation on the `AnkkaFlow`. The operator checks the same guards again, because the
CLI's view of the pods may be stale: while a target runs, or a Flink streamlet's job is not yet
suspended, it records `ResetRefused` with `reset <id> waits: …` and tries again every few seconds.

For each Flink streamlet it then deletes the streamlet's `FlinkDeployment`, which removes the
job's high-availability records, advances its state epoch, recorded in the resource's annotation
`flow.ankka.thinkmorestupidless.com/state-epochs`, and records `StateDiscarded`, naming the new
epoch and where the earlier one's files are. The `FlinkDeployment` is made again, suspended, at
the new epoch.

## Check that it happened

The operator records one event per consumer group:

```bash
kubectl -n shop get events --field-selector involvedObject.kind=AnkkaFlow,reason=ResetOffsets
```

```text
REASON         OBJECT            MESSAGE
ResetOffsets   ankkaflow/carts   carts.cleaner.in: 3 partition(s) of 'shop.cart-events.v1' to earliest
ResetOffsets   ankkaflow/carts   carts.counter.in: 6 partition(s) of 'carts.clean-carts' to earliest
```

A group that could not be reset is `ResetOffsetsFailed`, with Kafka's reason; the pipeline itself is not
failed by it. When every group has been handled, the operator writes the request's id to the annotation
`flow.ankka.thinkmorestupidless.com/reset-offsets-done`, so the same request is never carried out again,
even after the operator restarts. A new reset is a new request with a new id.

## Start the streamlets again

Apply the resource without the stop configuration:

```bash
flow generate blueprint.conf --descriptors flow --conf prod.conf \
  --images images.conf -n shop | kubectl apply -f -
```

The streamlets start at the earliest offsets, a Flink streamlet with no state, and each inlet's lag
on the pipeline's status starts at the size of its input and falls as they catch up. See
[Observe a pipeline](observe.md).
