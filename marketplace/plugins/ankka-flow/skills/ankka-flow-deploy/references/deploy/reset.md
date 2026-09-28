# Rebuild from the start

> Reprocess a pipeline's inputs from the earliest offsets by stopping its streamlets, requesting a reset with flow reset, and starting them again.

Source: https://flow.ankka.cloud/deploy/reset/
A pipeline is rebuilt by moving its streamlets' consumer groups back to the earliest offset of every
topic they read, so they read their inputs again from the beginning. Kafka refuses to move a group that
still has members, so the streamlets are stopped first, reset, and started again. The operator does the
Kafka work, over the same connection each streamlet uses.

Each inlet has its own consumer group, `<pipeline>.<streamlet>.<inlet>`, so a reset can target some
streamlets and leave the rest where they are. Resetting reprocesses records; it does not delete what
downstream topics already hold, so downstream streamlets see those records again.

## Stop the streamlets

Set `replicas` to 0 for every streamlet to reset, through deploy-time configuration, and apply the
regenerated resource:

```hocon
# stop.conf
flow.streamlets.router { replicas = 0 }
```

```bash
flow generate blueprint.conf --descriptors flow --conf prod.conf --conf stop.conf \
  --image router=registry.example.com/cart-router:1.2 -n shop | kubectl apply -f -
kubectl -n shop get pods -l flow.ankka.thinkmorestupidless.com/pipeline=cart
```

Wait until the streamlets' pods are gone. Scaling the Deployment directly does not work: the operator
restores the resource's `replicas`, and `flow reset` reads `replicas` from the resource.

## Request the reset

```bash
flow reset cart -n shop
```

```text
reset requested for 'cart': 1b7c2f0e-9a4d-4d3e-8f55-2f1c0b6e9a71
```

With no `--streamlet`, every streamlet with an inlet is reset. `--streamlet router`, repeatable, limits
it to the named ones. `flow reset` refuses, and changes nothing, when a target still has `replicas` other
than 0 or has pods left, when a named streamlet does not exist or has no inlets, or when the pipeline is
not found. Every refusal is listed in [the CLI reference](../reference/cli.md#flow-reset).

The request is an annotation on the `AnkkaFlow`. The operator checks the same guards again, because the
CLI's view of the pods may be stale: while a target runs it records `ResetRefused` with
`reset <id> waits: …` and tries again every few seconds.

## Check that it happened

The operator records one event per consumer group:

```bash
kubectl -n shop get events --field-selector involvedObject.kind=AnkkaFlow,reason=ResetOffsets
```

```text
REASON         OBJECT            MESSAGE
ResetOffsets   ankkaflow/cart    cart.router.in: 3 partition(s) of 'shop.cart-events.v1' to earliest
```

A group that could not be reset is `ResetOffsetsFailed`, with Kafka's reason; the pipeline itself is not
failed by it. When every group has been handled, the operator writes the request's id to the annotation
`flow.ankka.thinkmorestupidless.com/reset-offsets-done`, so the same request is never carried out again,
even after the operator restarts. A new reset is a new request with a new id.

## Start the streamlets again

Apply the resource without the stop configuration:

```bash
flow generate blueprint.conf --descriptors flow --conf prod.conf \
  --image router=registry.example.com/cart-router:1.2 -n shop | kubectl apply -f -
```

The streamlets start at the earliest offsets, and their lag, labelled
`client_id="<pipeline>.<streamlet>.<inlet>"`, starts at the size of each input and falls to zero as they
catch up. See [Observe a pipeline](observe.md).
