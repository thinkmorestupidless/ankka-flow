# Observe a pipeline

> Read a running pipeline's phase, events, sidecar logs, consumer lag and stall warnings with kubectl and Prometheus.

Source: https://flow.ankka.cloud/deploy/observe/
A pipeline reports at three levels: the `AnkkaFlow`'s status says whether the whole pipeline runs, the
events on it say what the operator did and why, and each pod's sidecar says what one streamlet is doing
through its log and its metrics.

## The phase

```bash
kubectl -n shop get aflow
kubectl -n shop get aflow cart -o wide -w
```

```text
NAME   PIPELINE   PHASE   DETAIL   AGE
cart   cart       Ready            4m
```

| phase | meaning |
|---|---|
| `Pending` | a streamlet is still rolling out |
| `Ready` | every streamlet has all its pods ready and every topic exists |
| `Degraded` | an unmanaged topic is missing or Kafka is unreachable, or a streamlet has fewer ready pods than desired |
| `Failed` | the operator refused the resource and applied nothing; `DETAIL` lists why |

`DETAIL` joins the reasons with `;`, such as `topic 'shop.cart-events.v1' does not exist` or
`router: 1 of 3 ready`. The full status, per streamlet and per topic:

```bash
kubectl -n shop get aflow cart -o jsonpath='{.status}' | jq .
```

A streamlet's pod is ready when its sidecar is: a conversation with the process is running and every
inlet is subscribed to a topic that exists. The process container has no probes of its own.

## Events

The operator records what it did on the `AnkkaFlow`:

```bash
kubectl -n shop get events --field-selector involvedObject.kind=AnkkaFlow
```

`TopicCreated`, `StreamletRolled`, `StreamletRemoved` and `ResetOffsets` are Normal. Every Warning names
something to act on; each is described in [Troubleshooting](troubleshooting.md). Stalled partitions are
recorded by the sidecar on its own pod:

```bash
kubectl -n shop get events --field-selector reason=PartitionStalled
```

## Logs

Every pod has two containers: `sidecar` and `process`.

```bash
kubectl -n shop logs deploy/flow-cart-router -c sidecar
kubectl -n shop logs deploy/flow-cart-router -c process
```

The sidecar logs discovery, each conversation, partitions assigned and revoked, every stream failure with
its cause, and every reconnect. When the sidecar refuses to start because the process does not match the
deployed descriptor, the problems are in both logs: the sidecar sends them to the process before it
exits. No record value is ever logged.

## Lag and throughput

Each sidecar serves Prometheus metrics on port 2050. Every Kafka client's id is
`<pipeline>.<streamlet>.<port>`, so lag is attributed to one streamlet's inlet:

```bash
kubectl -n shop port-forward deploy/flow-cart-router 2050 &
curl -s localhost:2050/metrics | grep records_lag
```

```text
kafka_consumer_consumer_fetch_manager_metrics_records_lag{client_id="cart.router.in",partition="0",topic="shop_cart-events_v1"} 0.0
```

Kafka writes topic names in these labels with dots replaced by underscores. The metrics worth watching:

| metric | watch for |
|---|---|
| `kafka_consumer_consumer_fetch_manager_metrics_records_lag` | lag per inlet partition that grows and does not fall |
| `kafka_consumer_consumer_fetch_manager_metrics_records_consumed_rate` | an inlet that stops reading |
| `kafka_producer_producer_metrics_record_send_rate` | emits per outlet |
| `ankka_flow_sidecar_stalled_seconds` | a partition that has not committed for a long time |
| `ankka_flow_sidecar_in_flight` | a batch that stays with the process |

Every metric is listed in [the sidecar reference](../reference/sidecar.md#metrics).

## Scraping with Prometheus

Streamlet pods carry `prometheus.io/scrape: "true"` and `prometheus.io/port: "2050"`, and the port is
named `metrics`. A Prometheus that discovers pods by those annotations scrapes every sidecar without
further configuration. With the Prometheus Operator, a PodMonitor selecting
`app.kubernetes.io/managed-by: ankka-flow` on port `metrics` does the same:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: ankka-flow
  namespace: shop
spec:
  selector:
    matchLabels:
      app.kubernetes.io/managed-by: ankka-flow
  podMetricsEndpoints:
    - port: metrics
```

Pods also carry `flow.ankka.thinkmorestupidless.com/pipeline` and
`flow.ankka.thinkmorestupidless.com/streamlet` labels, which a relabelling rule can turn into series
labels.

## Stalled partitions

A batch the process fails is redelivered from the last commit, indefinitely; nothing is skipped. A batch
that fails every time therefore stalls its partition. It shows three ways: that partition's lag grows,
`ankka_flow_sidecar_stalled_seconds` rises, and after `FLOW_STALL_WARNING_AFTER` (five minutes by default)
the sidecar records one `PartitionStalled` Warning on its pod, naming the inlet, the partition and the
last error. The fix is in the streamlet's code: skip the record by acknowledging without emitting, or
correct what makes it fail. See [Delivery and failure](../concepts/delivery.md).

The stall is measured from the first attempt at the batch, across the sidecar's reconnects, so the
warning appears even though every failure tears the stream down and starts it again.

## A built-in stage

A built-in streamlet's pod, such as the [Neo4j merge sink](../reference/neo4j-merge-sink.md), exports
every inlet metric, plus three counters of its own per inlet partition:

| Metric | Meaning |
|---|---|
| `ankka_flow_stage_deltas_written_total` | deltas applied to the graph |
| `ankka_flow_stage_deltas_stale_total` | deltas found stale: folded away within a batch, or older than what the graph holds |
| `ankka_flow_stage_batches_failed_total` | batches whose transaction failed or whose records could not be read |

A rising written count with lag near zero is a healthy sink. A replay from the start, or a burst of
redelivery, shows as stale deltas rather than written ones: nothing in the graph changed. Failed
batches beside growing lag mean the database is refusing or not answering; the sidecar's log and the
`PartitionStalled` note carry the reason.

A sink whose credentials may not create the uniqueness constraint it needs records one
`ConstraintNotCreated` Warning on its pod each time it opens, and keeps going. Merges then scan the
graph instead of looking elements up, so throughput falls as the graph grows. Create the constraint
with an account that may:

```cypher
CREATE CONSTRAINT element_id IF NOT EXISTS FOR (n:Element) REQUIRE n.id IS UNIQUE
```
