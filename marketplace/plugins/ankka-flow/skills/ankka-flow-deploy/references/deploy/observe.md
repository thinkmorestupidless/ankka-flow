# Observe a pipeline

> Read a running pipeline's phase, conditions, events, each inlet's consumer lag and every streamlet's logs, whatever its engine, with kubectl and flow mcp.

Source: https://flow.ankka.cloud/deploy/observe/
A pipeline reports at three levels: the `AnkkaFlow`'s status says whether the whole pipeline runs
and how far behind each inlet is, the events on it say what the operator did and why, and each
streamlet's pods say what it is doing through their logs.

## The phase

```bash
kubectl -n shop get aflow
kubectl -n shop get aflow carts -o wide -w
```

```text
NAME    PIPELINE   PHASE   DETAIL   AGE
carts   carts      Ready            4m
```

| phase | meaning |
|---|---|
| `Pending` | a streamlet is still rolling out |
| `Ready` | every streamlet runs and every topic exists |
| `Degraded` | an unmanaged topic is missing or Kafka is unreachable, a streamlet has fewer ready pods than desired, or a streamlet failed |
| `Failed` | the operator refused the resource and applied nothing; `DETAIL` lists why |

A Pekko Streams streamlet runs when its Deployment is rolled out with every pod ready. A Flink
streamlet runs when its `FlinkDeployment` is `STABLE` with its job `RUNNING`; at `replicas = 0` its
job is suspended and its status says so.

`DETAIL` names what is not running, such as `topic 'shop.cart-events.v1' does not exist` or
`cleaner: 1 of 3 ready`.

## Conditions

The status lists conditions as Kubernetes resources do. `Ready` is always there, `True` or `False`;
each other condition is listed while it holds:

```bash
kubectl -n shop get aflow carts -o jsonpath='{.status.conditions}' | jq .
```

| Condition | Holds while |
|---|---|
| `FlinkOperatorMissing` | the pipeline has a Flink streamlet and the Flink Kubernetes Operator is not installed |
| `StateStoreMissing` | the pipeline has a Flink streamlet and the Secret `flink-state` does not exist |
| `TopicNotGranted` | the pipeline's machine holds no grant for a topic it reads or writes |
| `MachineCredentialRefused` | ankka refuses the machine's credentials |
| `StateUnreadable` | a Flink streamlet's new image cannot read the state the previous one left |

Each is described, with its remedy, in [Troubleshooting](troubleshooting.md). The operator looks again
at every pipeline every 30 seconds, so a condition appears, and clears, within a minute of its cause.

## Lag

The status holds every streamlet's inlets, whatever the engine, with the topic, the consumer group
and the lag: how many records the group has yet to read, summed over the topic's partitions.

```bash
kubectl -n shop get aflow carts -o jsonpath='{.status.streamlets}' | jq '.[] | {name, inlets}'
```

```json
{ "name": "cleaner", "inlets": [ { "name": "in", "topic": "shop.cart-events.v1", "group": "carts.cleaner.in", "lag": 0 } ] }
{ "name": "counter", "inlets": [ { "name": "in", "topic": "carts.clean-carts", "group": "carts.counter.in", "lag": 12 } ] }
```

The operator asks each inlet's Kafka cluster every 30 seconds. `lag` is absent when the cluster could
not be asked. A Flink streamlet commits its group on each checkpoint, so its lag trails by up to one
checkpoint interval, and on a topic written in transactions a group that has read everything may show
the few commit markers at the end. A lag that grows and does not fall is a streamlet that cannot keep
up, or a record it fails on every time.

`flow mcp`'s `pipeline_lag` tool reads the same rows from the status.

## Events

The operator records what it did on the `AnkkaFlow`:

```bash
kubectl -n shop get events --field-selector involvedObject.kind=AnkkaFlow
```

`TopicCreated`, `StreamletRolled`, `StreamletRemoved`, `ResetOffsets` and `StateDiscarded` are
Normal. Every Warning names something to act on; each is described in
[Troubleshooting](troubleshooting.md).

A Flink streamlet's `FlinkDeployment` carries the Flink operator's own events, the job's exceptions
among them:

```bash
kubectl -n shop get events --field-selector involvedObject.kind=FlinkDeployment
```

## Logs

A Pekko Streams streamlet's pods have one container, `streamlet`. A Flink streamlet's job manager and
task managers have Flink's `flink-main-container`. Every pod carries the labels
`flow.ankka.thinkmorestupidless.com/pipeline` and `flow.ankka.thinkmorestupidless.com/streamlet`:

```bash
kubectl -n shop logs -l flow.ankka.thinkmorestupidless.com/streamlet=cleaner
kubectl -n shop logs -l flow.ankka.thinkmorestupidless.com/streamlet=counter --all-containers
```

`flow mcp`'s `pipeline_logs` tool reads the last lines of every container of every pod with those
labels.

A Flink streamlet also has Flink's web interface on its job manager, for checkpoints, back pressure
and the job's exceptions:

```bash
kubectl -n shop port-forward svc/flow-carts-counter-rest 8081
```
