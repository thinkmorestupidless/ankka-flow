---
title: AnkkaFlow resource
description: Every field of the AnkkaFlow custom resource and its status, the Kafka cluster Secret, what the operator renders per streamlet, the events it records and the reset annotations.
kind: reference
related: [reference/operator.md, reference/cli.md, concepts/topics.md, deploy/observe.md]
---

# AnkkaFlow resource

Group `flow.ankka.thinkmorestupidless.com`, version `v1alpha1`, kind `AnkkaFlow`, plural `ankkaflows`,
short name `aflow`, namespaced. `flow generate` writes it, you apply it, and the
[operator](operator.md) runs it. It says everything that will run except the Kafka cluster settings
and the state store, which only the cluster knows.

```yaml
apiVersion: flow.ankka.thinkmorestupidless.com/v1alpha1
kind: AnkkaFlow
metadata:
  name: carts
  namespace: shop
  labels:
    app.kubernetes.io/managed-by: ankka-flow
    flow.ankka.thinkmorestupidless.com/pipeline: carts
spec:
  pipeline: carts
  version: "0.6.0"
  descriptorVersion: "2.0"
  onDelete: { managedTopics: Keep }
  streamlets:
    - name: cleaner
      image: registry.example.com/cart-cleaner:0.6.0
      engine: pekko-streams
      replicas: 3
      inlets: { in: cart-events }
      outlets: { out: clean-carts }
      descriptor: { ... }
    - name: counter
      image: registry.example.com/cart-counter:0.6.0
      engine: flink
      replicas: 1
      parallelism: 2
      flink:
        entryClass: cart.counter.CartCounter
        version: "2.2"
        checkpointInterval: 10s
        jobManager: { cpu: "1", memory: 1024m }
        taskManager: { cpu: "1", memory: 1024m }
      config: { window: 1m }
      inlets: { in: clean-carts }
      outlets: { out: cart-counts }
      descriptor: { ... }
  topics:
    - id: cart-events
      name: shop.cart-events.v1
      managed: false
      cluster: shop
      consumerConfig: { auto.offset.reset: earliest }
    - id: clean-carts
      name: carts.clean-carts
      managed: true
      partitions: 3
      replicas: 1
    - id: cart-counts
      name: carts.cart-counts
      managed: true
status:
  observedGeneration: 1
  phase: Ready
  detail: ""
  lastTransitionTime: "2026-11-22T10:00:00Z"
  conditions:
    - { type: Ready, status: "True", reason: Ready, lastTransitionTime: "2026-11-22T10:00:00Z" }
  streamlets:
    - name: cleaner
      engine: pekko-streams
      desired: 3
      ready: 3
      detail: "3 of 3 ready"
      inlets: [ { name: in, topic: shop.cart-events.v1, group: carts.cleaner.in, lag: 0 } ]
    - name: counter
      engine: flink
      desired: 1
      ready: 1
      detail: running
      inlets: [ { name: in, topic: carts.clean-carts, group: carts.counter.in, lag: 12 } ]
  topics:
    - { id: cart-events, exists: true }
    - { id: clean-carts, exists: true, detail: created by the operator }
    - { id: cart-counts, exists: true, detail: created by the operator }
```

`kubectl get aflow` prints the columns `PIPELINE`, `PHASE` and `AGE`; `-o wide` adds `DETAIL`.

## `spec`

| field | type | meaning |
|---|---|---|
| `pipeline` | string, required | the pipeline id: 1–40 of `[a-z0-9-]`, not starting or ending with `-`; prefixes every Kafka name and every rendered object |
| `version` | string | the pipeline's version, for people; the operator does not read it |
| `descriptorVersion` | string | the descriptor version the streamlets' descriptors were written in, `MAJOR.MINOR`; the operator reads major `2` |
| `onDelete.managedTopics` | `Keep` or `Delete` | whether deleting the resource deletes the managed topics it created; default `Keep`. Unmanaged topics are never touched |
| `streamlets` | list, required | one entry per streamlet |
| `topics` | list | one entry per topic |

### `spec.streamlets[]`

| field | type | meaning |
|---|---|---|
| `name` | string, required | the streamlet's name in the blueprint, a DNS label |
| `image` | string, required | the streamlet's image |
| `engine` | string, required | `pekko-streams` or `flink`, as its descriptor says |
| `replicas` | integer ≥ 0 | a Pekko Streams streamlet's pods; default `1`, `0` stops it. A Flink streamlet takes `1`, running, or `0`, suspended with its state kept |
| `parallelism` | integer ≥ 1 | a Flink streamlet's parallel tasks; default `1`. Not taken by a Pekko Streams streamlet |
| `flink` | object | a Flink streamlet's job, required for one and refused for any other: `entryClass` and `version` from its descriptor; `checkpointInterval`, default `10s`; `jobManager` and `taskManager`, each `cpu` (default `"1"`) and `memory` (default `1024m`) |
| `config` | map | every declared parameter, resolved and typed |
| `inlets` | map | inlet name → topic id; every declared inlet must be bound |
| `outlets` | map | outlet name → topic id; an outlet may be left unbound |
| `descriptor` | object, required | the descriptor's `streamlet` object, verbatim, with its snake_case keys; see [the descriptor reference](descriptor.md) |

### `spec.topics[]`

| field | type | meaning |
|---|---|---|
| `id` | string | the topic's id in the blueprint |
| `name` | string | the Kafka topic's name: `<pipeline>.<id>` for a managed topic unless the blueprint sets `topic.name` |
| `managed` | boolean | `true` (default): the pipeline owns the topic and the operator creates it. `false`: something else owns it |
| `ankka` | object | `project` and `name` of an ankka project's topic; such a topic is never managed, and its cluster defaults to `ankka` |
| `cluster` | string | a Kafka cluster, resolved from the Secret `kafka-cluster-<cluster>` |
| `bootstrapServers` | string | brokers named directly; a topic with neither this nor `cluster` uses cluster `default` |
| `partitions`, `replicas` | integer | a managed topic's size; each falls back to its cluster's default |
| `connectionConfig`, `producerConfig`, `consumerConfig` | map of string | Kafka client properties, merged over the cluster's |
| `topicConfig` | map of string | Kafka topic configuration applied when a managed topic is created, such as `retention.ms` |

## `status`

Written by the operator, never by you.

| field | meaning |
|---|---|
| `observedGeneration` | the resource generation the status describes |
| `phase` | `Pending`, `Ready`, `Degraded` or `Failed`; see the phases below |
| `detail` | why the phase is not `Ready`, as `;`-separated reasons; empty when ready |
| `lastTransitionTime` | when the status last changed |
| `conditions[]` | `type`, `status`, `reason`, `message`, `lastTransitionTime`; see the conditions below |
| `streamlets[]` | `name`, `engine`, `desired` (the spec's `replicas`), `ready`, `detail`, and `inlets[]` |
| `streamlets[].detail` | a Pekko Streams streamlet: `rolling out` or `<n> of <m> ready`. A Flink streamlet: `rolling out`, `running`, `suspended`, or why its job failed |
| `streamlets[].inlets[]` | `name`, `topic` (the Kafka name), `group` (the consumer group), and `lag`: the records on the topic the group has not committed, summed over partitions; absent when it could not be read |
| `topics[]` | `id`, `exists`, and `detail`: `created by the operator`, `topic '<name>' does not exist`, or `Kafka unreachable: <reason>` |

| phase | when |
|---|---|
| `Ready` | every streamlet has rolled out with `ready` equal to `desired`, and every topic exists |
| `Pending` | some streamlet is still rolling out, and every topic exists |
| `Degraded` | an unmanaged topic is missing or Kafka is unreachable, a streamlet that has rolled out has fewer ready pods than desired or a failed job, or a machine lacks a grant or its credentials are refused |
| `Failed` | the operator refused the resource, or the cluster lacks what a Flink streamlet needs; `detail` lists every problem and nothing was applied |

| condition | phase | when | cleared |
|---|---|---|---|
| `Ready` | `Ready` when `True` | always present; `False` with the phase as its reason in every other phase | |
| `FlinkOperatorMissing` | `Failed` | a Flink streamlet, and no `flinkdeployments.flink.apache.org` CRD | the next reconcile that finds it |
| `StateStoreMissing` | `Failed` | a Flink streamlet, and no Secret `flink-state` with a `dir` key | the next reconcile that finds it |
| `TopicNotGranted` | `Degraded` | the machine of an ankka topic's cluster may not read (reason `Read`) or write (`Write`) it | the first observe pass the broker allows it |
| `MachineCredentialRefused` | `Degraded` | the broker refuses the machine's credentials | the first observe pass that authenticates |
| `StateUnreadable` | `Degraded` | a Flink streamlet's new image cannot restore its state | the streamlet runs again: the previous image, or after a reset |

Every condition but `Ready` is removed once it no longer holds.

## Kafka clusters

A Kafka cluster is a Secret named `kafka-cluster-<name>` in the operator's clusters namespace
(`ankka-flow` unless the operator's `FLOW_KAFKA_CLUSTERS_NAMESPACE` says otherwise). The operator reads
every Secret there whose name starts with `kafka-cluster-`; the label
`flow.ankka.thinkmorestupidless.com/kafka-cluster: <name>` is a convention for finding them.

<!-- include: samples/cart-pipeline/k8s/kafka-cluster-shop.yaml -->
```yaml
# A named Kafka cluster the operator resolves topics against: a topic with `cluster = shop` uses
# these brokers and settings. It lives in the operator's namespace. Never commit real credentials.
apiVersion: v1
kind: Secret
metadata:
  name: kafka-cluster-shop
  namespace: ankka-flow
  labels:
    flow.ankka.thinkmorestupidless.com/kafka-cluster: shop
stringData:
  bootstrap.servers: kafka.kafka.svc:9092
  connection-config: |
    security.protocol=PLAINTEXT
```

| key | meaning |
|---|---|
| `bootstrap.servers` | required |
| `connection-config` | Java properties text for every client: `security.protocol`, `sasl.*`, `ssl.*` |
| `producer-config`, `consumer-config` | Java properties text for producers or consumers |
| `partitions`, `replicas` | defaults for managed topics that do not set their own |
| `machine` | `<organization>/<name>`: the cluster is reached as that ankka machine, so its consumer groups are prefixed `ankka.machine.<organization>.<name>.` and its grants are checked |

Each topic setting resolves from the resource first, then from its cluster. A topic's client properties
are the cluster's with the topic's own merged over them, and reach a streamlet through its rendered
Secret, which holds the settings of that streamlet's own topics only. See
[Topics and Kafka clusters](../concepts/topics.md).

## The state store

A pipeline with a Flink streamlet needs the Secret `flink-state` in the operator's clusters
namespace:

<!-- include: samples/cart-pipeline/k8s/flink-state.yaml#flink-state -->
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: flink-state
  namespace: ankka-flow
stringData:
  dir: s3://flow-state/local
  flink-config: |
    s3.endpoint=http://minio.minio.svc:9000
    s3.path-style-access=true
  env: |
    AWS_ACCESS_KEY_ID=flow
    AWS_SECRET_ACCESS_KEY=flow-state-password
    ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-presto-2.2.1.jar
```

| key | meaning |
|---|---|
| `dir` | required: a URI Flink can write, such as `s3://flink-state` |
| `flink-config` | Java properties text merged into every FlinkDeployment's configuration, such as the store's endpoint |
| `env` | Java properties text given to Flink's pods as environment, through the Secret `flow-<pipeline>-flink-state` in the pipeline's namespace |

A streamlet's state is under `<dir>/<namespace>/<pipeline>/<streamlet>/<epoch>/`, in
`checkpoints`, `savepoints` and `ha`. The epoch starts at 1 and moves on with each
[reset](../deploy/reset.md).

## What the operator renders

Once per pipeline, owned by the `AnkkaFlow`:

- a ServiceAccount, a Role allowing `create` on `events.k8s.io` events, and a RoleBinding, all named
  `flow-<pipeline>`, the service account of every Pekko Streams pod;
- with a Flink streamlet and a state store whose `env` is set, the Secret `flow-<pipeline>-flink-state`.

Once per streamlet, owned by the `AnkkaFlow`, so deleting the resource deletes them, a Secret
`flow-<pipeline>-<streamlet>` holding `streamlet.properties`, the
[streamlet settings](streamlet-settings.md), and:

- for a **Pekko Streams** streamlet, a Deployment `flow-<pipeline>-<streamlet>` with `replicas` from
  the spec, rolling one pod at a time (`maxSurge: 1`, `maxUnavailable: 0`). Its one container,
  `streamlet`, runs the image with `FLOW_CONFIG_DIR=/etc/flow/config`, where the Secret is mounted
  read-only. Its readiness probe is the file `/tmp/flow/ready`, and its liveness probe the age of
  `/tmp/flow/alive`, which the SDK touches while the streamlet runs. The pod mounts no service-account
  token; `terminationGracePeriodSeconds` is 30, and `preStop` waits 2 s;
- for a **Flink** streamlet, a FlinkDeployment `flow-<pipeline>-<streamlet>`, which the Flink
  Kubernetes Operator runs as a job manager and task managers. Its job is the image's
  `local:///opt/flink/usrlib/streamlet.jar` at the descriptor's entry class, with the spec's
  `parallelism`, upgrade mode `last-state`, and state `running` at `replicas: 1` and `suspended` at
  `0`. Its configuration is the state store's `flink-config` with these over it: exactly-once
  checkpoints every `checkpointInterval` into the streamlet's state directories, Kubernetes high
  availability, and an exponential-delay restart strategy. Its pods run as the operator's
  `FLOW_FLINK_SERVICE_ACCOUNT`, with the Secret mounted read-only at `/etc/flow/config`.

The pod template carries `flow.ankka.thinkmorestupidless.com/config-hash`, a SHA-256 of the
streamlet's settings. Changing a streamlet's image, descriptor or resolved configuration changes that
streamlet's pods and rolls it and nothing else; a Flink streamlet restores its state from the last
checkpoint when it does. Every object carries `app.kubernetes.io/managed-by: ankka-flow` and
`flow.ankka.thinkmorestupidless.com/pipeline`; per-streamlet objects also carry
`flow.ankka.thinkmorestupidless.com/streamlet` and `app.kubernetes.io/name: <pipeline>-<streamlet>`.

A streamlet removed from the spec loses its Deployment or FlinkDeployment and its Secret. With
`onDelete.managedTopics: Delete` the operator adds the finalizer
`flow.ankka.thinkmorestupidless.com/managed-topics` and deletes the managed topics when the resource is
deleted.

## Events

The operator records `events.k8s.io/v1` Events regarding the `AnkkaFlow`, with reporting controller
`flow.ankka.thinkmorestupidless.com/operator`. A given reason and note are recorded once, however often a
reconcile repeats them.

```bash
kubectl -n shop get events --field-selector involvedObject.kind=AnkkaFlow
```

| reason | type | when |
|---|---|---|
| `Refused` | Warning | one per problem that stops the resource running; it is `Failed` and nothing is applied |
| `FlinkOperatorMissing`, `StateStoreMissing` | Warning | the condition of that name began; the resource is `Failed` |
| `TopicNotGranted`, `MachineCredentialRefused` | Warning | the condition of that name began or its message changed |
| `StateUnreadable` | Warning | a Flink streamlet's new image cannot restore its state |
| `TopicCreated` | Normal | a managed topic was created |
| `TopicDiffers` | Warning | an existing managed topic has other partitions or replication; left as it is |
| `TopicSettingsIgnored` | Warning | an existing managed topic's configuration differs from `topicConfig`; not applied |
| `TopicMissing` | Warning | an unmanaged topic does not exist; its consumers do not become ready |
| `StreamletRolled` | Normal | a streamlet's image, descriptor or configuration changed and it rolls out |
| `StreamletRemoved` | Normal | a streamlet is no longer in the spec and its objects were deleted |
| `ResetRefused` | Warning | a reset request waits for its targets to stop, or names an unknown streamlet |
| `ResetOffsets` | Normal | one consumer group moved to the earliest offsets |
| `ResetOffsetsFailed` | Warning | one consumer group was not reset, with the reason |
| `StateDiscarded` | Normal | a reset Flink streamlet starts from an empty state at its next epoch |

## Annotations

| annotation | written by | value |
|---|---|---|
| `flow.ankka.thinkmorestupidless.com/reset-offsets` | `flow reset` | `{"id":"<uuid>","streamlets":["cleaner"]}`; an empty list means every streamlet with an inlet |
| `flow.ankka.thinkmorestupidless.com/reset-offsets-done` | the operator | the id of the last request it carried out |
| `flow.ankka.thinkmorestupidless.com/state-epochs` | the operator | each Flink streamlet's state epoch, where it is not 1 |

A request whose id equals the done id is never carried out again. See
[Rebuild from the start](../deploy/reset.md).
