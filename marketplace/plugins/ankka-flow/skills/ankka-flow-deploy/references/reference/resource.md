# AnkkaFlow resource

> Every field of the AnkkaFlow custom resource and its status, the Kafka cluster Secret, what the operator renders per streamlet, the events it records and the reset annotations.

Source: https://flow.ankka.cloud/reference/resource/
Group `flow.ankka.thinkmorestupidless.com`, version `v1alpha1`, kind `AnkkaFlow`, plural `ankkaflows`,
short name `aflow`, namespaced. `flow generate` writes it, you apply it, and the
[operator](operator.md) runs it. It says everything that will run except the sidecar image and the Kafka
cluster settings, which only the cluster knows.

```yaml
apiVersion: flow.ankka.thinkmorestupidless.com/v1alpha1
kind: AnkkaFlow
metadata:
  name: cart
  namespace: shop
  labels:
    app.kubernetes.io/managed-by: ankka-flow
    flow.ankka.thinkmorestupidless.com/pipeline: cart
spec:
  pipeline: cart
  version: "0.3.1"
  protocolVersion: "1.0"
  onDelete: { managedTopics: Keep }
  streamlets:
    - name: router
      image: registry.example.com/cart-router:0.3.1
      replicas: 1
      config: { review-threshold: 100 }
      inlets: { in: cart-events }
      outlets: { valid: valid-carts, review: review-carts }
      descriptor: { ... }
  topics:
    - id: cart-events
      name: shop.cart-events.v1
      managed: false
      bootstrapServers: kafka.kafka.svc:9092
      consumerConfig: { auto.offset.reset: earliest }
    - id: valid-carts
      name: cart.valid-carts
      managed: true
      partitions: 3
      replicas: 1
status:
  observedGeneration: 1
  phase: Ready
  detail: ""
  lastTransitionTime: "2026-09-28T10:00:00Z"
  streamlets: [ { name: router, desired: 1, ready: 1 } ]
  topics: [ { id: cart-events, exists: true }, { id: valid-carts, exists: true } ]
```

`kubectl get aflow` prints the columns `PIPELINE`, `PHASE` and `AGE`; `-o wide` adds `DETAIL`.

## `spec`

| field | type | meaning |
|---|---|---|
| `pipeline` | string, required | the pipeline id: 1–40 of `[a-z0-9-]`, not starting or ending with `-`; prefixes every Kafka name and every rendered object |
| `version` | string | the pipeline's version, for people; the operator does not read it |
| `protocolVersion` | string | the protocol version the descriptors were written against, `MAJOR.MINOR` |
| `onDelete.managedTopics` | `Keep` or `Delete` | whether deleting the resource deletes the managed topics it created; default `Keep`. Unmanaged topics are never touched |
| `streamlets` | list, required | one entry per streamlet |
| `topics` | list | one entry per topic |

### `spec.streamlets[]`

| field | type | meaning |
|---|---|---|
| `name` | string, required | the streamlet's name in the blueprint, a DNS label |
| `image` | string | the streamlet's image, holding only its code; required unless `builtin`, and empty when it is |
| `replicas` | integer ≥ 0 | pods to run; default `1`. `0` stops the streamlet |
| `config` | map | every declared parameter, resolved and typed |
| `inlets` | map | inlet name → topic id; every declared inlet must be bound |
| `outlets` | map | outlet name → topic id; an outlet may be left unbound |
| `descriptor` | object, required | the descriptor's `streamlet` object, verbatim, with its snake_case keys; see [the descriptor reference](descriptor.md) |
| `builtin` | boolean | `true` for a streamlet whose descriptor ships with the platform (`builtin/<name>` in the blueprint): its pod has only the sidecar, which runs the stage; default `false` |

### `spec.topics[]`

| field | type | meaning |
|---|---|---|
| `id` | string | the topic's id in the blueprint |
| `name` | string | the Kafka topic's name: `<pipeline>.<id>` for a managed topic unless the blueprint sets `topic.name` |
| `managed` | boolean | `true` (default): the pipeline owns the topic and the operator creates it. `false`: something else owns it and the platform only reads it |
| `cluster` | string | a Kafka cluster, resolved from the Secret `kafka-cluster-<cluster>` |
| `bootstrapServers` | string | brokers named directly; a topic with neither this nor `cluster` uses cluster `default` |
| `partitions`, `replicas` | integer | a managed topic's size; each falls back to its cluster's default |
| `connectionConfig`, `producerConfig`, `consumerConfig` | map of string | Kafka client properties, merged over the cluster's |
| `topicConfig` | map of string | Kafka topic configuration applied when a managed topic is created, such as `retention.ms`. `flow generate` writes `cleanup.policy: compact` here for a managed topic that carries graph deltas and sets no policy of its own |
| `batch.maxRecords`, `batch.maxBytes` | integer | the largest batch the sidecar sends for an inlet reading this topic; default `100` records and 1 MiB |

## `status`

Written by the operator, never by you.

| field | meaning |
|---|---|
| `observedGeneration` | the resource generation the status describes |
| `phase` | `Pending`, `Ready`, `Degraded` or `Failed`; see the phases below |
| `detail` | why the phase is not `Ready`, as `;`-separated reasons; empty when ready |
| `lastTransitionTime` | when the status last changed |
| `streamlets[]` | `name`, `desired` (the spec's `replicas`), `ready` (the Deployment's ready pods), and `detail`: `rolling out`, `<n> of <m> ready`, or empty |
| `topics[]` | `id`, `exists`, and `detail`: `created by the operator`, `topic '<name>' does not exist`, or `Kafka unreachable: <reason>` |

| phase | when |
|---|---|
| `Ready` | every streamlet has rolled out with `ready` equal to `desired`, and every topic exists |
| `Pending` | some streamlet is still rolling out, and every topic exists |
| `Degraded` | an unmanaged topic is missing or Kafka is unreachable, or a streamlet that has rolled out has fewer ready pods than desired |
| `Failed` | the operator refused the resource; `detail` lists every problem and nothing was applied |

## Kafka clusters

A Kafka cluster is a Secret named `kafka-cluster-<name>` in the operator's clusters namespace
(`ankka-flow` unless the operator's `FLOW_KAFKA_CLUSTERS_NAMESPACE` says otherwise). The operator reads
every Secret there whose name starts with `kafka-cluster-`; the label
`flow.ankka.thinkmorestupidless.com/kafka-cluster: <name>` is a convention for finding them.

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

Each topic setting resolves from the resource first, then from its cluster. A topic's client properties
are the cluster's with the topic's own merged over them. The connection reaches the sidecar through the
streamlet's rendered Secret; the streamlet's own container never sees it. See
[Topics and Kafka clusters](../concepts/topics.md).

## What the operator renders

Once per pipeline, owned by the `AnkkaFlow`:

- a ServiceAccount, a Role allowing `create` on `events.k8s.io` events, and a RoleBinding, all named
  `flow-<pipeline>`, which the sidecars use to record `PartitionStalled` warnings.

Once per streamlet, owned by the `AnkkaFlow`, so deleting the resource deletes them:

- a Secret `flow-<pipeline>-<streamlet>` holding `descriptor.json` and `streamlet.conf`, the two files
  the [sidecar](sidecar.md) reads;
- a Deployment `flow-<pipeline>-<streamlet>` with `replicas` from the spec, rolling one pod at a time
  (`maxSurge: 1`, `maxUnavailable: 0`), and two containers.

| container | image | environment | mounts | ports | probes |
|---|---|---|---|---|---|
| `sidecar` | the operator's `FLOW_SIDECAR_IMAGE` | `FLOW_PROCESS_ADDRESS=127.0.0.1:9010`, `FLOW_CONFIG_DIR=/etc/flow/config`, `FLOW_STATE_DIR=/tmp/flow`, `FLOW_METRICS_PORT=2050`, `FLOW_POD_NAME`, `FLOW_POD_NAMESPACE` | the Secret at `/etc/flow/config`, read-only; a projected service-account token | `metrics` 2050 | readiness and liveness from files |
| `process` | `spec.streamlets[].image` | `FLOW_PROCESS_PORT=9010` | none | none | none |

A **built-in** streamlet (`builtin: true`) has only the `sidecar` container, without
`FLOW_PROCESS_ADDRESS`, and its `streamlet.conf` carries a `stage` block. The
[Neo4j merge sink](neo4j-merge-sink.md) also gets the Secret its `secret` parameter names, mounted
read-only at `/etc/flow/neo4j` with mode `0440`; that Secret's `resourceVersion` is part of the config
hash, so changing it rolls the pod.

The pod does not automount a service-account token, so only the sidecar has one. The pod template
carries `prometheus.io/scrape: "true"`, `prometheus.io/port: "2050"` and
`flow.ankka.thinkmorestupidless.com/config-hash`, a SHA-256 of the two files. Changing a streamlet's
image, descriptor or resolved configuration changes that streamlet's pod template and rolls it and
nothing else. `terminationGracePeriodSeconds` is 30, and the sidecar's `preStop` waits 2 s. Every object
carries `app.kubernetes.io/managed-by: ankka-flow` and `flow.ankka.thinkmorestupidless.com/pipeline`;
per-streamlet objects also carry `flow.ankka.thinkmorestupidless.com/streamlet` and
`app.kubernetes.io/name: <pipeline>-<streamlet>`.

A streamlet removed from the spec loses its Deployment and Secret. With `onDelete.managedTopics: Delete`
the operator adds the finalizer `flow.ankka.thinkmorestupidless.com/managed-topics` and deletes the
managed topics when the resource is deleted.

## Events

The operator records `events.k8s.io/v1` Events regarding the `AnkkaFlow`, with reporting controller
`flow.ankka.thinkmorestupidless.com/operator`. A given reason and note are recorded once, however often a
reconcile repeats them.

```bash
kubectl -n shop get events --field-selector involvedObject.kind=AnkkaFlow
```

| reason | type | when |
|---|---|---|
| `SidecarImageMissing` | Warning | the operator has no `FLOW_SIDECAR_IMAGE`; the resource is `Failed` and nothing is applied |
| `Refused` | Warning | one per problem that stops the resource running; it is `Failed` and nothing is applied |
| `TopicCreated` | Normal | a managed topic was created |
| `TopicDiffers` | Warning | an existing managed topic has other partitions or replication; left as it is |
| `TopicSettingsIgnored` | Warning | an existing managed topic's configuration differs from `topicConfig`; not applied |
| `TopicNotCompacted` | Warning | an existing managed topic is not compacted and `topicConfig` asks for `cleanup.policy` with `compact`; left as it is, so it will not hold the whole graph |
| `TopicMissing` | Warning | an unmanaged topic does not exist; its consumers do not become ready |
| `StreamletRolled` | Normal | a streamlet's image, descriptor or configuration changed and it rolls out |
| `StreamletRemoved` | Normal | a streamlet is no longer in the spec and its Deployment and Secret were deleted |
| `ResetRefused` | Warning | a reset request waits for its targets to stop, or names an unknown streamlet |
| `ResetOffsets` | Normal | one consumer group moved to the earliest offsets |
| `ResetOffsetsFailed` | Warning | one consumer group was not reset, with the reason |

The sidecar records `PartitionStalled` (Warning) on its own pod, not on the `AnkkaFlow`.

## Annotations

| annotation | written by | value |
|---|---|---|
| `flow.ankka.thinkmorestupidless.com/reset-offsets` | `flow reset` | `{"id":"<uuid>","streamlets":["router"]}`; an empty list means every streamlet with an inlet |
| `flow.ankka.thinkmorestupidless.com/reset-offsets-done` | the operator | the id of the last request it carried out |

A request whose id equals the done id is never carried out again. See
[Rebuild from the start](../deploy/reset.md).
