# Contract: the blueprint, the `AnkkaFlow` resource and the operator

**Feature**: [spec.md](../spec.md) | **Data model**: [data-model.md](../data-model.md) | **Research**: R6, R11–R13

## The blueprint (`blueprint.conf`)

HOCON with Cloudflow's keys, parsed by the verification carried from `cloudflow-blueprint`:

```hocon
blueprint {
  name = cart                                   # the pipeline id; --pipeline overrides
  streamlets {
    router = cart-router                        # streamlet name = descriptor name
    sink   = cart-sink
  }
  topics {
    cart-events {                               # a topic the platform does not own (FR-005)
      managed    = false
      topic.name = "shop.cart-events.v1"
      cluster    = shop                         # or bootstrap.servers = "…"
      consumers  = [router.in]
      consumer-config { auto.offset.reset = earliest }
    }
    valid-carts {                               # managed: Kafka name defaults to cart.valid-carts
      producers  = [router.valid]
      consumers  = [sink.in]
      partitions = 6
      replicas   = 1
      topic { retention.ms = 86400000 }
    }
    review-carts { producers = [router.review] }          # an unconnected outlet's topic: allowed
  }
}
```

A topic with no `cluster` and no `bootstrap.servers` uses cluster `default`. Per-streamlet
`replicas` and `config` come from `--conf` (contracts/cli.md) or default to 1 and the declared
defaults.

## The resource

```yaml
apiVersion: flow.ankka.thinkmorestupidless.com/v1alpha1
kind: AnkkaFlow
metadata:
  name: cart
  namespace: shop
  annotations:
    flow.ankka.thinkmorestupidless.com/reset-offsets: '{"id":"7f3c…","streamlets":["router"]}'   # by flow reset
    flow.ankka.thinkmorestupidless.com/reset-offsets-done: '7f3c…'                              # by the operator
spec:
  pipeline: cart
  version: "0.3.1"
  protocolVersion: "1.0"
  onDelete:
    managedTopics: Keep            # Keep | Delete
  streamlets:
    - name: router
      image: ghcr.io/example/cart-router:0.3.1
      replicas: 1
      config: { review-threshold: 100 }
      inlets:  { in: cart-events }
      outlets: { valid: valid-carts, review: review-carts }
      descriptor: { … the streamlet object from descriptor.json, verbatim … }
  topics:
    - id: cart-events
      name: shop.cart-events.v1
      managed: false
      cluster: shop
      consumerConfig: { auto.offset.reset: earliest }
    - id: valid-carts
      name: cart.valid-carts
      managed: true
      cluster: default
      partitions: 6
      replicas: 1
      topicConfig: { retention.ms: "86400000" }
status:
  observedGeneration: 3
  phase: Ready                     # Pending | Ready | Degraded | Failed
  detail: ""
  lastTransitionTime: "2026-09-28T10:00:00Z"
  streamlets:
    - name: router
      desired: 1
      ready: 1
      detail: ""
  topics:
    - id: valid-carts
      exists: true
      detail: ""
```

Printer columns: `PIPELINE`, `PHASE`, `READY` (`1/2` streamlets), `AGE`. Field names are camelCase
(Kubernetes convention) except inside `descriptor`, which is the canonical descriptor object with
its snake_case keys, preserved unknown fields. `bootstrapServers`, `connectionConfig`,
`producerConfig` are optional per topic for the unmanaged-with-brokers case.

## The Kafka cluster secret

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafka-cluster-default          # kafka-cluster-<name>
  namespace: ankka-flow                # FLOW_KAFKA_CLUSTERS_NAMESPACE
  labels: { flow.ankka.thinkmorestupidless.com/kafka-cluster: default }
stringData:
  bootstrap.servers: kafka.kafka.svc:9092
  connection-config: |
    security.protocol=PLAINTEXT
  producer-config: ""
  consumer-config: ""
  partitions: "3"
  replicas: "1"
```

Precedence per topic setting: the resource (deploy-time over blueprint, merged by the CLI), then
the named or default cluster (FR-021, S3.4). A managed topic with no `partitions` after that is a
refusal.

## Operator settings

| setting | env var | default |
|---|---|---|
| sidecar image | `FLOW_SIDECAR_IMAGE` | none: a resource is refused with `SidecarImageMissing` (FR-024, S3.6) |
| Kafka clusters namespace | `FLOW_KAFKA_CLUSTERS_NAMESPACE` | the operator's own namespace |
| resync interval | `FLOW_OPERATOR_RESYNC_SECONDS` | 300 |
| retry backoff | `FLOW_OPERATOR_RETRY_MIN_BACKOFF_SECONDS` / `_MAX_` | 1 / 300 |
| max concurrent reconciles | `FLOW_OPERATOR_MAX_CONCURRENT_RECONCILES` | 4 |

Each also has a system property `flow.operator.<kebab-name>` for tests, as ankka's.

## What the operator renders for one streamlet

- A `Secret` `flow-<pipeline>-<streamlet>` with `descriptor.json` and `streamlet.conf`
  (contracts/sidecar.md), owned by the `AnkkaFlow`.
- A `ServiceAccount` `flow-<pipeline>`, a `Role` allowing `create` on `events.k8s.io/events`, and
  a `RoleBinding`, once per pipeline (for the sidecar's stall warnings).
- A `Deployment` `flow-<pipeline>-<streamlet>`, `replicas` from the spec, `RollingUpdate` with
  `maxSurge 1, maxUnavailable 0`, labels `flow.ankka.thinkmorestupidless.com/pipeline`,
  `/streamlet`, `app.kubernetes.io/managed-by: ankka-flow`, `app.kubernetes.io/name:
  <pipeline>-<streamlet>`; pod template annotation
  `flow.ankka.thinkmorestupidless.com/config-hash: <sha256 of the Secret's data>` so a config
  change rolls and nothing else does (FR-024a); annotations `prometheus.io/scrape`, `prometheus.io/port`.
  Two containers:

  | container | image | env | mounts | ports | probes |
  |---|---|---|---|---|---|
  | `sidecar` | `FLOW_SIDECAR_IMAGE` | `FLOW_PROCESS_ADDRESS=127.0.0.1:9010`, `FLOW_CONFIG_DIR`, `FLOW_STATE_DIR`, `FLOW_METRICS_PORT` | the Secret at `/etc/flow/config`, read-only | `metrics` 2050 | exec readiness and liveness (sidecar.md) |
  | `process` | `spec.streamlets[].image` | `FLOW_PROCESS_PORT=9010` | none | none | none |

  `terminationGracePeriodSeconds: 30`; the sidecar's `preStop` sleeps 2 s so the process is still
  there while the sidecar sends `Stop` and drains.

## Reconcile

Level-triggered, as ankka's: informers on `AnkkaFlow` in every namespace and on Deployments
labelled `managed-by: ankka-flow` enqueue a `PipelineRef`; `Rendering.render(resource, settings,
clusters, observed): Either[Vector[String], Vector[Action]]` is pure; two executors apply. Order
within one reconcile:

1. Refusals: no sidecar image; a topic naming a cluster with no secret; a managed topic with no
   partitions or replicas after resolution; a descriptor that fails validation; a streamlet whose
   `inlets`/`outlets` keys are not the descriptor's ports. → `SetStatus(Failed, detail)`, one
   `Warning` event per refusal, nothing else applied.
2. `EnsureTopic` for every managed topic (FR-019): create when absent (`Normal` `TopicCreated`);
   when present with different partitions or replication, leave it and `Warning` `TopicDiffers`;
   when present with a changed `topicConfig` since the last generation, `Warning`
   `TopicSettingsIgnored`. Unmanaged topics: describe only; absent → the streamlet cannot be
   ready and `Warning` `TopicMissing` (edge case). Topic creation precedes Deployments.
3. `EnsureSecret`, `EnsureServiceAccount/Role/RoleBinding`, `ApplyDeployment` per streamlet;
   `DeleteDeployment` (and its Secret) for a labelled Deployment not in the spec.
4. A pending reset request (annotation id ≠ done id): if any target has `replicas != 0` or pods,
   `Warning` `ResetRefused` and no marker; otherwise `ResetGroup` per target inlet, each `Normal`
   `ResetOffsets` (`<group>: <n> partitions to earliest`) or `Warning` `ResetOffsetsFailed`
   (Kafka's "group has members" is a `Warning`, never a pipeline failure, S4.2), then the done
   marker. Never repeated after a restart (S4.3).
5. `SetStatus`: `streamlets[].ready` from the Deployment's `readyReplicas`; `phase = Ready` when
   every streamlet has `ready == desired` and every topic check passed; `Pending` while rolling;
   `Degraded` when some streamlet is not ready or a topic is missing; `Failed` on refusal.
   Skipped when unchanged except `lastTransitionTime`.

## Events

`events.k8s.io/v1`, `regarding` the `AnkkaFlow`, `reportingController
flow.ankka.thinkmorestupidless.com/operator`, `reportingInstance` the operator's pod name.

| reason | type | when |
|---|---|---|
| `Refused` | Warning | any refusal in step 1, one per problem |
| `SidecarImageMissing` | Warning | `FLOW_SIDECAR_IMAGE` unset |
| `TopicCreated` | Normal | a managed topic created |
| `TopicDiffers` | Warning | an existing managed topic with other partitions or replication |
| `TopicSettingsIgnored` | Warning | a changed setting on an existing topic |
| `TopicMissing` | Warning | an unmanaged topic that does not exist |
| `StreamletRolled` | Normal | a Deployment's pod template changed |
| `StreamletRemoved` | Normal | a Deployment deleted because the spec no longer names it |
| `ResetRefused` | Warning | targets running |
| `ResetOffsets` | Normal | one group reset |
| `ResetOffsetsFailed` | Warning | one group not reset, with Kafka's message |

## RBAC (`kustomization/components/operator/operator.yaml`)

ClusterRole: `ankkaflows` get/list/watch/patch; `ankkaflows/status` get/update/patch;
`deployments`, `secrets`, `serviceaccounts`, `roles`, `rolebindings`, `pods` get/list/watch/
create/update/patch/delete; `events.k8s.io/events` create/patch. The operator runs in namespace
`ankka-flow` as `ankka-flow-operator` with `FLOW_SIDECAR_IMAGE` set by the overlay.

## Tests

`operator/src/test`: `RenderingSuite` (pure: every row of the container table, the config hash,
refusals, removal, the reset guard); `TopicResolutionSuite` (pure, carrying `TopicActionsSpec`'s
cases: managed created, unmanaged skipped, named cluster defaults); `ConsumerGroupResetSuite`
(Kafka, carrying `ConsumerGroupResetSpec`); `ResetRequestSuite` (pure, carrying
`ResetOffsetsSpec`); `CrdSchemaSuite` (every spec/status field is in the YAML); `FlowClusterSuite`
(k3s: Kafka, the cart pipeline with the sample images, topics created, `Ready`, records through,
scale to 3, reset after scale to 0, events observed).
