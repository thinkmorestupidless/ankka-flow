# The `AnkkaFlow` resource

Group `flow.ankka.thinkmorestupidless.com`, version `v1alpha1`, kind `AnkkaFlow`, short name
`aflow`, namespaced. `flow generate` writes it; you apply it; the operator runs it.

```yaml
apiVersion: flow.ankka.thinkmorestupidless.com/v1alpha1
kind: AnkkaFlow
metadata: { name: cart, namespace: shop }
spec:
  pipeline: cart
  version: "0.3.1"
  protocolVersion: "1.0"
  onDelete: { managedTopics: Keep }        # or Delete
  streamlets:
    - name: router
      image: registry/cart-router:0.3.1
      replicas: 1
      config: { review-threshold: 100 }
      inlets:  { in: cart-events }         # port -> topic id
      outlets: { valid: valid-carts, review: review-carts }
      descriptor: { ... }                  # the descriptor's streamlet object, verbatim
  topics:
    - { id: cart-events, name: shop.cart-events.v1, managed: false, cluster: shop }
    - { id: valid-carts, name: cart.valid-carts, partitions: 6, replicas: 1, topicConfig: { retention.ms: "86400000" } }
status:
  phase: Ready                             # Pending | Ready | Degraded | Failed
  streamlets: [ { name: router, desired: 1, ready: 1 } ]
  topics:     [ { id: cart-events, exists: true } ]
```

## Kafka clusters

A Kafka cluster is a Secret named `kafka-cluster-<name>` in the operator's namespace (`ankka-flow`
by default). A topic with no `cluster` and no `bootstrapServers` uses `default`.

| key | meaning |
|---|---|
| `bootstrap.servers` | required |
| `connection-config`, `producer-config`, `consumer-config` | Java properties text, merged under the topic's own |
| `partitions`, `replicas` | defaults for managed topics that do not set them |

A topic's settings resolve from the resource first, then its cluster (FR-021). The Kafka
connection reaches the sidecar as a mounted Secret; your container never sees it.

## What the operator renders per streamlet

A Secret `flow-<pipeline>-<streamlet>` holding the descriptor and the sidecar's configuration, and a
Deployment of the same name with two containers, rolling one pod at a time and never below the
desired count. A pod-template annotation holds a hash of the configuration, so changing a
streamlet's image, descriptor or configuration rolls that streamlet and nothing else. A streamlet
removed from the spec loses its Deployment and Secret.

## Events

Recorded on the `AnkkaFlow` (`kubectl get events --field-selector involvedObject.kind=AnkkaFlow`):

| reason | type | when |
|---|---|---|
| `Refused`, `SidecarImageMissing` | Warning | the resource cannot run; nothing is applied |
| `TopicCreated` | Normal | a managed topic was created |
| `TopicDiffers` | Warning | an existing managed topic has other partitions or replication; left as it is |
| `TopicSettingsIgnored` | Warning | a changed topic setting was not applied to an existing topic |
| `TopicMissing` | Warning | an unmanaged topic does not exist; its consumers will not be ready |
| `StreamletRolled`, `StreamletRemoved` | Normal | a streamlet rolled out or was removed |
| `ResetRefused` | Warning | a reset was requested while a target still runs |
| `ResetOffsets` | Normal | one consumer group moved to the earliest offsets |
| `ResetOffsetsFailed` | Warning | one group was not reset, with Kafka's reason |

The sidecar records `PartitionStalled` on its own pod.

## Reset annotations

`flow reset` writes `flow.ankka.thinkmorestupidless.com/reset-offsets: {"id": "...", "streamlets": [...]}`.
The operator writes the id it last carried out to `flow.ankka.thinkmorestupidless.com/reset-offsets-done`.
