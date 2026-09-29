# Operator

> The ankka-flow operator's settings, the namespace and permissions it runs with, and what one reconcile of an AnkkaFlow does, in order.

Source: https://flow.ankka.cloud/reference/operator/
The operator is one Deployment, `ankka-flow-operator` in namespace `ankka-flow`, image
`ankka-flow-operator`. It watches `AnkkaFlow` resources in every namespace, creates the managed Kafka
topics, and renders each streamlet as a Secret and a Deployment. It adds to a pipeline only what only it
can know: the sidecar image, which is its own setting, and the Kafka cluster settings, which it reads
from Secrets. Installing it is covered in [Install the platform](../deploy/install.md).

## Settings

Each setting is read from a system property, then an environment variable, then its default. An empty
value counts as unset.

| environment variable | system property | default | meaning |
|---|---|---|---|
| `FLOW_SIDECAR_IMAGE` | `flow.operator.sidecar-image` | none | the sidecar image added to every streamlet pod. Unset, every pipeline is refused with `SidecarImageMissing` |
| `FLOW_KAFKA_CLUSTERS_NAMESPACE` | `flow.operator.kafka-clusters-namespace` | `FLOW_OPERATOR_NAMESPACE`, else `ankka-flow` | where the `kafka-cluster-<name>` Secrets are read from |
| `FLOW_OPERATOR_RESYNC_SECONDS` | `flow.operator.resync-seconds` | `300` | how often every pipeline is reconciled even when nothing changed |
| `FLOW_OPERATOR_RETRY_MIN_BACKOFF_SECONDS` | `flow.operator.retry-min-backoff-seconds` | `1` | the first retry delay after a failed reconcile; it doubles per attempt |
| `FLOW_OPERATOR_RETRY_MAX_BACKOFF_SECONDS` | `flow.operator.retry-max-backoff-seconds` | `300` | the retry delay's ceiling |
| `FLOW_OPERATOR_MAX_CONCURRENT_RECONCILES` | `flow.operator.max-concurrent-reconciles` | `4` | pipelines reconciled at once |

`HOSTNAME`, set by Kubernetes to the pod's name, is the `reportingInstance` of every event it records.

Upgrading the platform changes `FLOW_SIDECAR_IMAGE`. Every streamlet then rolls onto the new sidecar
image at its next reconcile, because the image is part of each pod template. A pipeline never names the
sidecar image.

## Installed objects

The manifest at `kustomization/components/operator/operator.yaml` installs:

- the Namespace `ankka-flow`;
- the ServiceAccount `ankka-flow-operator`;
- a ClusterRole and ClusterRoleBinding `ankka-flow-operator`;
- the Deployment `ankka-flow-operator`: one replica, `Recreate` strategy, `FLOW_SIDECAR_IMAGE` and
  `FLOW_KAFKA_CLUSTERS_NAMESPACE` set, requesting 100m CPU and 256Mi memory.

The CRD is installed separately, from `kustomization/components/crd/ankkaflow.yaml`.

## Permissions

The ClusterRole grants:

| API group | resources | verbs |
|---|---|---|
| `flow.ankka.thinkmorestupidless.com` | `ankkaflows` | get, list, watch, patch, update |
| `flow.ankka.thinkmorestupidless.com` | `ankkaflows/status` | get, update, patch |
| `apps` | `deployments` | get, list, watch, create, update, patch, delete |
| core | `secrets`, `serviceaccounts` | get, list, watch, create, update, patch, delete |
| core | `pods` | get, list, watch |
| `rbac.authorization.k8s.io` | `roles`, `rolebindings` | get, list, watch, create, update, patch, delete |
| `events.k8s.io` | `events` | create, patch |

It holds `create` on events because it grants that permission to each pipeline's sidecars, and
Kubernetes forbids granting what the grantor does not hold. Every object it writes is applied
server-side with the field manager `ankka-flow-operator`, forcing conflicts: the resource is the source of
truth for every field the operator owns.

## What starts a reconcile

The operator watches `AnkkaFlow` resources in every namespace, and Deployments labelled
`app.kubernetes.io/managed-by: ankka-flow`. Any change to either queues its pipeline, and every pipeline
is reconciled again every `FLOW_OPERATOR_RESYNC_SECONDS`. A reconcile that throws is retried with
backoff. A reset waiting for its targets' pods to go is looked at again every 5 s.

## One reconcile

Rendering is pure: from the resource, the Kafka cluster Secrets, the Secrets built-in streamlets name
(their keys and `resourceVersion`, never their values), the observed Deployments, pods and topics, it produces a list of actions, which two executors carry out, one for Kubernetes and one for
Kafka. In order:

1. **Refusals.** The resource is refused, its phase set to `Failed` with every problem in `detail`, one
   Warning event per problem, and nothing else applied, when:
    - `FLOW_SIDECAR_IMAGE` is unset (reason `SidecarImageMissing`);
    - a topic names a cluster with no `kafka-cluster-<name>` Secret, or the Secret has no
      `bootstrap.servers` (reason `Refused`, as are all that follow);
    - a topic has no brokers after resolution;
    - a managed topic has no `partitions` or no `replicas` after resolution;
    - a streamlet has no descriptor, or its descriptor does not parse or fails validation;
    - a declared inlet is not bound, or a binding names a port the descriptor does not declare;
    - a port names a topic id the resource does not declare;
    - a built-in streamlet has an image, names a built-in this operator does not know, names no Secret
      in its `secret` parameter, or names a Secret that does not exist in the pipeline's namespace,
      cannot be read, or lacks `uri`, `username` or `password`. The messages are listed on
      [Neo4j merge sink](neo4j-merge-sink.md#the-connection-secret).
2. **Topics.** Each managed topic that does not exist is created with its partitions, replication and
   `topicConfig` (`TopicCreated`). An existing managed topic is never altered: other partitions or
   replication is `TopicDiffers`, a differing `topicConfig` entry is `TopicSettingsIgnored`. An
   unmanaged topic is only described; one that does not exist is `TopicMissing`. Topics are handled
   before any Deployment.
3. **Per pipeline**, the ServiceAccount, Role and RoleBinding `flow-<pipeline>`.
4. **Per streamlet**, the Secret and the Deployment `flow-<pipeline>-<streamlet>`, with `StreamletRolled`
   when the pod template's image or configuration hash changed. A built-in streamlet's Deployment has
   only the sidecar container, and its stage's Secret mounted read-only at `/etc/flow/neo4j`; that
   Secret's `resourceVersion` is part of the configuration hash. A labelled Deployment the spec no longer
   names is deleted with its Secret (`StreamletRemoved`).
5. **A pending reset request**, when the request annotation's id differs from the done annotation's.
   While any target has `replicas` other than 0 or pods left, it records `ResetRefused` and waits. A
   request naming an unknown streamlet records `ResetRefused` and is marked done. Otherwise it moves each
   target inlet's consumer group to the earliest offsets over that topic's own connection
   (`ResetOffsets`, or `ResetOffsetsFailed` with Kafka's reason, which is never a pipeline failure),
   then writes the done annotation.
6. **Status.** `phase`, `detail`, and per streamlet and topic status; written only when it changed.

The phases and every event are listed in [the resource reference](resource.md). The operator logs each
event it records, at `warn` for a Warning and `info` otherwise.

## Deletion

Deleting an `AnkkaFlow` deletes everything the operator rendered for it, through owner references. With
`onDelete.managedTopics: Delete`, the operator holds the finalizer
`flow.ankka.thinkmorestupidless.com/managed-topics` until it has deleted the managed topics. Unmanaged
topics are never deleted.
