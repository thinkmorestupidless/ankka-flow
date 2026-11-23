---
title: Operator
description: The ankka-flow operator's settings, the namespace and permissions it runs with, and what one reconcile of an AnkkaFlow does, in order.
kind: reference
related: [reference/resource.md, deploy/install.md, reference/streamlet-settings.md, deploy/troubleshooting.md]
---

# Operator

The operator is one Deployment, `ankka-flow-operator` in namespace `ankka-flow`, image
`ankka-flow-operator`. It watches `AnkkaFlow` resources in every namespace, creates the managed Kafka
topics, and renders each streamlet by its engine: a Pekko Streams streamlet as a Deployment, a Flink
streamlet as a FlinkDeployment the Flink Kubernetes Operator runs, each with a Secret of its settings.
It adds to a pipeline only what only it can know: the Kafka cluster settings and the state store,
which it reads from Secrets. Installing it is covered in [Install the platform](../deploy/install.md).

## Settings

Each setting is read from a system property, then an environment variable, then its default. An empty
value counts as unset.

| environment variable | system property | default | meaning |
|---|---|---|---|
| `FLOW_KAFKA_CLUSTERS_NAMESPACE` | `flow.operator.kafka-clusters-namespace` | `FLOW_OPERATOR_NAMESPACE`, else `ankka-flow` | where the `kafka-cluster-<name>` Secrets and the `flink-state` Secret are read from |
| `FLOW_FLINK_SERVICE_ACCOUNT` | `flow.operator.flink-service-account` | `flink` | the service account every FlinkDeployment runs as; it needs the rights Kubernetes high availability takes, as the Flink operator's chart grants its `flink` account |
| `FLOW_OPERATOR_OBSERVE_SECONDS` | `flow.operator.observe-seconds` | `30` | how often every pipeline is looked at for what no watch reports: lag, a machine's grants, a Flink job's state |
| `FLOW_OPERATOR_RESYNC_SECONDS` | `flow.operator.resync-seconds` | `300` | how often every pipeline is reconciled even when nothing changed |
| `FLOW_OPERATOR_RETRY_MIN_BACKOFF_SECONDS` | `flow.operator.retry-min-backoff-seconds` | `1` | the first retry delay after a failed reconcile; it doubles per attempt |
| `FLOW_OPERATOR_RETRY_MAX_BACKOFF_SECONDS` | `flow.operator.retry-max-backoff-seconds` | `300` | the retry delay's ceiling |
| `FLOW_OPERATOR_MAX_CONCURRENT_RECONCILES` | `flow.operator.max-concurrent-reconciles` | `4` | pipelines reconciled at once |

`HOSTNAME`, set by Kubernetes to the pod's name, is the `reportingInstance` of every event it records.

## Installed objects

The manifest at `kustomization/components/operator/operator.yaml` installs:

- the Namespace `ankka-flow`;
- the ServiceAccount `ankka-flow-operator`;
- a ClusterRole and ClusterRoleBinding `ankka-flow-operator`;
- the Deployment `ankka-flow-operator`: one replica, `Recreate` strategy, `FLOW_KAFKA_CLUSTERS_NAMESPACE`
  and `FLOW_FLINK_SERVICE_ACCOUNT` set, requesting 100m CPU and 256Mi memory.

The CRD is installed separately, from `kustomization/components/crd/ankkaflow.yaml`.

## Permissions

The ClusterRole grants:

| API group | resources | verbs |
|---|---|---|
| `flow.ankka.thinkmorestupidless.com` | `ankkaflows` | get, list, watch, patch, update |
| `flow.ankka.thinkmorestupidless.com` | `ankkaflows/status` | get, update, patch |
| `apps` | `deployments` | get, list, watch, create, update, patch, delete |
| `flink.apache.org` | `flinkdeployments` | get, list, watch, create, patch, delete |
| `apiextensions.k8s.io` | `customresourcedefinitions` | get |
| core | `secrets`, `serviceaccounts` | get, list, watch, create, update, patch, delete |
| core | `pods` | get, list, watch |
| `rbac.authorization.k8s.io` | `roles`, `rolebindings` | get, list, watch, create, update, patch, delete |
| `events.k8s.io` | `events` | create, patch |
| core | `events` | list |

It holds `create` on events because it grants that permission to each pipeline's service account,
and Kubernetes forbids granting what the grantor does not hold. It lists core events to read the
Flink operator's `JobException` events, which is how a job that cannot restore its state shows. Every object it writes is applied
server-side with the field manager `ankka-flow-operator`, forcing conflicts: the resource is the source of
truth for every field the operator owns.

## What starts a reconcile

The operator watches `AnkkaFlow` resources in every namespace, and Deployments and, once the Flink
operator's CRD exists, FlinkDeployments labelled `app.kubernetes.io/managed-by: ankka-flow`. Any change
to one queues its pipeline. Every pipeline is also queued every `FLOW_OPERATOR_OBSERVE_SECONDS`, to read
its lag, its machines' grants and its Flink jobs, and reconciled again every
`FLOW_OPERATOR_RESYNC_SECONDS`. A reconcile that throws is retried with
backoff. A reset waiting for its targets' pods to go is looked at again every 5 s.

## One reconcile

Rendering is pure: from the resource, the Kafka cluster Secrets, the state store Secret, the observed
Deployments, FlinkDeployments, pods, topics, lag and grants, it produces a list of actions, which two
executors carry out, one for Kubernetes and one for Kafka. In order:

1. **Refusals.** The resource is refused, its phase set to `Failed` with every problem in `detail`, one
   Warning event per problem, and nothing else applied, when:
    - it has a Flink streamlet and the Flink operator's CRD is not installed (`FlinkOperatorMissing`),
      or there is no `flink-state` Secret with a `dir` key (`StateStoreMissing`);
    - a topic names a cluster with no `kafka-cluster-<name>` Secret, or the Secret has no
      `bootstrap.servers` or a `machine` that is not `<organization>/<name>` (reason `Refused`, as are
      all that follow);
    - a topic has no brokers after resolution;
    - a managed topic has no `partitions` or no `replicas` after resolution;
    - a streamlet has no descriptor, or its descriptor does not parse or fails validation, or its
      engine is not its descriptor's or not one this operator runs;
    - a declared inlet is not bound, or a binding names a port the descriptor does not declare;
    - a port names a topic id the resource does not declare.
2. **Topics.** Each managed topic that does not exist is created with its partitions, replication and
   `topicConfig` (`TopicCreated`). An existing managed topic is never altered: other partitions or
   replication is `TopicDiffers`, and a differing `topicConfig` entry is `TopicSettingsIgnored`. An
   unmanaged or ankka topic is only described; one that does not exist is `TopicMissing`. Topics are
   handled before any streamlet.
3. **Per pipeline**, the ServiceAccount, Role and RoleBinding `flow-<pipeline>`, and with a Flink
   streamlet the state store's `env` Secret.
4. **Per streamlet**, the Secret `flow-<pipeline>-<streamlet>` and the Deployment or FlinkDeployment of
   that name, with `StreamletRolled` when its image or configuration hash changed. One the spec no
   longer names is deleted with its Secret (`StreamletRemoved`).
5. **A pending reset request**, when the request annotation's id differs from the done annotation's.
   While any target is running, it records `ResetRefused` and waits: a Pekko Streams streamlet until it
   has `replicas: 0` and no pods, a Flink streamlet until its job is suspended. A request naming an
   unknown streamlet records `ResetRefused` and is marked done. Otherwise it deletes each Flink
   target's FlinkDeployment and moves its state epoch on (`StateDiscarded`), moves each target inlet's
   consumer group to the earliest offsets over that topic's own connection (`ResetOffsets`, or
   `ResetOffsetsFailed` with Kafka's reason, which is never a pipeline failure), then writes the done
   annotation. The next reconcile applies the FlinkDeployment again, at the new epoch.
6. **Status.** `phase`, `detail`, the conditions, and per streamlet, inlet and topic status; written
   only when it changed. A condition other than `Ready` that begins, or whose message changes, is also
   recorded as an event.

The phases, conditions and every event are listed in [the resource reference](resource.md). The
operator logs each event it records, at `warn` for a Warning and `info` otherwise.

## Deletion

Deleting an `AnkkaFlow` deletes everything the operator rendered for it, through owner references. With
`onDelete.managedTopics: Delete`, the operator holds the finalizer
`flow.ankka.thinkmorestupidless.com/managed-topics` until it has deleted the managed topics. Unmanaged
topics are never deleted.
