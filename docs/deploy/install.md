---
title: Install the platform
description: Install the Flink Kubernetes Operator, the AnkkaFlow custom resource definition, the operator, the Kafka cluster Secrets and the flink-state Secret, locally on kind or on any other cluster.
kind: guide
related: [get-started/deploy-locally.md, reference/operator.md, deploy/configuration.md, reference/resource.md]
---

# Install the platform

ankka-flow on Kubernetes is these pieces:

| Piece | What it is |
|---|---|
| The Flink Kubernetes Operator | Apache Flink's own operator, which runs each Flink streamlet as a `FlinkDeployment`; needed only for pipelines with a Flink streamlet |
| The `AnkkaFlow` custom resource definition | group `flow.ankka.thinkmorestupidless.com`, version `v1alpha1`, kind `AnkkaFlow`, short name `aflow`; one resource per pipeline |
| The operator | one Deployment, `ankka-flow-operator` in namespace `ankka-flow`, watching `AnkkaFlow` resources in every namespace |
| Kafka cluster Secrets | `kafka-cluster-<name>` Secrets in the operator's namespace, saying how to reach each Kafka cluster; `default` is used by any topic that names no cluster and no brokers |
| The Secret `flink-state` | in the operator's namespace: the state store every Flink streamlet checkpoints to |

Kafka and the state store are not part of the platform. A pipeline uses whatever Kafka its cluster
Secrets point at, and whatever store `flink-state` names.

## The Flink Kubernetes Operator first

ankka-flow's operator neither installs nor upgrades the Flink Kubernetes Operator. A pipeline with a
Flink streamlet applied where it is missing is refused with the condition `FlinkOperatorMissing`.
Install it with its Helm chart, version 1.16 or later:

```bash
helm repo add flink-operator https://archive.apache.org/dist/flink/flink-kubernetes-operator-1.16.1/
helm install flink-kubernetes-operator flink-operator/flink-kubernetes-operator \
  --namespace flink-operator --create-namespace \
  --set 'watchNamespaces={shop}'
```

- **With cert-manager** in the cluster, the chart's webhook validates every `FlinkDeployment`; keep
  it.
- **Without cert-manager**, add `--set webhook.create=false`. Nothing ankka-flow renders needs the
  webhook.
- **`watchNamespaces`** lists the namespaces whose pipelines have Flink streamlets. The chart makes
  the job service account, `flink`, in each of them. A namespace added later is a `helm upgrade`
  with the longer list.
- **The service account** a Flink streamlet runs as is the operator's `FLOW_FLINK_SERVICE_ACCOUNT`,
  `flink` unless set: the account the chart made.

## The state store

Every Flink streamlet checkpoints to one store, an S3-compatible bucket or any file system Flink
can write. Name it in a Secret `flink-state` in the operator's namespace:

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

| Key | Meaning |
|---|---|
| `dir` | required; the root of every streamlet's checkpoints, savepoints and high-availability files, under `<dir>/<namespace>/<pipeline>/<streamlet>/<epoch>/` |
| `flink-config` | optional; Flink configuration, one `key=value` per line, copied into every `FlinkDeployment` |
| `env` | optional; environment variables, one `NAME=value` per line, given only to Flink's pods: the store's credentials go here, never in `flink-config` |

A pipeline with a Flink streamlet applied before `flink-state` exists is refused with the condition
`StateStoreMissing`. The operator never deletes anything in the store: files of a state epoch left
behind by a reset stay until the store's own lifecycle rules remove them.

## The manifests

The manifests are Kustomize components in
[`kustomization/`](https://github.com/thinkmorestupidless/ankka-flow/tree/main/kustomization):

| Path | Holds |
|---|---|
| `components/crd` | the custom resource definition |
| `components/operator` | the `ankka-flow` namespace, the operator's ServiceAccount, ClusterRole and ClusterRoleBinding, and its Deployment |
| `components/kafka` | a single-node development Kafka in namespace `kafka`, and a `kafka-cluster-default` Secret pointing at it; never for production |
| `components/minio` | a single MinIO in namespace `minio` and a Job that makes its bucket `flow-state`; a development state store, never for production |
| `overlays/local` | the four components, for a kind cluster |

Render an overlay to see exactly what it applies:

```bash
kubectl kustomize kustomization/overlays/local
```

## On kind

```bash
just up
```

This creates a kind cluster named `ankka` if there is none, switches to its context, and runs
`kustomization/deploy-local.sh`. The script builds the operator and the two sample images, loads them
into the cluster, installs the Flink Kubernetes Operator watching namespace `shop`, applies the
custom resource definition, the `local` overlay and the `flink-state` Secret naming MinIO, and waits
for each. It refuses to touch any kubectl context but `kind-<cluster>`. `ANKKA_FLOW_KIND_CLUSTER`
changes the cluster name, and `just down` deletes the cluster.

Run `just deploy` to rebuild and reinstall on a cluster that already exists.

## On any other cluster

Install the Flink Kubernetes Operator as [its section](#the-flink-kubernetes-operator-first) says, then the custom resource definition, and wait until
it is established:

```bash
kubectl apply --server-side -f kustomization/components/crd/ankkaflow.yaml
kubectl wait --for=condition=Established crd/ankkaflows.flow.ankka.thinkmorestupidless.com --timeout=60s
```

Then install the operator, with its container's `image` set, in an overlay of your own, to a
reference your cluster can pull, such as `ghcr.io/thinkmorestupidless/ankka-flow-operator:<version>`.
A release publishes it to `ghcr.io/thinkmorestupidless` with the release's version as the tag.

Finally, create in namespace `ankka-flow` a `kafka-cluster-default` Secret for your Kafka, one
`kafka-cluster-<name>` Secret for every other cluster a pipeline names, and `flink-state`.
[Configure at deploy time](configuration.md) describes a cluster Secret's keys. Keep real
credentials out of version control.

[Operator](../reference/operator.md) lists the operator's settings: where it looks for its Secrets,
the Flink service account, its resync and observe intervals, its retry backoff and how many
pipelines it reconciles at once.

## Check the installation

```bash
kubectl -n flink-operator rollout status deployment/flink-kubernetes-operator
kubectl -n ankka-flow rollout status deployment/ankka-flow-operator
kubectl -n ankka-flow logs deployment/ankka-flow-operator
```
