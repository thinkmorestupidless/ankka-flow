# Install the platform

> Install the AnkkaFlow custom resource definition, the operator and at least one Kafka cluster Secret on a Kubernetes cluster, locally on kind or on any other cluster.

Source: https://flow.ankka.cloud/deploy/install/
ankka-flow on Kubernetes is three things:

| Piece | What it is |
|---|---|
| The `AnkkaFlow` custom resource definition | group `flow.ankka.thinkmorestupidless.com`, version `v1alpha1`, kind `AnkkaFlow`, short name `aflow`; one resource per pipeline |
| The operator | one Deployment, `ankka-flow-operator` in namespace `ankka-flow`, watching `AnkkaFlow` resources in every namespace |
| Kafka cluster Secrets | `kafka-cluster-<name>` Secrets in the operator's namespace, saying how to reach each Kafka cluster; `default` is used by any topic that names no cluster and no brokers |

Kafka itself is not part of the platform. A pipeline uses whatever Kafka its cluster Secrets point at.

## The manifests

The manifests are Kustomize components in
[`kustomization/`](https://github.com/thinkmorestupidless/ankka-flow/tree/main/kustomization):

| Path | Holds |
|---|---|
| `components/crd` | the custom resource definition |
| `components/operator` | the `ankka-flow` namespace, the operator's ServiceAccount, ClusterRole and ClusterRoleBinding, and its Deployment |
| `components/kafka` | a single-node development Kafka in namespace `kafka`, and a `kafka-cluster-default` Secret pointing at it; one broker and no persistence beyond the pod, so never for production |
| `overlays/local` | all three components, for a kind cluster |

Render an overlay to see exactly what it applies:

```bash
kubectl kustomize kustomization/overlays/local
```

## On kind

```bash
just up
```

This creates a kind cluster named `ankka` if there is none, switches to its context, and runs
`kustomization/deploy-local.sh`. The script builds the sidecar, operator and sample images, loads them
into the cluster, applies the custom resource definition and waits for it, applies the `local`
overlay, restarts the operator so it runs the image just loaded, and waits for the operator and Kafka.
It refuses to touch any kubectl context but `kind-<cluster>`. `ANKKA_FLOW_KIND_CLUSTER` changes the
cluster name, and `just down` deletes the cluster.

Run `just deploy` to rebuild and reinstall on a cluster that already exists.

## On any other cluster

Install the custom resource definition first, and wait until it is established:

```bash
kubectl apply --server-side -f kustomization/components/crd/ankkaflow.yaml
kubectl wait --for=condition=Established crd/ankkaflows.flow.ankka.thinkmorestupidless.com --timeout=60s
```

Then install the operator with two changes to `components/operator/operator.yaml`, made in an overlay
of your own:

- the operator container's `image`, a reference your cluster can pull, such as
  `ghcr.io/thinkmorestupidless/ankka-flow-operator:<version>`;
- the `FLOW_SIDECAR_IMAGE` environment variable, the sidecar image every pipeline's pods run, such as
  `ghcr.io/thinkmorestupidless/ankka-flow-sidecar:<version>`.

A release publishes both images to `ghcr.io/thinkmorestupidless` with the release's version as the tag.

Finally, create a `kafka-cluster-default` Secret in namespace `ankka-flow` for your Kafka, and one
`kafka-cluster-<name>` Secret for every other cluster a pipeline names. [Configure at deploy
time](configuration.md) describes the Secret's keys. Keep real credentials out of version control.

## The sidecar image is the operator's

No pipeline names the sidecar image. The operator adds the sidecar to every streamlet pod from its own
`FLOW_SIDECAR_IMAGE` setting, so changing that setting upgrades every pipeline's sidecar the next time
each streamlet rolls. An operator with no sidecar image refuses every pipeline: the resource's phase is
`Failed` and each problem is a `SidecarImageMissing` event.

[Operator](../reference/operator.md) lists the operator's other settings: where it looks for Kafka
cluster Secrets, its resync interval, its retry backoff and how many pipelines it reconciles at once.

## Check the installation

```bash
kubectl -n ankka-flow rollout status deployment/ankka-flow-operator
kubectl -n ankka-flow logs deployment/ankka-flow-operator
```

The operator logs the sidecar image it will use when it starts.
