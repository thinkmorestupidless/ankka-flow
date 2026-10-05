# Deploy to a local cluster

> Install the operator and a development Kafka on a kind cluster, deploy the sample cart router as a pipeline with flow generate and kubectl, and watch it become Ready.

Source: https://flow.ankka.cloud/get-started/deploy-locally/
This tutorial runs the sample cart router on a local kind cluster. The operator creates the pipeline's
managed topics and runs the streamlet as a Deployment whose pods hold two containers: the router's image
and the sidecar.

You need the `flow` CLI on your `PATH`, and Docker, sbt, uv, kind and kubectl; [Install the
tools](install.md) covers all of them. Commands run from the repository root unless they say otherwise.

## Create the cluster and install the platform

```bash
just up
```

`just up` creates a kind cluster named `ankka` (unless one exists), switches kubectl to its context
`kind-ankka`, and runs `kustomization/deploy-local.sh`, which:

1. builds the sidecar, operator and sample images;
2. loads all three into the kind cluster, so nothing is pulled from a registry;
3. installs the `AnkkaFlow` custom resource definition and waits for it to be established;
4. applies the `local` overlay: the operator in namespace `ankka-flow`, and a single-node Kafka in
   namespace `kafka` with a `kafka-cluster-default` Secret pointing at it;
5. waits for the operator and Kafka to be running.

The script refuses to run against any kubectl context but `kind-ankka`. Set `ANKKA_FLOW_KIND_CLUSTER`
to use another cluster name. [Install the platform](../deploy/install.md) describes each piece.

## Create the input topic

The router reads `shop.cart-events.v1`, a topic the pipeline does not own: the blueprint declares it
`managed = false`, so the operator only reads it and never creates it. Create it as its owner would:

```bash
kubectl create namespace shop
kubectl -n kafka exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic shop.cart-events.v1 --partitions 3
```

## Generate and apply the resource

```bash
cd samples/cart-router
flow generate blueprint.conf --descriptors flow --conf k8s/in-cluster.conf \
  --image router=sample-cart-router:latest -n shop | kubectl apply -f -
```

`flow generate` verifies the blueprint against the descriptors in `flow/`, merges the deploy-time
configuration in `k8s/in-cluster.conf`, and writes one `AnkkaFlow` resource to standard output. The
configuration file points the unmanaged input at the in-cluster Kafka, because the blueprint's
`kafka:9092` is the address on the laptop's compose network:

```hocon
# Deploy-time configuration for the kind cluster: the input topic lives on the in-cluster Kafka.
flow.topics.cart-events { bootstrap.servers = "kafka.kafka.svc:9092" }
```

## Watch it become ready

```bash
kubectl -n shop get aflow cart -w
```

The pipeline is `Pending` while its streamlet rolls out and `Ready` once every streamlet has all its
pods ready. The operator records what it did as events on the resource:

```bash
kubectl -n shop get events --field-selector involvedObject.kind=AnkkaFlow
```

Expect a `TopicCreated` event for each managed topic, `cart.valid-carts` and `cart.review-carts`. The
managed topics take their Kafka names from the pipeline id and the topic id, `<pipeline>.<topic id>`,
unless the blueprint sets `topic.name`.

## Send events and read the lag

Write a few events to the input topic with Kafka's console producer, then read the sidecar's metrics:

```bash
kubectl -n kafka exec -i kafka-0 -- /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic shop.cart-events.v1 <<< '{"cartId": "cart-1", "total": 10}'
kubectl -n shop port-forward deploy/flow-cart-router 2050 &
curl -s localhost:2050/metrics | grep records_lag
```

The Deployment is named `flow-<pipeline>-<streamlet>`. Lag is labelled with the client id
`<pipeline>.<streamlet>.<inlet>`, here `cart.router.in`, so it is attributable to one streamlet's inlet.
[Observe a pipeline](../deploy/observe.md) lists every metric and event.

## Clean up

```bash
just down
```

This deletes the kind cluster and everything in it.

## Where to go from here

- [Deploy a pipeline](../deploy/deploy-a-pipeline.md) covers images, versions, updates and deletion.
- [Configure at deploy time](../deploy/configuration.md) covers overrides and Kafka cluster Secrets.
