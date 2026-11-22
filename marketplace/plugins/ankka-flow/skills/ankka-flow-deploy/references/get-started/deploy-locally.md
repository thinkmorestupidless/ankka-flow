# Deploy to a local cluster

> Install the Flink Kubernetes Operator, ankka-flow's operator, a development Kafka and MinIO as the state store on a kind cluster, deploy the sample cart pipeline with flow generate and kubectl, and watch it become Ready.

Source: https://flow.ankka.cloud/get-started/deploy-locally/
This tutorial runs the sample cart pipeline on a local kind cluster. The operator creates the
pipeline's own topics and runs each streamlet by its engine: the Pekko Streams `cleaner` as a
Deployment, and the Flink `counter` as a `FlinkDeployment` that the Flink Kubernetes Operator runs,
checkpointing its state to MinIO.

You need `flow` on your `PATH`, and Docker, a JDK 21, sbt, kind and kubectl; [Install the
tools](install.md) covers all of them. Commands run from the repository's root unless they say
otherwise. [Your first streamlet](first-streamlet.md) describes the pipeline itself.

## Create the cluster and install the platform

```bash
just up
```

`just up` creates a kind cluster named `ankka` (unless one exists), switches kubectl to its context
`kind-ankka`, and runs `kustomization/deploy-local.sh`, which:

1. builds the operator's image and the two streamlets' images, `sample-cart-cleaner` and
   `sample-cart-counter`, and loads them into the kind cluster, so nothing is pulled from a registry;
2. installs the Flink Kubernetes Operator in namespace `flink-operator`, watching namespace `shop`,
   which it creates with the service account `flink` that a Flink streamlet's job runs as;
3. installs the `AnkkaFlow` custom resource definition and waits for it to be established;
4. applies the `local` overlay: the operator in namespace `ankka-flow`; a single-node Kafka in
   namespace `kafka`, with a `kafka-cluster-default` Secret pointing at it; and a single MinIO in
   namespace `minio`, with a Job that makes its bucket `flow-state`;
5. waits for each of them, then applies the Secret `flink-state`, which names MinIO's bucket as the
   store every Flink streamlet checkpoints to.

The script refuses to run against any kubectl context but `kind-ankka`. Set `ANKKA_FLOW_KIND_CLUSTER`
to use another cluster name. [Install the platform](../deploy/install.md) describes each piece, and
how to install them on a cluster that is not kind.

## Name the shop's Kafka and create its topic

The blueprint says the cart events are on a Kafka cluster named `shop`, in a topic the pipeline does
not own: `cart-events` is `managed = false`, so the operator reads it and never creates it. The
operator finds a named cluster through a Secret `kafka-cluster-<name>` in its own namespace. The
sample's Secret for `shop` points at the in-cluster Kafka:

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

Apply it, and create the topic as its owner would:

```bash
kubectl apply -f samples/cart-pipeline/k8s/kafka-cluster-shop.yaml
kubectl -n kafka exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic shop.cart-events.v1 --partitions 3
```

## Generate and apply the resource

```bash
cd samples/cart-pipeline
flow generate blueprint.conf --descriptors flow \
  --image cleaner=sample-cart-cleaner:latest \
  --image counter=sample-cart-counter:latest \
  -n shop | kubectl apply -f -
```

`flow generate` verifies the blueprint against the descriptors in `flow/` and writes one `AnkkaFlow`
resource, named `carts` after the blueprint's `name`, to standard output. Each streamlet in it carries
its engine, its descriptor and its image.

The counter's job manager and task manager each ask for one CPU and 1024 MiB unless deploy-time
configuration says otherwise. On a laptop short of either, give `flow generate` a `--conf` file with
smaller `flink.job-manager` and `flink.task-manager` settings; [Configure at deploy
time](../deploy/configuration.md) lists them.

## Watch it become ready

```bash
kubectl -n shop get aflow carts -w
```

The pipeline is `Pending` while its streamlets start, and `Ready` once the cleaner's pods are ready,
the counter's job is running and every topic check has passed. A Flink job takes a minute or two to
start. The operator records what it did as events on the resource:

```bash
kubectl -n shop get events --field-selector involvedObject.kind=AnkkaFlow
```

Expect a `TopicCreated` event for each of the pipeline's own topics. They take their Kafka names
from the pipeline id and the topic id, `carts.clean-carts` and `carts.cart-counts`. Each streamlet's
workload is named `flow-<pipeline>-<streamlet>`:

```bash
kubectl -n shop get deployment flow-carts-cleaner
kubectl -n shop get flinkdeployment flow-carts-counter
```

The cleaner's pod has one container, the cleaner's own image. The counter's job manager and task
manager run the counter's image, which is Flink's with the streamlet's jar added.

## Send events and read the counts

Write a few cart events to the input topic, two carts a few seconds apart over two and a half
minutes, each carrying its own time in `at`:

```bash
start=$(( $(date +%s) * 1000 ))
for i in 0 1 2 3 4 5; do
  cart="cart-$((i % 2))"
  echo "$cart|{\"cartId\":\"$cart\",\"seq\":$((i + 1)),\"total\":20,\"at\":$((start + i * 30000))}"
done | kubectl -n kafka exec -i kafka-0 -- /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic shop.cart-events.v1 \
  --property parse.key=true --property key.separator='|'
```

Then read what the counter wrote:

```bash
kubectl -n kafka exec kafka-0 -- /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic carts.cart-counts --from-beginning \
  --isolation-level read_committed --property print.key=true --timeout-ms 60000
```

Each line is one cart's count for one window. A window is written once an event ten seconds past
its end has been read, and a reader sees it once the counter's next checkpoint commits it, ten
seconds later unless configured otherwise. The last window stays open until later events arrive.

## Read the lag

The operator writes each inlet's consumer lag on the pipeline's status, for both engines:

```bash
kubectl -n shop get aflow carts -o jsonpath='{.status.streamlets}'
```

Each streamlet lists its inlets with the topic, the consumer group, `carts.<streamlet>.<inlet>`, and
how many records the group has yet to read. [Observe a pipeline](../deploy/observe.md) covers the
status, the conditions, the events and the logs.

## Clean up

```bash
just down
```

This deletes the kind cluster and everything in it, MinIO's bucket included.

## Where to go from here

- [Deploy a pipeline](../deploy/deploy-a-pipeline.md) covers images, versions, updates and deletion.
- [Rebuild from the start](../deploy/reset.md) reads a pipeline's inputs again from the start, and starts
  a Flink streamlet without its state.
- [Configure at deploy time](../deploy/configuration.md) covers replicas, Flink's settings,
  parameters and Kafka cluster Secrets.
