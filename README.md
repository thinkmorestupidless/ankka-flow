<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/brand/ankka-flow-lockup-white.png">
  <img src="docs/assets/brand/ankka-flow-lockup-black.png" alt="ankka-flow" width="400">
</picture>

Streaming pipelines beside [ankka](https://github.com/thinkmorestupidless/ankka). A pipeline is a
graph of **streamlets**, each with typed inlets and outlets, wired by a **blueprint** over Kafka
topics. A streamlet's logic is written in any language and shipped as an image holding only that
code. A **sidecar** in every pod owns everything Kafka: subscribing, batching, producing,
committing only after the write, consumer groups, lag and resets.

![ankka-flow on Kubernetes: a developer or CI job writes an AnkkaFlow resource with flow generate and applies it. The operator, in namespace ankka-flow, watches AnkkaFlow resources in every namespace, reads the kafka-cluster Secrets, creates the pipeline's managed topics in Kafka, creates and owns a Deployment and a Secret per streamlet, and writes status back. Each streamlet pod has two containers: the process, holding only the streamlet's code, and the sidecar, the operator's image, which owns everything Kafka; they speak gRPC on loopback. The sidecar consumes the unmanaged topic an ankka service publishes, as its own consumer group, and produces to the pipeline's managed topics.](docs/assets/diagrams/platform.svg)

It descends from [Cloudflow](https://github.com/lightbend/cloudflow) by way of the
[thinkmorestupidless fork](https://github.com/thinkmorestupidless/cloudflow), which moved it to
Apache Pekko. Neither is a dependency: six pieces were carried over with their tests (see `NOTICE`).

## A streamlet on a laptop

```bash
sbt sidecar/docker:publishLocal                 # the sidecar image
cd samples/cart-router
uv sync && uv run descriptor                    # the Python router's descriptor
docker compose up -d                            # Kafka and the sidecar; the sidecar waits for the router
uv run python -m cart_router.main &             # the router on 127.0.0.1:9010
uv run python produce.py && uv run python verify.py
```

## On Kubernetes

```bash
just up                                          # kind, the CRD, the operator, a development Kafka
brew install thinkmorestupidless/tap/ankka-flow      # the flow CLI; or the release archive for your platform
flow generate samples/cart-router/blueprint.conf \
  --descriptors samples/cart-router/flow --conf samples/cart-router/k8s/in-cluster.conf \
  --image router=sample-cart-router:latest -n shop | kubectl apply -f -
```

## Read next

The documentation is at **[flow.ankka.cloud](https://flow.ankka.cloud/)**, built from [`docs/`](docs/)
and published as a site, `llms.txt` and agent skills for the ankka marketplace. Start with
[Pipelines and streamlets](https://flow.ankka.cloud/concepts/pipelines/),
[the sidecar](https://flow.ankka.cloud/concepts/sidecar/) and
[your first streamlet](https://flow.ankka.cloud/get-started/first-streamlet/).

The specification, plan and research behind version one are in
[`specs/001-version-one/`](specs/001-version-one/spec.md). `just docs` builds the documentation into
`target/docs-site`; `just docs-serve` serves it with live reload.
