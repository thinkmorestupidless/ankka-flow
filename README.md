# ankka-flow

Streaming pipelines beside [ankka](https://github.com/thinkmorestupidless/ankka). A pipeline is a
graph of **streamlets**, each with typed inlets and outlets, wired by a **blueprint** over Kafka
topics. A streamlet's logic is written in any language and shipped as an image holding only that
code. A **sidecar** in every pod owns everything Kafka: subscribing, batching, producing,
committing only after the write, consumer groups, lag and resets.

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
sbt cli/stage
cli/target/universal/stage/bin/flow generate samples/cart-router/blueprint.conf \
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
