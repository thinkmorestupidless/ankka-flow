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

## A streamlet

A streamlet declares its ports and parameters and implements `process`, which gets one batch of
records from one inlet partition and yields what to emit. The sample router reads cart events keyed
by cart id and sends each to one of two outlets, by a threshold set when the pipeline is deployed:

<!-- include: samples/cart-router/src/cart_router/router.py#router -->
```python
from collections.abc import Iterable

from ankka_flow import Batch, Emit, IntegerParameter, JsonInlet, JsonOutlet, Streamlet, json


class CartRouter(Streamlet):
    name = "cart-router"
    description = "Routes cart events to the valid or review outlet."
    inlet = JsonInlet("in", schema_name="cart-events.v1")
    valid = JsonOutlet("valid", schema_name="cart-events.v1")
    review = JsonOutlet("review", schema_name="cart-events.v1")
    threshold = IntegerParameter(
        "review-threshold",
        default=100,
        description="Carts with a total above this go to the review outlet.",
    )

    def process(self, batch: Batch) -> Iterable[Emit]:
        limit = self.config[self.threshold]
        for record in batch:
            event = json.loads(record.value)  # the SDK decodes nothing; this is the router's choice
            outlet = self.review if event["total"] > limit else self.valid
            yield outlet.emit(record)  # same key, same headers, same bytes
```

The SDK decodes nothing and keeps nothing: a record is bytes in and bytes out, and an emit that
reuses the record keeps its key, so each cart's events stay in order through the next topic. The
sidecar does the rest — subscribes, batches, produces, and commits the batch's offsets only after
every emit is confirmed by the broker, so a failed batch is delivered again and nothing is skipped
unless the streamlet acknowledges without emitting. The streamlet's test runs `process` through a
harness with no Kafka and no sidecar.

The blueprint wires the ports to topics. The input is published by something else — an ankka
service, say — so the pipeline only reads it; the two outputs are the pipeline's own, created by the
operator with the partitions and replication declared:

<!-- include: samples/cart-router/blueprint.conf -->
```hocon
blueprint {
  name = cart
  streamlets {
    router = cart-router
  }
  topics {
    # Published by something else (an ankka service in a cluster, produce.py on a laptop):
    # the platform consumes it and never creates, alters or deletes it.
    cart-events {
      managed           = false
      topic.name        = "shop.cart-events.v1"
      bootstrap.servers = "kafka:9092"
      consumers         = [router.in]
      consumer-config { auto.offset.reset = earliest }
    }
    valid-carts {
      producers  = [router.valid]
      partitions = 3
      replicas   = 1
    }
    review-carts {
      producers  = [router.review]
      partitions = 3
      replicas   = 1
    }
  }
}
```

`flow verify` checks the blueprint against the streamlets' descriptors: every inlet connected, every
topic's contract agreed by the ports on it. [Your first streamlet](https://flow.ankka.cloud/get-started/first-streamlet/)
runs this one on a laptop and [Write a blueprint](https://flow.ankka.cloud/build/blueprints/) builds
the blueprint line by line.

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
