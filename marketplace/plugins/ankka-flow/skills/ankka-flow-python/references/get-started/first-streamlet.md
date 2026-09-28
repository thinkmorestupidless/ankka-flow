# Your first streamlet

> Run the sample cart router on a laptop — test it with the harness, check its descriptor, start Kafka and the sidecar in containers, and watch records flow through it and survive a restart.

Source: https://flow.ankka.cloud/get-started/first-streamlet/
The sample in `samples/cart-router` is a Python streamlet with one inlet of cart events, keyed by cart
id, and two outlets. It sends each event to the `review` outlet when the cart's total is above a
threshold, and to the `valid` outlet otherwise. This tutorial runs it on a laptop: Kafka and the sidecar
in Docker, the streamlet itself as an ordinary Python process on the host.

You need Docker, sbt and uv; [Build the tools](install.md) lists them.

## The streamlet

The streamlet declares its ports and parameter as class attributes and implements `process`, which
receives one batch of records and yields emits:

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

The process's entry point serves it on `127.0.0.1:$FLOW_PROCESS_PORT`, where the sidecar finds it:

```python
from ankka_flow import serve

from .router import CartRouter

if __name__ == "__main__":
    serve(CartRouter())
```

## Test it without Kafka

Build the sidecar image once, from the repository root, then work in the sample's directory:

```bash
sbt sidecar/docker:publishLocal
cd samples/cart-router
uv sync
uv run pytest -q
```

The tests use the SDK's harness, which calls `process` with batches it builds and applies the
protocol's rules. No Kafka, sidecar or network is involved. [Test a streamlet](../build/testing.md)
describes the harness.

## Check the descriptor

```bash
uv run descriptor --check
```

The SDK writes `flow/descriptor.json` from the streamlet's declaration. The descriptor is what a
blueprint is checked against, and what the sidecar compares with the running process before it sends a
single record. `--check` exits 1 when the committed file differs from the declaration; `uv run
descriptor` rewrites it.

## Start Kafka and the sidecar

```bash
docker compose up -d
```

The compose file starts a single-node Kafka, reachable from the host on `localhost:9094`, and the
sidecar. The sidecar reads its configuration from `flow/`: the descriptor, and a `streamlet.conf`
naming the pipeline, the streamlet, the topic behind each port and the Kafka address. In a cluster the
operator writes that file; on a laptop it is committed beside the compose file.

The sidecar looks for the streamlet on `host.docker.internal:9010` and keeps asking until it answers,
so the two can start in either order.

## Run the streamlet and send it events

```bash
uv run python -m cart_router.main &
uv run python produce.py
```

`produce.py` creates the three topics when they are missing (the compose Kafka does not create topics
by itself, and there is no operator on a laptop), then writes fifty CloudEvents over ten cart ids to
`shop.cart-events.v1` with a plain Kafka client.

The sidecar subscribes to the input topic, sends batches to the router, writes the router's emits to
`cart.valid-carts` and `cart.review-carts`, and commits the input offsets only once the broker has
confirmed every write.

## Kill it mid-stream

```bash
kill %1; sleep 1; uv run python -m cart_router.main &
uv run python verify.py
```

When the router goes away, the sidecar discards the batches in flight, commits nothing for them, and
waits for the process to come back. When it does, the sidecar describes it again, opens a new
conversation and resumes from the last committed offsets.

`verify.py` reads the input and both outlets from the beginning and exits 0 when every event is on the
outlet its total chose, each cart's events are in the order they were produced, and the CloudEvents
headers arrived intact. It reports repeats separately: delivery is at least once, so an event whose
emits were written but whose offsets were not yet committed when the router died is delivered again. A
repeat never reorders a cart. [Delivery and failure](../concepts/delivery.md) explains why.

## Clean up

```bash
kill %1
docker compose down
```

## Where to go from here

- [Write a streamlet in Python](../build/python-streamlet.md) covers declaring ports and parameters,
  skipping and failing, and the descriptor.
- [Deploy to a local cluster](deploy-locally.md) runs this streamlet on kind, with the operator creating
  its topics.
