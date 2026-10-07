# Your first streamlet

> Start a streamlet project with flow init, in Scala or Python — test it, check its descriptor, verify its blueprint and run it beside the sidecar on a laptop — then watch the cart router survive a restart.

Source: https://flow.ankka.cloud/get-started/first-streamlet/
`flow init` writes a streamlet project that builds, tests, checks its descriptor and runs beside the
sidecar, in Scala or in Python. This tutorial starts one, runs it on a laptop, and then uses the cart
router sample to show what the sidecar does when a streamlet fails mid-stream.

You need `flow`, Docker, and the language's build tool: sbt and a JDK 21 for Scala, uv for Python.
[Install the tools](install.md) lists them.

## Start a project

**Scala**

```bash
flow init greeter
cd greeter
```

**Python**

```bash
flow init greeter -l python
cd greeter
```

The project's streamlet reads JSON objects from the topic `greeter.in`, adds a `greeting` field — its
parameter of the same name, `hello, ankka-flow` unless the pipeline sets it — and writes them to
`greeter.out`, keeping each record's key and headers. A value that is not a JSON object fails its
batch.

| File | What it is |
|---|---|
| the streamlet, its entry point and its test | under `src/` (and `tests/` for Python) |
| `flow/descriptor.json` | the streamlet's descriptor, committed |
| `flow/streamlet.conf` | the sidecar's configuration for the laptop |
| `blueprint.conf` | the pipeline: an input topic something else writes, an output topic it owns |
| `docker-compose.yml` | Kafka and the sidecar, for the laptop |
| `k8s/in-cluster.conf` | deploy-time configuration for a kind cluster |
| `build.sbt`, or `pyproject.toml` and a `Dockerfile` | the build and the image |
| `README.md`, `.gitignore`, `.github/workflows/ci.yml` | the commands, and CI for the tests and the descriptor check |
| `.claude/skills/` | the ankka-flow skills, for a coding agent |
| `.mcp.json` | connects Claude Code to `flow mcp`, which serves `flow`'s abilities as tools |
| `flow.toml` | the one cluster those tools may touch: the kind cluster and this project's namespace |

The project depends on the SDK of the same release as the `flow` that wrote it, and its compose file
runs that release's sidecar image.

## Test it, check its descriptor, verify its blueprint

**Scala**

```bash
sbt test
sbt descriptorCheck
flow verify blueprint.conf --descriptors flow
```

**Python**

```bash
uv sync && uv run pytest -q
uv run descriptor --check
flow verify blueprint.conf --descriptors flow
```

The tests use the SDK's harness: no Kafka, no sidecar. The descriptor is what the blueprint is
verified against and what the sidecar compares with the running process; the check fails when the
committed file differs from what the streamlet declares, and `sbt descriptor` or `uv run descriptor`
rewrites it.

## Run it on a laptop

Kafka and the sidecar run in containers and the streamlet runs on the host, where the sidecar dials it
on port 9010. There is no operator on a laptop, so the topics are created by hand:

```bash
docker compose up -d
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic greeter.in --partitions 3
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic greeter.out --partitions 3
```

The first command may print connection warnings while Kafka starts, then create the topic. Start the
streamlet:

**Scala**

```bash
sbt stage && target/universal/stage/bin/greeter &
```

**Python**

```bash
uv run python -m greeter.main &
```

Send it a record and read the output topic:

```bash
echo 'k-1:{"id": 1}' | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic greeter.in --property parse.key=true --property key.separator=:
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic greeter.out --from-beginning --property print.key=true --timeout-ms 10000
```

The consumer prints `k-1` and `{"id": 1}` with its greeting. Stop the streamlet with `kill %1` and the
containers with `docker compose down`. The project's README has the same commands, and how to build
its image and deploy it to a cluster.

## What the sidecar does: the cart router

The cart router is a streamlet with one inlet of cart events, keyed by cart id, and two outlets. It
sends each event to the `review` outlet when the cart's total is above a threshold, and to the `valid`
outlet otherwise. It is written twice, in Scala in `samples/cart-router-scala` and in Python in
`samples/cart-router`; the two declare the same streamlet, so one blueprint and one sidecar
configuration serve both. It comes with scripts that send fifty events and check every promise the
sidecar makes, which the steps below use to kill the router mid-stream. Run them from a clone of the
repository; the Kafka and sidecar files and the scripts are in `samples/cart-router` and serve both
languages.

### The streamlet

The streamlet declares its ports and parameter and implements `process`, which receives one batch of
records and returns the emits to make:

**Scala**

```scala
import com.thinkmorestupidless.ankka.flow.protocol.Json
import com.thinkmorestupidless.ankka.flow.sdk.*

final class CartRouter
    extends Streamlet("cart-router", "Routes cart events to the valid or review outlet."):
  val in     = inlet("in", schemaName = "cart-events.v1")
  val valid  = outlet("valid", schemaName = "cart-events.v1")
  val review = outlet("review", schemaName = "cart-events.v1")
  val threshold = parameter.integer(
    "review-threshold",
    default = 100,
    description = "Carts with a total above this go to the review outlet."
  )

  def process(batch: Batch): Iterable[Emit] =
    val limit = config(threshold)
    batch.records.map { record =>
      // The SDK decodes nothing; this is the router's choice. A value that is not a cart event
      // fails the batch, as the Python router's does.
      val event =
        Json.parse(record.valueString).fold(e => throw new IllegalArgumentException(e), identity)
      val total = event.field("total") match
        case Some(Json.Num(n)) => n
        case _ => throw new IllegalArgumentException(s"no total in ${record.valueString}")
      val outlet = if total > limit then review else valid
      outlet.emit(record) // same key, same headers, same bytes
    }
```

**Python**

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

**Scala**

```scala
import com.thinkmorestupidless.ankka.flow.sdk.Serve

object Main:
  def main(args: Array[String]): Unit = Serve.run(new CartRouter)
```

**Python**

```python
from ankka_flow import serve

from .router import CartRouter

if __name__ == "__main__":
    serve(CartRouter())
```

### Test it without Kafka

Build the sidecar image once, from the repository root:

```bash
sbt sidecar/docker:publishLocal
```

**Scala**

From the repository root:

```bash
sbt cartRouterScala/test
```

**Python**

```bash
cd samples/cart-router
uv sync
uv run pytest -q
```

The tests use the SDK's harness, which calls `process` with batches it builds and applies the
protocol's rules. No Kafka, sidecar or network is involved. [Test a streamlet](../build/testing.md)
describes the harness.

### Check the descriptor

**Scala**

```bash
sbt cartRouterScala/descriptorCheck
```

`sbt cartRouterScala/descriptor` rewrites it.

**Python**

```bash
uv run descriptor --check
```

`uv run descriptor` rewrites it.

The SDK writes `flow/descriptor.json` from the streamlet's declaration. The descriptor is what a
blueprint is checked against, and what the sidecar compares with the running process before it sends a
single record. The check fails when the committed file differs from the declaration. The two
samples' descriptors declare the same streamlet and differ only in the SDK that wrote them, which the
sidecar does not compare.

### Start Kafka and the sidecar

```bash
cd samples/cart-router      # for either language
docker compose up -d
```

The compose file starts a single-node Kafka, reachable from the host on `localhost:9094`, and the
sidecar. The sidecar reads its configuration from `flow/`: the descriptor, and a `streamlet.conf`
naming the pipeline, the streamlet, the topic behind each port and the Kafka address. In a cluster the
operator writes that file; on a laptop it is committed beside the compose file.

The sidecar looks for the streamlet on `host.docker.internal:9010` and keeps asking until it answers,
so the two can start in either order.

### Run the streamlet and send it events

**Scala**

```bash
(cd ../.. && sbt cartRouterScala/stage)
../cart-router-scala/target/universal/stage/bin/cart-router-scala &
uv sync
uv run python produce.py
```

**Python**

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

### Kill it mid-stream

**Scala**

```bash
kill %1; sleep 1; ../cart-router-scala/target/universal/stage/bin/cart-router-scala &
uv run python verify.py
```

**Python**

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

### Clean up

```bash
kill %1
docker compose down
```

## Where to go from here

- [Write a streamlet in Scala](../build/scala-streamlet.md) and
  [Write a streamlet in Python](../build/python-streamlet.md) cover declaring ports and parameters,
  skipping and failing, and the descriptor.
- [Deploy to a local cluster](deploy-locally.md) runs this streamlet on kind, with the operator creating
  its topics.
