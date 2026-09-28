# Write a streamlet in Python

> Declare a streamlet's ports and parameters with the Python SDK, process batches into emits, skip or fail records, serve it to the sidecar, and write its descriptor.

Source: https://flow.ankka.cloud/build/python-streamlet/
A Python streamlet is a subclass of `ankka_flow.Streamlet`. It declares its inlets, outlets and
parameters as class attributes and implements one method, `process`, which takes a batch of records
from one inlet partition and yields the records to send to its outlets. `serve()` runs it where the
sidecar in the same pod can reach it, and `uv run descriptor` writes the descriptor that a blueprint
is checked against.

The SDK is the [`ankka-flow`](https://pypi.org/project/ankka-flow/) package on PyPI, built from
[`sdks/python`](https://github.com/thinkmorestupidless/ankka-flow/blob/main/sdks/python). Its version
is the ankka-flow release it belongs to, and the sidecar of the same release speaks its protocol.

## Start a project

The project template in
[`sdks/python/template`](https://github.com/thinkmorestupidless/ankka-flow/blob/main/sdks/python/template)
is a working streamlet that forwards every record. Copy it and replace its three placeholders:
`{{name}}` (the streamlet and pipeline name, such as `cart-router`), `{{module}}` (the Python package,
such as `cart_router`) and `{{version}}` (the ankka-flow version, which picks the sidecar image).

```text
my-streamlet/
├── pyproject.toml            # depends on ankka-flow; [tool.ankka-flow] names the streamlet
├── Dockerfile                # the image: only this code, no ports exposed
├── blueprint.conf            # a pipeline of this one streamlet and its topics
├── docker-compose.yml        # Kafka and the sidecar, for running on a laptop
├── flow/streamlet.conf       # the sidecar's configuration on a laptop
├── src/my_streamlet/
│   ├── streamlet.py          # the streamlet
│   └── main.py               # serve()
└── tests/test_streamlet.py   # tests with the Harness
```

The template's `pyproject.toml` depends on `ankka-flow=={{version}}`, so `uv sync` installs the SDK
from PyPI. To work against a checkout of ankka-flow instead, as the samples in its repository do,
point the dependency at it:

```toml
[tool.uv.sources]
ankka-flow = { path = "../ankka-flow/sdks/python", editable = true }
```

## Declare the streamlet

This is the cart router from
[`samples/cart-router`](https://github.com/thinkmorestupidless/ankka-flow/blob/main/samples/cart-router):
one inlet of cart events, two outlets, and one parameter.

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

- `name` is the name a blueprint refers to: 1 to 63 lower-case letters, digits and hyphens, not
  starting or ending with a hyphen. `description` is optional.
- A port's first argument is its wire name, which a blueprint uses (`router.valid`); the Python
  attribute name does not matter. Port names match `[a-z][a-z0-9-]{0,62}` and are unique across inlets
  and outlets together.
- `schema_name` is the port's contract. Two ports connect only when their schema names are equal. See
  [Contracts](../concepts/contracts.md).
- A parameter's key matches `[a-z][a-z0-9-]*`. A parameter with no `default` must be given a value
  when the pipeline is deployed. `self.config[param]` returns the value typed by the parameter: `int`
  for `IntegerParameter`, `timedelta` for `DurationParameter`, and so on; the full list is in the
  [Python SDK reference](../reference/python-sdk.md#parameters).

Declaration order does not matter; the descriptor sorts ports by name and parameters by key. Declaring
two ports with one name, or two parameters with one key, raises `TypeError` when the class is defined.

## Process a batch

`process` receives a `Batch`: records from one partition of one inlet, in offset order. Each `Record`
has `value` (bytes), `key` (bytes or `None`), `headers` (a list of `(str, bytes)` pairs, in order),
`offset` and `timestamp_ms`. Nothing is decoded; `ankka_flow.json.loads` and `json.dumps` convert
between bytes and Python values when the contract is JSON.

For each record, `process` yields any number of emits:

- `outlet.emit(record)` sends the record to that outlet unchanged: the same key, headers and value.
- `outlet.emit(record, value=..., key=..., headers=...)` sends a copy with the given parts replaced.
  `key=None` sends it without a key.
- `outlet.emit(value=..., key=..., headers=...)` builds a new record.

Keep the key when downstream streamlets rely on per-key order: records with the same key land on the
same partition of the outlet topic, and a keyless record is placed by Kafka's default partitioner.

How `process` ends decides what happens to the batch:

| `process` | the batch |
|---|---|
| returns (or its generator finishes) | acknowledged; the sidecar writes every emit, then commits the offsets |
| yields nothing for a record | that record is skipped, and still committed with the batch |
| raises | failed; its emits are discarded and the batch is delivered again from the last commit |
| yields an emit to an outlet it does not declare | failed, as if it raised |

A record the streamlet cannot use, including one that does not decode, should be skipped rather than
raised on. A raised exception redelivers the same batch indefinitely, which stalls its partition until
the code changes. See [Delivery and failure](../concepts/delivery.md).

Delivery is at least once: after a failure, records whose emits were already written may arrive
again. `process` must tolerate seeing a record twice.

## Concurrency

`process` is synchronous and runs on a worker thread. The sidecar keeps at most one batch in flight per
inlet partition, so `process` never runs twice at once for the same partition, but it may run
concurrently for different partitions. Anything `process` shares between calls must be thread-safe.

Do not keep state per partition or per key in memory across batches. Which partitions a pod holds
changes on every rebalance, and after a failure the same records are delivered again.

## Serve it

```python
from ankka_flow import serve

from .router import CartRouter

if __name__ == "__main__":
    serve(CartRouter())
```

`serve` binds `127.0.0.1` on `FLOW_PROCESS_PORT` (9010 when unset), the only variable the platform
gives the process, and blocks until `SIGTERM` or `SIGINT`. It answers the sidecar's discovery with the
streamlet's descriptor, logs any problems the sidecar reports when it refuses the process, applies the
deployed parameter values before the first batch, and runs batches as they arrive. The process needs
no Kafka address, no credentials and no open ports.

## Write the descriptor

The descriptor is the streamlet's declaration as canonical JSON. `flow verify` and `flow generate`
check a blueprint against it, and the sidecar refuses to start a process whose declaration differs
from the descriptor it was deployed with.

```bash
uv run descriptor            # writes flow/descriptor.json
uv run descriptor --check    # exits 1 when flow/descriptor.json is out of date
```

The command loads the streamlet named `module:Class` in `pyproject.toml`, or in the `FLOW_STREAMLET`
environment variable when that is set:

```toml
[tool.ankka-flow]
streamlet = "cart_router.router:CartRouter"
```

Run it after every change to a port, a contract or a parameter, and commit `flow/descriptor.json`.
Never edit it by hand. The format is on the [Descriptor](../reference/descriptor.md) page.

## Next steps

- [Test a streamlet](testing.md) with the Harness, without Kafka or a sidecar.
- [Build an image](images.md) holding only the streamlet's code.
- [Write a blueprint](blueprints.md) that connects the streamlet to topics.
