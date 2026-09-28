# ankka-flow for Python

Write [ankka-flow](https://flow.ankka.cloud/) streamlets in Python. You declare ports and parameters and
implement `process`; the sidecar the platform runs beside your container owns everything Kafka.

```python
from collections.abc import Iterable
from ankka_flow import Batch, Emit, IntegerParameter, JsonInlet, JsonOutlet, Streamlet, json, serve

class CartRouter(Streamlet):
    name = "cart-router"
    inlet = JsonInlet("in", schema_name="cart-events.v1")
    valid = JsonOutlet("valid", schema_name="cart-events.v1")
    review = JsonOutlet("review", schema_name="cart-events.v1")
    threshold = IntegerParameter("review-threshold", default=100)

    def process(self, batch: Batch) -> Iterable[Emit]:
        for record in batch:
            event = json.loads(record.value)
            yield (self.review if event["total"] > self.config[self.threshold] else self.valid).emit(record)

if __name__ == "__main__":
    serve(CartRouter())          # 127.0.0.1:$FLOW_PROCESS_PORT (9010), loopback only
```

- `process` runs once per batch on a worker thread. One batch per partition is in flight at a time,
  so it never runs twice for one partition at once; different partitions run concurrently.
- Returning acknowledges the batch. Raising fails it, and the sidecar redelivers it from the last
  commit. Yielding nothing for a record skips it.
- Records are bytes with a key and headers. The SDK decodes nothing; `ankka_flow.json` helps.

## Descriptor

`uv run descriptor` writes `flow/descriptor.json` from the streamlet named by
`[tool.ankka-flow] streamlet = "module:Class"` in your `pyproject.toml` (or `$FLOW_STREAMLET`).
Commit it. `uv run descriptor --check` fails when it is stale. The format is
[`proto/DESCRIPTOR.md`](https://github.com/thinkmorestupidless/ankka-flow/blob/main/sdks/python/proto/DESCRIPTOR.md).

## Testing without Kafka

```python
from ankka_flow.testkit import Harness

h = Harness(CartRouter(), config={"review-threshold": 50})
h.inlet("in").put(key=b"cart-1", value=b'{"total": 10}')
h.run()
assert [r.key for r in h.outlet("valid").records] == [b"cart-1"]
```

## Developing the SDK

```bash
uv sync
uv run python scripts/proto.py   # copy ../../protocol into proto/ (committed) and generate src/ankka_flow/_proto/
uv run mypy                      # strict
uv run pytest -q                 # fixtures, the server against a scripted sidecar, the harness
uv run conformance               # the reference streamlet against the platform's conformance suite
```

`proto/` must equal `../../protocol` byte for byte; CI diffs it. `tests/test_descriptor_fixtures.py`
proves this SDK writes every fixture descriptor exactly.

A new project starts from [`template/`](https://github.com/thinkmorestupidless/ankka-flow/tree/main/sdks/python/template), which carries a compose file with Kafka and the
sidecar for the laptop loop.

## Conformance

`uv run conformance` serves the reference streamlet (`ankka_flow._conformance`) on
`127.0.0.1:$FLOW_PROCESS_PORT` and runs the sidecar's conformance suite against it from the
repository root (sbt and a JDK are needed). Last run: every one of the 18 cases that apply to an SDK
passes; the five `violation.*` and `version.*` cases are skipped, as they only run against the
Scala double.

Two deliberate breaks prove the suite points at what broke:

| `ANKKA_FLOW_BREAK` | what it breaks | cases that fail |
|---|---|---|
| `keyless-empty-key` | a keyless emit carries an empty key | exactly `run.unkeyed-emit` (SC-005) |
| `ack-first` | the ack is sent before the batch's emits | the 12 cases that emit, `run.emits-precede-ack` among them |
