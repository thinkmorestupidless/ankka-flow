# Contract: the Python SDK (`sdks/python`, package `ankka-flow`)

**Feature**: [spec.md](../spec.md) | **Protocol**: [protocol.md](./protocol.md) | **Research**: R15

## Declaring a streamlet

```python
from ankka_flow import Streamlet, JsonInlet, JsonOutlet, IntegerParameter, Batch, Emit, json

class CartRouter(Streamlet):
    name = "cart-router"
    description = "Routes cart events to the valid or review outlet."
    inlet = JsonInlet("in", schema_name="cart-events.v1")
    valid = JsonOutlet("valid", schema_name="cart-events.v1")
    review = JsonOutlet("review", schema_name="cart-events.v1")
    threshold = IntegerParameter("review-threshold", default=100,
                                 description="Carts with a total above this go to the review outlet.")

    def process(self, batch: Batch) -> Iterable[Emit]:
        limit = self.config[self.threshold]
        for record in batch:
            event = json.loads(record.value)          # the SDK decodes nothing; this is the user's choice
            outlet = self.review if event["total"] > limit else self.valid
            yield outlet.emit(record)                 # same key, same headers, same bytes
            # or: yield outlet.emit(value=json.dumps(x), key=record.key, headers=record.headers)
```

Ports and parameters are class attributes discovered by the base class; declaration order does
not matter (the descriptor sorts). `process` is synchronous and is called once per batch on a
worker thread; the sidecar guarantees one batch in flight per partition, so `process` never runs
twice for one partition at once but may run concurrently for different partitions. Returning
normally acknowledges the batch; raising fails it (`Fail` with the exception's message). Yielding
nothing for a record skips it. `Emit` values are sent as they are yielded, before the ack.

Types: `Record(key: bytes | None, headers: list[tuple[str, bytes]], value: bytes, offset: int,
timestamp_ms: int)`; `Batch(inlet: str, partition: int, records: list[Record])` iterable;
`Emit(outlet: str, record: Record)`. Parameters: `StringParameter`, `IntegerParameter`,
`DoubleParameter`, `BooleanParameter`, `DurationParameter` (→ `timedelta`),
`MemorySizeParameter` (→ `int` bytes); `self.config[param]` is typed by the parameter.

## Running

```python
# src/cart_router/main.py
from ankka_flow import serve
from .router import CartRouter

if __name__ == "__main__":
    serve(CartRouter())            # 127.0.0.1:$FLOW_PROCESS_PORT, blocks
```

`serve` binds loopback only (FR-007), serves `Discovery` and `Streamlet` with `grpc.server` and a
thread pool, answers `Discover` with the descriptor, logs `ReportError` problems at `error`, and
runs one `Run` conversation at a time: a second `Run` while one is open ends the first (the
sidecar reconnected). On `Start` it resolves `config_json` into `self.config`; on `Stop` it
finishes the current batches and returns.

## Writing the descriptor

```
uv run descriptor                 # writes flow/descriptor.json from the streamlet in FLOW_STREAMLET (module:attr)
uv run descriptor --check         # exit 1 if the file on disk differs
```

`descriptor` is `ankka_flow._descriptor:main`; `FLOW_STREAMLET` defaults to what `pyproject.toml`
declares under `[tool.ankka-flow] streamlet = "cart_router.router:CartRouter"`. The output follows
`protocol/DESCRIPTOR.md`; `tests/test_descriptor_fixtures.py` declares every fixture streamlet and
asserts byte equality (FR-002).

## The harness (FR-025)

```python
from ankka_flow.testkit import Harness

def test_routes_by_total():
    h = Harness(CartRouter(), config={"review-threshold": 50})
    h.inlet("in").put(key=b"cart-1", value=b'{"total": 10}', headers=[("ce_type", b"ItemAdded")])
    h.inlet("in").put(key=b"cart-2", value=b'{"total": 99}')
    h.run()                                              # one batch per partition, in order
    assert [r.key for r in h.outlet("valid").records] == [b"cart-1"]
    assert h.outlet("review").records[0].headers == []
    assert h.skipped == []
```

No Kafka, no sidecar, no gRPC: the harness calls `process` with batches it builds, applies the
protocol's rules (emits before ack, undeclared outlet is an error, an exception fails the batch)
and records emits per outlet. `h.run(partitions=lambda key: …)` places keys on partitions to
test ordering assumptions.

## Project layout

```
sdks/python/
├── pyproject.toml            # ankka-flow; hatchling; uv; grpcio>=1.84,<2; protobuf>=6,<8; mypy --strict; pytest
├── scripts/proto.py          # copies protocol/ into proto/ (committed) and generates src/ankka_flow/_proto/ (ignored)
├── proto/                    # the verbatim copy CI diffs against protocol/ (FR-027)
├── src/ankka_flow/
│   ├── __init__.py           # Streamlet, JsonInlet, JsonOutlet, *Parameter, Batch, Record, Emit, serve, PROTOCOL_VERSION
│   ├── streamlet.py, ports.py, parameters.py, records.py
│   ├── descriptor.py         # Spec from a Streamlet; canonical JSON writer
│   ├── server.py             # Discovery + Streamlet servicers; the conversation state machine
│   ├── json.py               # loads/dumps helpers over bytes
│   ├── testkit/__init__.py   # Harness
│   ├── _descriptor.py        # the `descriptor` script
│   ├── _conformance.py       # the `conformance` script (contracts/conformance.md)
│   └── py.typed
├── tests/                    # pytest: descriptor fixtures, server against a scripted sidecar double, harness
├── template/                 # the project template (below)
└── README.md
```

`[project.scripts]`: `descriptor = "ankka_flow._descriptor:main"`,
`conformance = "ankka_flow._conformance:main"`. Dev: `uv sync && uv run python scripts/proto.py &&
uv run mypy && uv run pytest -q && uv run conformance`.

## The project template (`sdks/python/template/`) — FR-028

```
{{name}}/
├── pyproject.toml            # depends on ankka-flow; [tool.ankka-flow] streamlet = "…"; scripts descriptor via the SDK
├── Dockerfile                # python:3.12-slim, uv, copies src, CMD python -m {{module}}.main; EXPOSE nothing
├── blueprint.conf            # one streamlet, its topics
├── docker-compose.yml        # kafka (apache/kafka:3.9.1), sidecar (ghcr.io/…/ankka-flow-sidecar:{{version}})
├── flow/streamlet.conf       # the local sidecar configuration (contracts/sidecar.md); descriptor.json lands beside it
├── src/{{module}}/{__init__,main,streamlet}.py
├── tests/test_streamlet.py   # uses the Harness
└── README.md                 # the laptop loop: uv sync, uv run descriptor, docker compose up, uv run python -m …
```

The sample `samples/cart-router/` is this template filled in with the router above, plus a
`produce.py` that writes fifty CloudEvents over ten cart ids with a plain `kafka-python` client and
a `verify.py` that reads both outlet topics and asserts S1's independent test, and `bench.py` for
SC-008.
