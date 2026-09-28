# Python SDK

> Every public name of the ankka-flow Python package — Streamlet, ports, parameters and their Python types, records, serve, the testkit Harness — and the descriptor and conformance commands.

Source: https://flow.ankka.cloud/reference/python-sdk/
The package is `ankka-flow`, imported as `ankka_flow`, in
[`sdks/python`](https://github.com/thinkmorestupidless/ankka-flow/blob/main/sdks/python). It needs
Python 3.12 or later and depends on `grpcio` and `protobuf`. It is published to PyPI as
[`ankka-flow`](https://pypi.org/project/ankka-flow/) (`uv add ankka-flow`), versioned as the
ankka-flow release it belongs to. It is typed (`py.typed`) and checked with
`mypy --strict`.

## Names

Everything below is importable from `ankka_flow` unless another module is named.

| name | what it is |
|---|---|
| `Streamlet` | the base class of every streamlet |
| `JsonInlet`, `JsonOutlet` | ports with a JSON contract |
| `StringParameter`, `IntegerParameter`, `DoubleParameter`, `BooleanParameter`, `DurationParameter`, `MemorySizeParameter` | typed configuration parameters |
| `Parameter` | the base class of the parameters |
| `Config` | a streamlet's resolved parameter values |
| `Record`, `Batch`, `Emit` | a record, a batch of records, a record for an outlet |
| `serve` | runs a streamlet for the sidecar |
| `json` | `loads(bytes)` and `dumps(obj) -> bytes` |
| `PROTOCOL_VERSION` | the protocol version the SDK speaks, `"1.0"` |
| `ankka_flow.testkit.Harness` | runs a streamlet over in-memory inlets and outlets |
| `ankka_flow.testkit.hash_partitioner` | a stable key-to-partition function for the Harness |

## `Streamlet`

```python
class Streamlet(ABC):
    name: ClassVar[str]
    description: ClassVar[str] = ""
    config: Config

    def process(self, batch: Batch) -> Iterable[Emit]: ...
```

- `name` is required: 1 to 63 of `[a-z0-9-]`, not starting or ending with `-`.
- Ports and parameters are class attributes, found when the subclass is defined, including those
  inherited from a base class. A port name used twice, or a parameter key used twice, raises
  `TypeError` at class definition.
- `config` holds the parameter values: the declared defaults until the sidecar's `Start` applies the
  deployed values.
- `process` is abstract. It is called once per batch on a worker thread; never twice at once for one
  partition, possibly concurrently for different partitions. Returning acknowledges the batch; raising
  fails it; yielding nothing for a record skips it. Yielding something other than an `Emit`, or an
  emit to an undeclared outlet, fails the batch.
- `inlets()`, `outlets()` and `parameters()` are class methods returning the declared ports and
  parameters. `configure(values)` resolves a mapping of parameter values into `config`.

## Ports

```python
JsonInlet(name: str, *, schema_name: str)
JsonOutlet(name: str, *, schema_name: str)
```

`name` is the wire name, matching `[a-z][a-z0-9-]{0,62}`. `schema_name` is required and must not be
empty. Every port has `format` (`"json"`), `schema_name` and `fingerprint`, the Base64 of the SHA-256
of the schema name.

```python
JsonOutlet.emit(record: Record | None = None, *, value: bytes | None = None,
                key: bytes | None = <unset>, headers: list[tuple[str, bytes]] | None = None) -> Emit
```

With a record, `emit` forwards it: the same key, headers and value, and the same offset and timestamp,
with any keyword argument replacing its part. `key=None` removes the key. Without a record, `value` is
required and the other parts default to no key and no headers. Calling neither raises `ValueError`.

## Parameters

```python
StringParameter(key: str, *, default: str | None = None, description: str = "")
```

Every parameter class takes the same arguments. `key` matches `[a-z][a-z0-9-]*`. A parameter with no
`default` is required when the pipeline is deployed; reading it from `config` when it has no value
raises `KeyError`. A default that is not a value of the parameter's type raises `ValueError` at
declaration.

| class | descriptor type | `config[param]` returns | accepts |
|---|---|---|---|
| `StringParameter` | `STRING` | `str` | any text |
| `IntegerParameter` | `INTEGER` | `int` | an integer |
| `DoubleParameter` | `DOUBLE` | `float` | a number |
| `BooleanParameter` | `BOOLEAN` | `bool` | `true` or `false` |
| `DurationParameter` | `DURATION` | `datetime.timedelta` | a HOCON duration: `100 ms`, `5m`, `1.5 s`; a bare number is milliseconds |
| `MemorySizeParameter` | `MEMORY_SIZE` | `int`, in bytes | a HOCON size: `1 MiB`, `512k`, `10MB`; a bare number is bytes |

A default may be given as the Python type (`default=timedelta(seconds=5)`, `default=True`) or as text
(`default="5s"`).

`Config[param]` returns the typed value; `Config.get(key)` returns a value by key, or `None`;
`Config.as_dict()` returns every resolved value.

## Records

```python
@dataclass(frozen=True)
class Record:
    value: bytes
    key: bytes | None = None
    headers: list[tuple[str, bytes]] = []
    offset: int = -1
    timestamp_ms: int = 0

@dataclass(frozen=True)
class Batch:          # iterable over its records; len() is their count
    inlet: str
    partition: int
    records: list[Record]

@dataclass(frozen=True)
class Emit:
    outlet: str
    record: Record
```

A record is exactly what Kafka holds; nothing is decoded. Headers keep their order. A batch's records
come from one partition of one inlet, in offset order. The SDK sends each emit to the sidecar as it is
yielded; the sidecar holds a batch's emits until the batch is acknowledged and writes none of them if
it fails.

## `serve`

```python
serve(streamlet: Streamlet, port: int | None = None, block: bool = True, workers: int = 32) -> Server
```

Serves the protocol's `Discovery` and `Streamlet` services on `127.0.0.1:port`, where `port` defaults
to `FLOW_PROCESS_PORT`, or 9010 when that is unset. It binds loopback only and raises `OSError` when the
port cannot be bound. `workers` is the number of threads that run `process`.

- `Discover` answers with the streamlet's descriptor. `ReportError` logs each problem the sidecar
  reports at `ERROR` level.
- One conversation runs at a time. A new `Run` ends the previous one, because the sidecar has
  reconnected. `Start` applies the deployed parameter values; `Stop` lets in-flight batches finish and
  ends the conversation.
- Messages are limited to 16 MiB in each direction.
- With `block=True`, `SIGTERM` and `SIGINT` stop the server and `serve` returns when it has stopped.
  With `block=False` it returns at once; the returned `Server` has `port`, `stop(grace=2.0)` and
  `wait()`.

When the root logger has no handlers, `serve` configures logging at `INFO`. The SDK logs under the
logger name `ankka_flow`.

## `ankka_flow.testkit`

```python
Harness(streamlet: Streamlet, config: Mapping[str, object] | None = None)
```

| member | what it does |
|---|---|
| `inlet(name).put(*, value, key=None, headers=None)` | queues a record on a declared inlet |
| `run(partitions=None, max_records=None)` | processes everything queued: per inlet, per partition, batches in offset order |
| `outlet(name).records` | the records emitted to a declared outlet, in order |
| `skipped` | records of successful batches that no emit was derived from |
| `failures` | a `Failure(batch, error)` per failed batch; its emits are discarded |
| `batches` | every batch given to `process`, in order |

`partitions` maps a key to a partition number (default: everything on partition 0); `max_records`
caps a batch (default: one batch per partition). `hash_partitioner(n)` places keys on `n` partitions by
CRC-32 of the key, with a keyless record on partition 0. An undeclared inlet, outlet or configuration
key raises `KeyError`. See [Test a streamlet](../build/testing.md).

## Commands

The package installs two console scripts.

### `descriptor`

```bash
uv run descriptor [--out flow/descriptor.json] [--check]
```

Loads the streamlet named `module:Class` by the `FLOW_STREAMLET` environment variable or, when that is
unset, by `pyproject.toml`, validates its descriptor and writes it as canonical JSON. `src/` is put on
the import path when it exists. `--check` writes nothing and exits 1 when the file is missing or
differs. An invalid declaration prints each problem and exits 1.

```toml
[tool.ankka-flow]
streamlet = "cart_router.router:CartRouter"
```

The descriptor's `sdk` block is `{"name": "ankka-flow-python", "version": <the SDK's version>}`. See
[Descriptor](descriptor.md).

### `conformance`

```bash
uv run conformance
```

Serves the SDK's conformance reference streamlet on `FLOW_PROCESS_PORT` (default 9010) and runs the
platform's conformance suite against it with sbt. It must run inside the ankka-flow repository, with
sbt and a JDK. `ANKKA_FLOW_CONFORMANCE_ONLY` runs only the cases whose names start with its value. See
[Adding a language SDK](../contributing/language-sdks.md).
