# Scala SDK

> Every public name of the ankka-flow Scala SDK — Streamlet and its port and parameter factories, the graph delta outlet, records, Config, Serve, the testkit Harness — and the descriptor and conformance commands.

Source: https://flow.ankka.cloud/reference/scala-sdk/
The artifact is `com.thinkmorestupidless:ankka-flow-sdk_3` on Maven Central, built from
[`sdks/scala`](https://github.com/thinkmorestupidless/ankka-flow/blob/main/sdks/scala), versioned as
the ankka-flow release it belongs to:

```scala
libraryDependencies += "com.thinkmorestupidless" %% "ankka-flow-sdk" % "<version>"
```

It is compiled for Scala 3.3 LTS, so a project on Scala 3.3 or any later Scala 3 can depend on it,
and for Java 21; on an older JVM the first SDK class fails to load, naming its class version. It
depends on `ankka-flow-protocol_3` at the same version (the protocol's generated messages, the
canonical descriptor writer and its validation), on grpc-java with ScalaPB, and on `slf4j-api` with
no binding.

## Names

Everything below is in the package `com.thinkmorestupidless.ankka.flow.sdk` unless another is named.

| name | what it is |
|---|---|
| `Streamlet` | the base class of every streamlet |
| `JsonInlet`, `JsonOutlet`, `Outlet`, `Port` | ports with a JSON contract |
| `GraphDeltaOutlet` | an outlet of graph deltas, which builds each record with its element key |
| `graph` | the object behind it: `nodeKey`, `edgeKey`, `read`, `Delta`, `SchemaName` |
| `Parameter[T]` | a typed configuration parameter, made by the `parameter` factories |
| `Config` | a streamlet's resolved parameter values |
| `Record`, `Batch`, `Emit` | a record, a batch of records, a record for an outlet |
| `UndeclaredOutlet` | the exception that fails a batch which emits to an outlet it does not declare |
| `Serve`, `Server` | runs a streamlet for the sidecar, and the running server |
| `Descriptor` | the descriptor: the `Spec`, its canonical JSON, and the command that writes it |
| `testkit.Harness` | runs a streamlet over in-memory inlets and outlets |
| `conformance.Conformance`, `conformance.ConformanceMain` | the reference streamlet and the main that serves it |

## `Streamlet`

```scala
abstract class Streamlet(val name: String, val description: String = ""):
  def process(batch: Batch): Iterable[Emit]
  final def inlets: Seq[JsonInlet]
  final def outlets: Seq[Outlet]
  final def parameters: Seq[Parameter[?]]
  final def config: Config
  final def configure(values: Map[String, String]): Unit
```

- `name` is required: 1 to 63 of `[a-z0-9-]`, not starting or ending with `-`. A name the protocol
  refuses throws `IllegalArgumentException` when the streamlet is constructed.
- Ports and parameters are `val`s built with the protected factories below. Each factory registers
  what it returns, in declaration order; nothing is discovered by scanning. A port name or parameter
  key used twice, or one the descriptor rules refuse, throws `IllegalArgumentException` where it is
  declared.
- `config` holds the parameter values: the declared defaults until `configure` applies deployed ones.
  The server calls `configure` with the values the sidecar's `Start` carries; the harness with the
  map it is given. A value is the text the protocol carries; a key no parameter declares, or a value
  of the wrong type, throws `IllegalArgumentException`.
- `process` is called once per batch on a worker thread; never twice at once for one partition,
  possibly concurrently for different partitions. Returning acknowledges the batch; throwing fails
  it; returning no emit for a record skips it. An emit to an undeclared outlet fails the batch with
  `UndeclaredOutlet`.
- A streamlet that `Serve` or the `Descriptor` command instantiates by class name has a public
  constructor with no arguments.
- `Streamlet.runBatch(streamlet, batch): Iterator[Emit]` runs `process` and checks each emit's
  outlet; the server and the harness share it.

## Ports

```scala
protected def inlet(name: String, schemaName: String): JsonInlet
protected def outlet(name: String, schemaName: String): JsonOutlet
protected def graphDeltaOutlet(name: String): GraphDeltaOutlet
```

`name` is the wire name, matching `[a-z][a-z0-9-]{0,62}`. `schemaName` is required and must not be
empty. Every port has `name`, `schemaName`, `format` (`"json"`) and `fingerprint`, the Base64 of the
SHA-256 of the schema name.

```scala
JsonOutlet.emit(record: Record): Emit
JsonOutlet.emit(value: Array[Byte], key: Option[Array[Byte]] = None,
                headers: Seq[(String, Array[Byte])] = Nil): Emit
```

`emit(record)` forwards the record: the same key, headers and value, and the same offset and
timestamp. Replace parts of it with the case class's `copy`: `emit(record.copy(value = ...))`, or
`record.copy(key = None)` to send it without a key. `emit(value, key, headers)` builds a new record.

## Graph deltas

A `graphDeltaOutlet` is an outlet whose contract is `ankka.graph-delta.v1`. Its four methods build a
[graph delta](graph-deltas.md) and return an `Emit` whose key is the delta's element key,
`node:<id>` or `edge:<id>`. No method takes a key, so a mapper cannot choose a wrong one.

```scala
node(id: String, version: Long, labels: Seq[String] = Nil,
     properties: Map[String, Any] = Map.empty, source: Option[Record] = None): Emit
edge(id: String, version: Long, `type`: String, fromId: String, toId: String,
     properties: Map[String, Any] = Map.empty, source: Option[Record] = None): Emit
tombstoneNode(id: String, version: Long, source: Option[Record] = None): Emit
tombstoneEdge(id: String, version: Long, `type`: String, fromId: String, toId: String,
              source: Option[Record] = None): Emit
```

`source` is the input record the delta was derived from. Its headers, offset and timestamp are
carried, so the record counts as not skipped; its key is not. `fromId` and `toId` are written as the
contract's `from` and `to`. A merge always carries `labels` and `properties`, empty when none are
given.

Each method throws `IllegalArgumentException` for what the sink would refuse, so a mistake fails the
batch in the mapper and never reaches the topic:

| Argument | Refused when |
|---|---|
| `id`, `fromId`, `toId` | empty |
| `version` | negative |
| `labels`, `type` | a label or the type is not an identifier, `[A-Za-z_][A-Za-z0-9_]*` |
| `properties` | a name is `id`, `_version` or `_deleted`; a value is `null`, a map, a non-finite number, an integer beyond 64 bits, an empty collection, or a collection of more than one kind of scalar |

A property value is a `String`, `Boolean`, `Int`, `Long`, `Double`, `Float`, a `BigInt` within 64
bits, or a non-empty collection of one of those. A `Double` with no fractional part counts as an
integer, as the sink reads it, so `Vector(1.5, 2.0)` is a mixed collection and is refused.

`graph` also has:

| name | what it is |
|---|---|
| `nodeKey(id): Array[Byte]`, `edgeKey(id): Array[Byte]` | the element key of a node or an edge: `node:<id>`, `edge:<id>` |
| `read(record): Delta` | parses an emitted record, for tests; throws `IllegalArgumentException` when the value is not a delta or the key is not its element key |
| `Delta` | a case class: `kind`, `element`, `id`, `version`, `labels`, `type`, `fromId`, `toId`, `properties`, `key` |
| `SchemaName` | `"ankka.graph-delta.v1"` |

`Delta.kind` is `"node"`, `"edge"` or `"tombstone"`, and `Delta.element` is `"node"` or `"edge"`
whichever the kind. `read` gives a whole-number property as a `Long`, as the sink stores it.

## Parameters

```scala
protected object parameter:
  def string(key: String, default: String = "", description: String = ""): Parameter[String]
  def integer(key: String, default: Long | Int | String | Null = null, description: String = ""): Parameter[Long]
  def double(key: String, default: Double | String | Null = null, description: String = ""): Parameter[Double]
  def boolean(key: String, default: Boolean | String | Null = null, description: String = ""): Parameter[Boolean]
  def duration(key: String, default: FiniteDuration | String | Null = null, description: String = ""): Parameter[FiniteDuration]
  def memorySize(key: String, default: Long | Int | String | Null = null, description: String = ""): Parameter[Long]
```

`key` matches `[a-z][a-z0-9-]*`. A parameter with no default is required when the pipeline is
deployed; reading it from `config` when it has no value throws `NoSuchElementException`. A default
that is not a value of the parameter's type throws `IllegalArgumentException` at declaration. A
default is the value or its text as the protocol carries it.

| factory | descriptor type | `config(p)` returns | accepts |
|---|---|---|---|
| `string` | `STRING` | `String` | any text |
| `integer` | `INTEGER` | `Long` | an integer |
| `double` | `DOUBLE` | `Double` | a number |
| `boolean` | `BOOLEAN` | `Boolean` | `true` or `false` |
| `duration` | `DURATION` | `FiniteDuration` | a HOCON duration: `100 ms`, `5m`, `1.5 s`; a bare number is milliseconds |
| `memorySize` | `MEMORY_SIZE` | `Long`, in bytes | a HOCON size: `1 MiB`, `512k`, `10MB`; a bare number is bytes |

A `FiniteDuration` default is written as the protocol carries it: whole milliseconds as `ms`, else
microseconds as `us`.

`Parameter[T]` has `key`, `configType`, `defaultValue` (the text, empty when there is none) and
`description`. `Config(p)` returns the typed value; `Config.get(key): Option[Any]` returns a value by
key. `Config.resolve(parameters, values)` resolves a map of text values over the defaults.

## Records

```scala
final case class Record(
    value: Array[Byte],
    key: Option[Array[Byte]] = None,
    headers: Seq[(String, Array[Byte])] = Nil,
    offset: Long = -1,
    timestampMs: Long = 0):
  def valueString: String
  def keyString: Option[String]

final case class Batch(inlet: String, partition: Int, records: Vector[Record]) extends Iterable[Record]

final case class Emit(outlet: String, record: Record)
```

A record is exactly what Kafka holds; nothing is decoded. `valueString` and `keyString` read the
bytes as UTF-8, a convenience the SDK applies nowhere itself. Two records are equal when their bytes,
headers, offset and timestamp are. Headers keep their order. A batch's records come from one
partition of one inlet, in offset order. The server sends each emit to the sidecar as `process`
produces it; the sidecar holds a batch's emits until the batch is acknowledged and writes none of
them if it fails.

## `Serve`

```scala
object Serve:
  val DefaultPort: Int        // 9010
  val Loopback: String        // "127.0.0.1"
  def port(env: Map[String, String] = sys.env): Int
  def start(streamlet: Streamlet, port: Int = 0): Server
  def run(streamlet: Streamlet): Unit
  def main(args: Array[String]): Unit       // <streamlet class>

final class Server:
  def port: Int
  def addresses: Seq[java.net.SocketAddress]
  def close(): Unit
  def awaitTermination(): Unit
```

`run` serves the protocol's `Discovery` and `Streamlet` services on `127.0.0.1` and `port()`, which is
`FLOW_PROCESS_PORT`, or 9010 when that is unset, and blocks until the process is stopped; a shutdown
hook closes the server. `start` binds `port` (0 for any free one) and returns at once. It binds
loopback only. A declaration the protocol refuses is refused before anything is bound. `main` serves
the streamlet class it is named, constructed with no arguments.

- `Discover` answers with the streamlet's descriptor. `ReportError` logs each problem the sidecar
  reports at `ERROR` level.
- One conversation runs at a time. A new `Run` ends the previous one, because the sidecar has
  reconnected; the old conversation's batches finish and send nothing. `Start` applies the deployed
  parameter values; a batch before `Start` ends the conversation; `Stop` lets in-flight batches
  finish and ends it.
- An inbound message is limited to 16 MiB.

The SDK logs through slf4j under the logger name `ankka.flow` and brings no binding.

## `testkit`

```scala
final class Harness(streamlet: Streamlet, config: Map[String, String] = Map.empty)
```

| member | what it does |
|---|---|
| `inlet(name).put(value, key = None, headers = Nil)` | queues a record on a declared inlet |
| `run(partitions = Harness.singlePartition, maxRecords = None)` | processes everything queued: per inlet, per partition, batches in offset order |
| `outlet(name).records` | the records emitted to a declared outlet, in order |
| `skipped` | records of successful batches that no emit was derived from |
| `failures` | a `Failure(batch, error)` per failed batch; its emits are discarded |
| `batches` | every batch given to `process`, in order |

`partitions` maps a key to a partition number (default: everything on partition 0); `maxRecords`
caps a batch (default: one batch per partition). `Harness.hashPartitioner(n)` places keys on `n`
partitions by CRC-32 of the key, with a keyless record on partition 0, as the Python harness does.
Offsets are numbered per inlet and partition across runs. An undeclared inlet or outlet throws
`NoSuchElementException` naming the declared ones; an undeclared configuration key, or a value of the
wrong type, throws `IllegalArgumentException`. See [Test a streamlet](../build/testing.md).

## Commands

### `Descriptor`

```bash
sbt "runMain com.thinkmorestupidless.ankka.flow.sdk.Descriptor <streamlet class> <path> [--check]"
```

Constructs the streamlet class with no arguments, validates its descriptor and writes it to `path`
as canonical JSON, creating the directory. `--check` writes nothing and exits 1 when the file is
missing or differs. A class that is not a streamlet, or one that refuses its own declaration, exits
2 naming the problem. In code, `Descriptor.spec(streamlet)`, `Descriptor.write(streamlet)` and
`Descriptor.validate(streamlet)` give the `Spec`, its JSON and every rule it breaks, and
`Descriptor.run(args)` the exit code.

The descriptor's `sdk` block is `{"name": "ankka-flow-scala", "version": <the SDK's version>}`; a build
of the SDK from anything but a release reports `0.0.0`. See [Descriptor](descriptor.md).

### `ConformanceMain`

```bash
sbt "runMain com.thinkmorestupidless.ankka.flow.sdk.conformance.ConformanceMain 9010"
```

Serves the SDK's reference streamlet, `conformance.Conformance`, on `127.0.0.1` and the port given
(9010 when none is), for the platform's conformance suite. In the ankka-flow repository the suite
serves the same streamlet in process: `sbt sdkConformance`. See
[Adding a language SDK](../contributing/language-sdks.md).
