# Contract: the Scala SDK's public API

Package `com.thinkmorestupidless.ankka.flow.sdk`, artifact `com.thinkmorestupidless:ankka-flow-sdk_3`.
Everything a reader may name; the Scala SDK reference page lists exactly this. Names are the Python
SDK's, in Scala's case.

```scala
abstract class Streamlet(val name: String, val description: String = ""):
  protected def inlet(name: String, schemaName: String): JsonInlet
  protected def outlet(name: String, schemaName: String): JsonOutlet
  protected def graphDeltaOutlet(name: String): GraphDeltaOutlet
  protected object parameter:
    def string(key: String, default: String = "", description: String = ""): Parameter[String]
    def integer(key: String, default: Long, description: String = ""): Parameter[Long]
    def double(key: String, default: Double, description: String = ""): Parameter[Double]
    def boolean(key: String, default: Boolean, description: String = ""): Parameter[Boolean]
    def duration(key: String, default: FiniteDuration, description: String = ""): Parameter[FiniteDuration]
    def memorySize(key: String, default: Long, description: String = ""): Parameter[Long]   // bytes
  def inlets: Seq[JsonInlet]            // in declaration order
  def outlets: Seq[Outlet]
  def parameters: Seq[Parameter[?]]
  def config: Config                    // the defaults until Start applies the deployed values
  def process(batch: Batch): Iterable[Emit]

final case class Record(
    value: Array[Byte],
    key: Option[Array[Byte]] = None,
    headers: Seq[(String, Array[Byte])] = Nil,
    offset: Long = -1,
    timestampMs: Long = 0)
final case class Batch(inlet: String, partition: Int, records: Vector[Record]) extends Iterable[Record]
final case class Emit(outlet: String, record: Record)

sealed trait Port { def name: String; def schemaName: String; def fingerprint: String }
final class JsonInlet  extends Port
sealed trait Outlet extends Port
final class JsonOutlet extends Outlet:
  def emit(record: Record): Emit                                          // same key, headers, value
  def emit(record: Record, value: Array[Byte] = ..., key: Option[Array[Byte]] = ..., headers: Seq[(String, Array[Byte])] = ...): Emit
  def emit(value: Array[Byte], key: Option[Array[Byte]], headers: Seq[(String, Array[Byte])] = Nil): Emit
final class GraphDeltaOutlet extends Outlet:
  def emit(delta: graph.Delta, from: Record): Emit                        // keyed by the element; validated
  def emit(delta: graph.Delta): Emit

final class Parameter[T](val key: String, val `type`: ConfigType, val default: T, val description: String)
final class Config:
  def apply[T](p: Parameter[T]): T
  def get(key: String): Option[String]                                    // the raw value, as Start carried it
final class UndeclaredOutlet(outlet: String) extends RuntimeException

object Descriptor:
  def spec(streamlet: Streamlet): ankka.flow.v1.discovery.Spec            // sdk = ankka-flow-scala, BuildInfo.version
  def write(streamlet: Streamlet): String                                  // canonical JSON
  def validate(streamlet: Streamlet): Vector[String]
  def main(args: Array[String]): Unit                                      // <class> <path> [--check]

object Serve:
  def run(streamlet: Streamlet): Unit                                      // 127.0.0.1:$FLOW_PROCESS_PORT, blocks until stopped
  def start(streamlet: Streamlet, port: Int = 0): Server                   // for tests; Server.port, Server.close()
  def main(args: Array[String]): Unit                                      // <class>

object graph:                                                              // the deltas of ankka.graph-delta.v1
  enum Delta: ...                                                          // as the Python SDK's graph module, name for name

package testkit:
  final class Harness(streamlet: Streamlet, config: Map[String, String] = Map.empty):
    def inlet(name: String): InletQueue                                    // put(value, key = None, headers = Nil)
    def outlet(name: String): OutletRecords                                // records: Vector[Record]
    def run(partitions: Option[Array[Byte]] => Int = Harness.singlePartition, maxRecords: Option[Int] = None): Unit
    def failures: Vector[Failure]                                          // Failure(batch, error)
    def skipped: Vector[Record]
    def batches: Vector[Batch]
  object Harness:
    def hashPartitioner(partitions: Int): Option[Array[Byte]] => Int       // CRC32 of the key mod n; None → 0
```

## Rules the API carries

- A `Streamlet` is constructed with no arguments; `Descriptor` and `Serve` instantiate it by class
  name. Construction validates the declaration with the protocol's `DescriptorValidation`; a
  refused declaration throws `IllegalArgumentException` with the rule's message, before anything
  is written or served.
- `process` runs on a worker thread, never twice at once for one `(inlet, partition)`, possibly
  concurrently for different partitions. Returning acknowledges the batch; throwing fails it; an
  emit to an undeclared outlet fails it with `UndeclaredOutlet`. Emitting nothing for a record skips
  it. The SDK holds nothing between batches.
- `Serve.run` binds the loopback address only. `FLOW_PROCESS_PORT` unset means 9010.
- `Descriptor.write` is `DescriptorJson.write` of `spec`: snake_case, sorted keys, two-space
  indentation, one trailing newline. With `sdk = SdkInfo("fixture", "0.0.0")`, the six fixture
  declarations reproduce `protocol/fixtures/descriptors/*.json` byte for byte.
- `Harness.run` places each pending record by `partitions(key)`, numbers offsets per `(inlet,
  partition)` across runs, and processes one batch per partition in partition order, or several of
  at most `maxRecords`; a failed batch's emits are discarded and recorded in `failures`.
- The reference streamlet `com.thinkmorestupidless.ankka.flow.sdk.conformance.Conformance`
  (test scope of `sdk`; served by `ConformanceMain <port>`) behaves by key exactly as
  `protocol/fixtures/declarations/conformance.md` says.
