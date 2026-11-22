# Contract: the two SDKs

What a streamlet author writes against. Signatures are the promise; bodies are the build's.

## Common to both

- A streamlet is declared: a name, typed inlets and outlets, typed parameters. Nothing is found by
  scanning; the main names the streamlet.
- The main does one of three things: `--descriptor <path>` writes the descriptor;
  `--descriptor-check <path>` compares; no argument runs the streamlet from `FLOW_CONFIG_DIR`.
- Logic never names a topic, a broker, a group or a credential.
- A harness runs the logic with no broker and no cluster.
- The same main runs in an image and on a laptop.

## Pekko Streams: `com.thinkmorestupidless.ankka.flow.pekko` (`ankka-flow-pekko-streams_3`)

```scala
abstract class PekkoStreamlet(val name: String, val description: String = ""):
  protected def inlet[A](name: String, contract: Contract, codec: Codec[A]): Inlet[A]
  protected def outlet[A](name: String, contract: Contract, codec: Codec[A]): Outlet[A]
  protected def stringParameter(key: String, default: String | Null = null, description: String = ""): Parameter[String]
  // intParameter, doubleParameter, booleanParameter, durationParameter, memorySizeParameter likewise
  protected def from[A](inlet: Inlet[A])(flow: Stage[A] => Stage.Out): Logic
  def logic(ctx: Context): Logic                    // combine several inlets with `and`

final case class Record[A](key: Option[String], value: A, headers: Seq[(String, Array[Byte])])
final class Outlet[A]:
  def apply(key: String, value: A): Emit
  def apply(record: Record[A]): Emit
trait Context:
  def apply[A](parameter: Parameter[A]): A
  def system: org.apache.pekko.actor.typed.ActorSystem[?]

object Contract:
  def apply(name: String): Contract
  def apply(name: String, schema: String): Contract   // a classpath resource; fingerprinted
object Codec:
  def json[A](using JsonValueCodec[A]): Codec[A];  val bytes: Codec[Array[Byte]];  val string: Codec[String]

object PekkoStreamlets:
  def run(streamlet: PekkoStreamlet, args: Array[String]): Unit
object PekkoHarness:
  def apply(streamlet: PekkoStreamlet, parameters: (String, String)*): PekkoHarness
final class PekkoHarness:
  def run[A](inputs: (Inlet[A], Seq[Record[A]])*): Result     // Result(outlet): Seq[Record[_]], committed: Seq[Int]
```

`Stage[A]` is `FlowWithContext[Record[A], Offset, Record[A], Offset, NotUsed]`'s operators
(`map`, `filter`, `collect`, `mapConcat`, `mapAsync`, `via`), and `Stage.Out` is one whose
elements are `Emit`, `Seq[Emit]` or `Option[Emit]`.

Promises: at least once; a record's offset is committed only after every `Emit` made from it is
confirmed; a failure restarts the stream from the last commit, indefinitely; per-key order within
a partition; no window, timer or store.

## Flink: `com.thinkmorestupidless.ankka.flow.flink` (`ankka-flow-flink`, Java 17)

```java
public abstract class FlinkStreamlet {
  protected FlinkStreamlet(String name);
  protected final <A> Inlet<A> inlet(String name, Contract contract, Codec<A> codec);
  protected final <A> Outlet<A> outlet(String name, Contract contract, Codec<A> codec);
  protected final Parameter<String> stringParameter(String key, String defaultValue);   // null: required
  // intParameter, doubleParameter, booleanParameter, durationParameter, memorySizeParameter
  public abstract void build(FlinkContext ctx);
}
public interface FlinkContext {
  StreamExecutionEnvironment env();
  <A> DataStream<Record<A>> read(Inlet<A> inlet);                                   // Kafka record timestamps, no watermarks
  <A> DataStream<Record<A>> read(Inlet<A> inlet, WatermarkStrategy<Record<A>> watermarks);
  <A> void write(Outlet<A> outlet, DataStream<Record<A>> stream);
  <A> A get(Parameter<A> parameter);
}
public record Record<A>(String key, A value, List<Header> headers, long timestamp) { }
public final class Contract { static Contract of(String name); static Contract of(String name, String schemaResource); }
public interface Codec<A> extends Serializable { static <A> Codec<A> json(Class<A> type); static Codec<byte[]> bytes(); static Codec<String> string(); }
public final class FlinkStreamlets { public static void run(FlinkStreamlet streamlet, String[] args); }
```

Testkit (`ankka-flow-flink-testkit`):

```java
public final class FlinkHarness {
  public static FlinkHarness of(FlinkStreamlet streamlet);
  public FlinkHarness parameter(String key, String value);
  public <A> FlinkHarness input(Inlet<A> inlet, List<Record<A>> records);
  public Result run();                       // result.records(outlet): List<Record<A>>
}
```

Promises: everything of Flink's DataStream API between `read` and `write`; state kept through a
failure and a new image under Flink's rules for the state's types; each record counted once in
what is written to a managed topic; at least once to any other; offsets committed to the inlet's
group on each checkpoint.

## Images

| Engine | Image |
|---|---|
| Pekko Streams | any JVM image whose entrypoint is the main; the sample uses sbt-native-packager |
| Flink | `FROM flink:2.2.1-java17`; the streamlet's jar at `/opt/flink/usrlib/streamlet.jar`, its dependencies that Flink does not provide beside it |
