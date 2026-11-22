# Write a Pekko Streams streamlet

> Declare a Pekko Streams streamlet's ports and parameters in Scala, write each inlet's stage with Pekko Streams' own operators, test it with the harness, and package its image.

Source: https://flow.ankka.cloud/build/pekko-streams-streamlet/
A Pekko Streams streamlet is a Scala object that extends `PekkoStreamlet`. It declares its inlets,
outlets and parameters as `val`s built with the base class's factories, and implements one method,
`logic`, which gives each inlet a stage written with Pekko Streams' own operators: `map`, `filter`,
`collect`, `mapConcat`, `mapAsync` and `via`. The SDK reads each inlet's topic, runs the stage, writes
what it emits to the outlets, and commits each record's offset once everything made from it is
written.

The SDK is `com.thinkmorestupidless::ankka-flow-pekko-streams` on Maven Central, built for Scala 3.3
and later on Java 21, with Pekko 1.7 and Pekko Connectors Kafka.

## Start a project

Any sbt project on Scala 3.3 or later:

```scala
libraryDependencies += "com.thinkmorestupidless" %% "ankka-flow-pekko-streams" % "<version>"
```

The sample uses [jsoniter-scala](https://github.com/plokhotnyuk/jsoniter-scala) for its JSON codecs;
add it too to use `Codec.json`. To package the process as an image, add sbt-native-packager and
enable `JavaAppPackaging` and `DockerPlugin`.

## Declare the streamlet

This is the cleaner from
[`samples/cart-pipeline/cleaner`](https://github.com/thinkmorestupidless/ankka-flow/blob/main/samples/cart-pipeline/cleaner):
one inlet of cart events, one outlet of clean ones, and one parameter, the largest total it keeps.

```scala
import com.github.plokhotnyuk.jsoniter_scala.core.{JsonValueCodec, readFromArray}
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import com.thinkmorestupidless.ankka.flow.pekko.*

import scala.util.Try

/** What the shop publishes: something that happened to a cart. */
final case class CartEvent(cartId: String, seq: Long, total: Long, at: Long)

/** What the cleaner writes: an event that decoded and is within the maximum, keyed by its cart. */
final case class CleanCart(cartId: String, seq: Long, total: Long, at: Long)

object CleanCart:
  given JsonValueCodec[CleanCart] = JsonCodecMaker.make

object CartEvent:
  given JsonValueCodec[CartEvent] = JsonCodecMaker.make

  /** The event, when the bytes are one. */
  def parse(bytes: Array[Byte]): Option[CartEvent] = Try(readFromArray[CartEvent](bytes)).toOption

object Cleaner
    extends PekkoStreamlet(
      "cart-cleaner",
      "Drops cart events that do not decode or whose total is above the maximum, and writes the rest clean."
    ):
  // The inlet reads bytes: a record that is not a cart event is the cleaner's to drop, where a
  // typed codec would fail the stream on it.
  val in  = inlet("in", Contract("cart-events.v1", schema = "cart-events.v1.json"), Codec.bytes)
  val out = outlet("out", Contract("clean-carts.v1"), Codec.json[CleanCart])
  val maxTotal =
    intParameter(
      "max-total",
      default = 10000,
      description = "Events whose total is above this are dropped."
    )

  def logic(ctx: Context): Logic =
    val max = ctx(maxTotal)
    from(in) {
      _.map(r =>
        CartEvent.parse(r.value).filter(_.total <= max).map { e =>
          out(e.cartId, CleanCart(e.cartId, e.seq, e.total, e.at))
        }
      )
    }
```

- `inlet` and `outlet` take the port's name, its [contract](../concepts/contracts.md) and the codec
  for its records. `Codec.json[A]` reads and writes JSON with jsoniter-scala, `Codec.string` UTF-8
  text, and `Codec.bytes` the bytes as they are. A record a codec cannot decode fails the stream, so
  a streamlet that would rather drop such records reads `Codec.bytes` and decodes in its stage, as
  the cleaner does.
- Each parameter has a type, a default and a description; `ctx(parameter)` reads its deployed value.
- `from(inlet) { stage => … }` is the inlet's stage. Its elements are records with their offsets
  carried alongside, and what it returns are emits: one `Emit`, a `Seq` of them (`Nil` writes
  nothing) or an `Option`. `out(key, value)` makes an emit for the outlet `out`. A record the stage
  emits nothing for is dropped, and its offset is committed with the next.
- A streamlet with several inlets joins their stages with `and`.

## Run it

`PekkoStreamlets.run` is the process's `main`:

```scala
import com.thinkmorestupidless.ankka.flow.pekko.PekkoStreamlets

object Main:
  def main(args: Array[String]): Unit = PekkoStreamlets.run(Cleaner, args)
```

In a pod it reads `streamlet.properties` from the directory `FLOW_CONFIG_DIR` names, refuses settings
that do not match the declared ports and parameters, and runs. Given `--descriptor <file>` it writes
the streamlet's descriptor instead, and `--descriptor-check <file>` fails when the file differs from
the declaration.

## Test it

`PekkoHarness` runs the stages over records in memory, with no broker, and returns what each outlet
wrote and which positions would be committed.

```scala
import com.thinkmorestupidless.ankka.flow.pekko.{PekkoHarness, Record}

class CleanerSuite extends munit.FunSuite:

  private def event(json: String) = Record(Some("ignored"), json.getBytes(UTF_8))

  test("keeps cart events within the maximum, keyed by their cart, and drops the rest") {
    val result = PekkoHarness(Cleaner, "max-total" -> "250").run(
      Cleaner.in -> Seq(
        event("""{"cartId":"c1","seq":1,"total":100,"at":1000}"""),
        event("""{"cartId":"c2","seq":1,"total":900,"at":1001}"""), // above the maximum
        event("""not a cart event"""),                              // does not decode
        event("""{"cartId":"c1","seq":2,"total":250,"at":1002}""")
      )
    )
    assertEquals(
      result(Cleaner.out),
      Vector(
        Record(Some("c1"), CleanCart("c1", 1, 100, 1000)),
        Record(Some("c1"), CleanCart("c1", 2, 250, 1002))
      )
    )
    // Every record is committed, the dropped ones too.
    assertEquals(result.committed, Vector(0, 1, 2, 3))
  }
```

## No windows and no state

A Pekko Streams streamlet handles each record on its own. It has no windows, no timers and no state
that survives a restart or a rebalance, and nothing in the SDK adds them: a stage that needs any of
them is a [Flink streamlet](flink-streamlet.md).
