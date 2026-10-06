# ankka-flow for Scala

Write [ankka-flow](https://flow.ankka.cloud/) streamlets in Scala. You declare ports and parameters and
implement `process`; the sidecar the platform runs beside your container owns everything Kafka.

```scala
libraryDependencies += "com.thinkmorestupidless" %% "ankka-flow-sdk" % "<version>"   // Scala 3.3+, Java 21+
```

```scala
import com.thinkmorestupidless.ankka.flow.protocol.Json
import com.thinkmorestupidless.ankka.flow.sdk.*

final class CartRouter extends Streamlet("cart-router"):
  val in        = inlet("in", schemaName = "cart-events.v1")
  val valid     = outlet("valid", schemaName = "cart-events.v1")
  val review    = outlet("review", schemaName = "cart-events.v1")
  val threshold = parameter.integer("review-threshold", default = 100)

  def process(batch: Batch): Iterable[Emit] =
    batch.records.map { record =>
      val total = Json.parse(record.valueString).toOption.flatMap(_.field("total"))
      total match
        case Some(Json.Num(n)) if n > config(threshold) => review.emit(record)
        case _                                          => valid.emit(record)
    }

object Main:
  def main(args: Array[String]): Unit = Serve.run(new CartRouter)   // 127.0.0.1:$FLOW_PROCESS_PORT (9010)
```

- `process` runs once per batch on a worker thread. One batch per partition is in flight at a time,
  so it never runs twice for one partition at once; different partitions run concurrently.
- Returning acknowledges the batch. Throwing fails it, and the sidecar redelivers it from the last
  commit. Emitting nothing for a record skips it.
- Records are bytes with a key and headers. The SDK decodes nothing and keeps nothing between batches.
- The descriptor is written from the declaration:
  `sbt "runMain com.thinkmorestupidless.ankka.flow.sdk.Descriptor cart.CartRouter flow/descriptor.json"`,
  and with `--check` it fails when the committed file differs.
- `com.thinkmorestupidless.ankka.flow.sdk.testkit.Harness` runs a streamlet with no Kafka and no
  sidecar, batching and partitioning by key as the sidecar does.

The SDK is a module of the ankka-flow build and depends on its `protocol` module, published beside
it as `ankka-flow-protocol`. In this repository:

```bash
sbt sdk/test                     # the descriptor fixtures, the server, the harness, graph deltas
sbt sdkConformance               # the conformance suite against the SDK's reference streamlet
just sdk-scala                   # both, and the Scala cart router sample
```

The [Scala guide](https://flow.ankka.cloud/build/scala-streamlet/) and the
[Scala SDK reference](https://flow.ankka.cloud/reference/scala-sdk/) cover the rest. The sample is
`samples/cart-router-scala`.
