# Write a streamlet in Scala

> Declare a streamlet's ports and parameters with the Scala SDK, process batches into emits, skip or fail records, serve it to the sidecar, and write its descriptor.

Source: https://flow.ankka.cloud/build/scala-streamlet/
A Scala streamlet is a subclass of `Streamlet`. It declares its inlets, outlets and parameters as
`val`s built with the base class's factories, and implements one method, `process`, which takes a
batch of records from one inlet partition and returns the records to send to its outlets.
`Serve.run` runs it where the sidecar in the same pod can reach it, and the `Descriptor` command
writes the descriptor that a blueprint is checked against.

The SDK is `com.thinkmorestupidless:ankka-flow-sdk_3` on Maven Central, built from
[`sdks/scala`](https://github.com/thinkmorestupidless/ankka-flow/blob/main/sdks/scala). Its version is
the ankka-flow release it belongs to, and the sidecar of the same release speaks its protocol.

## Start a project

Any sbt project on Scala 3.3 or later and Java 21 or later. Add the SDK:

```scala
libraryDependencies += "com.thinkmorestupidless" %% "ankka-flow-sdk" % "<version>"
```

It brings the protocol module it is written against, `ankka-flow-protocol`, at the same version. To
package the process as an image, add sbt-native-packager and enable `JavaAppPackaging` and
`DockerPlugin`; [Build an image](images.md) shows the settings.

```text
my-streamlet/
├── build.sbt                         # the SDK, the main class, the image
├── blueprint.conf                    # a pipeline of this one streamlet and its topics
├── flow/descriptor.json              # written by the Descriptor command; committed
└── src/
    ├── main/scala/cart/CartRouter.scala
    ├── main/scala/cart/Main.scala    # Serve.run
    └── test/scala/cart/CartRouterSuite.scala
```

## Declare the streamlet

This is the cart router from
[`samples/cart-router-scala`](https://github.com/thinkmorestupidless/ankka-flow/blob/main/samples/cart-router-scala):
one inlet of cart events, two outlets, and one parameter. It declares the same streamlet as the
Python sample, so one blueprint deploys either.

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

- The constructor's first argument is the name a blueprint refers to: 1 to 63 lower-case letters,
  digits and hyphens, not starting or ending with a hyphen. The description is optional.
- A port's first argument is its wire name, which a blueprint uses (`router.valid`); the `val`'s
  name does not matter. Port names match `[a-z][a-z0-9-]{0,62}` and are unique across inlets and
  outlets together.
- `schemaName` is the port's contract. Two ports connect only when their schema names are equal. See
  [Contracts](../concepts/contracts.md).
- A parameter's key matches `[a-z][a-z0-9-]*`. A parameter with no `default` must be given a value
  when the pipeline is deployed. A default is the value (`100`, `0.5`, `true`, `100.millis`) or its
  text as the protocol carries it (`"100 ms"`, `"1 MiB"`). `config(parameter)` returns the value typed
  by the parameter: `Long` for `parameter.integer`, `FiniteDuration` for `parameter.duration`, and so
  on; the full list is in the [Scala SDK reference](../reference/scala-sdk.md#parameters).

Each factory registers what it returns, so the declaration is the construction of the object and
nothing is discovered by scanning. Declaration order does not matter; the descriptor sorts ports by
name and parameters by key. A port name or parameter key declared twice, or one the descriptor rules
refuse, throws `IllegalArgumentException` where it is declared.

## Process a batch

`process` receives a `Batch`: records from one partition of one inlet, in offset order. Each `Record`
has `value` (`Array[Byte]`), `key` (`Option[Array[Byte]]`), `headers` (a `Seq` of `(String,
Array[Byte])`, in order), `offset` and `timestampMs`. Nothing is decoded; `valueString` and
`keyString` read the bytes as UTF-8 when that is what they hold, and the protocol module's `Json`
parses JSON.

For each record, `process` returns any number of emits:

- `outlet.emit(record)` sends the record to that outlet unchanged: the same key, headers and value.
- `outlet.emit(record.copy(value = ...))` sends a copy with the given parts replaced.
  `record.copy(key = None)` sends it without a key.
- `outlet.emit(value, key, headers)` builds a new record.

Keep the key when downstream streamlets rely on per-key order: records with the same key land on the
same partition of the outlet topic, and a keyless record is placed by Kafka's default partitioner.

How `process` ends decides what happens to the batch:

| `process` | the batch |
|---|---|
| returns | acknowledged; the sidecar writes every emit, then commits the offsets |
| returns no emit for a record | that record is skipped, and still committed with the batch |
| throws | failed; its emits are discarded and the batch is delivered again from the last commit |
| returns an emit to an outlet it does not declare | failed, with `UndeclaredOutlet` |

A record the streamlet cannot use, including one that does not decode, should be skipped rather than
thrown on. A thrown exception redelivers the same batch indefinitely, which stalls its partition until
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

```scala
import com.thinkmorestupidless.ankka.flow.sdk.Serve

object Main:
  def main(args: Array[String]): Unit = Serve.run(new CartRouter)
```

`Serve.run` binds `127.0.0.1` on `FLOW_PROCESS_PORT` (9010 when unset), the only variable the platform
gives the process, and blocks until the process is stopped. It answers the sidecar's discovery with the
streamlet's descriptor, logs any problems the sidecar reports when it refuses the process, applies the
deployed parameter values before the first batch, and runs batches as they arrive. It logs through
slf4j and brings no binding; add one, such as `slf4j-simple`, to see its lines. The process needs no
Kafka address, no credentials and no open ports.

## Write the descriptor

The descriptor is the streamlet's declaration as canonical JSON. `flow verify` and `flow generate`
check a blueprint against it, and the sidecar refuses to start a process whose declaration differs
from the descriptor it was deployed with.

```bash
sbt "runMain com.thinkmorestupidless.ankka.flow.sdk.Descriptor cart.CartRouter flow/descriptor.json"
sbt "runMain com.thinkmorestupidless.ankka.flow.sdk.Descriptor cart.CartRouter flow/descriptor.json --check"
```

The command constructs the named class with no arguments, writes the file, and with `--check` exits 1
when the committed file differs; it exits 2 when the class is not a streamlet or refuses its own
declaration. Fork `run` (`run / fork := true`) so the exit code is the task's. Run it after every
change to a port, a contract or a parameter, and commit `flow/descriptor.json`. Never edit it by hand.
The format is on the [Descriptor](../reference/descriptor.md) page.

## Next steps

- [Test a streamlet](testing.md) with the Harness, without Kafka or a sidecar.
- [Build an image](images.md) holding only the streamlet's code.
- [Write a blueprint](blueprints.md) that connects the streamlet to topics.
