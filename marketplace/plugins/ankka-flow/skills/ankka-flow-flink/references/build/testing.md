# Test a streamlet

> Test a Pekko Streams streamlet's stages with PekkoHarness and a Flink streamlet's job with FlinkHarness, each over records in memory with no Kafka and no cluster, and check the descriptor beside the tests.

Source: https://flow.ankka.cloud/build/testing/
Each engine's SDK has a harness that runs a streamlet's own logic over records held in memory, with
no broker and no cluster, from the language's own test runner:

| Engine | Harness | What it runs |
|---|---|---|
| Pekko Streams | `PekkoHarness`, in `ankka-flow-pekko-streams` | each inlet's stage, record by record, returning what each outlet was written and which positions would be committed |
| Flink | `FlinkHarness`, in `ankka-flow-flink-testkit` | the streamlet's whole job on a local Flink, over bounded inputs, returning what each outlet was given |

Both pass every record through its port's codec on the way in and out, as a running streamlet would,
so a test also proves the codecs. Both take parameter values as text, as the deployed settings carry
them.

## A Pekko Streams streamlet

This is the test of the sample's cleaner, which keeps the cart events within its maximum, keyed by
their cart, and drops the rest:

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

- `PekkoHarness(streamlet, "key" -> "value", …)` sets parameters by their keys; a parameter left out
  takes its default.
- `run(inlet -> records, …)` runs each inlet's stage over its records, in order. A `Record` is an
  optional key, a value of the inlet's type and optional headers.
- `result(outlet)` is what the outlet was written, in order, as records with their keys and headers.
- `result.committed` is the positions, counted from 0, whose offsets would be committed;
  `result.committedOf(inlet)` gives one inlet's when several were run. A record whose stage produced
  an element is committed, even an element that writes nothing (`None`, `Nil`); a record the stage
  filters out is covered by the next record's commit.

Assert on `committed` as well as on the outlets: a record committed before what it produced was
written is a record lost on a restart.

A run fails as the stream would:

- An element that throws, or a value its codec cannot decode or encode, fails the run with
  `PekkoHarness.Failed`, which carries the inlet, the position of the record that failed, the positions
  committed before it, and the cause. Nothing after the failed record is committed.
- A required parameter with no value, a value not of its parameter's type, and records for an inlet
  the logic gives no stage for each fail with an `IllegalArgumentException` that names it.

The harness runs one record at a time on one partition. It does not reproduce Kafka, partitions,
rebalances or redelivery; the SDK's own suites run its commit after write against a real broker.

## A Flink streamlet

This is the test of the sample's counter, which counts each cart's events in windows of the time each
event carries:

```java
import com.thinkmorestupidless.ankka.flow.flink.Record;
import com.thinkmorestupidless.ankka.flow.flink.testkit.FlinkHarness;
import java.util.Comparator;
import java.util.List;
import org.junit.jupiter.api.Test;

class CartCounterTest {

  static final long T0 = 1_767_225_600_000L; // 2026-01-01T00:00:00Z

  static Record<CleanCart> event(String cart, long seq, long secondsIn) {
    long at = T0 + secondsIn * 1000;
    // the Kafka timestamp is the time the record was written, not the event's own time
    return Record.of(cart, new CleanCart(cart, seq, 10, at), T0 + 3_600_000);
  }

  static List<CartCount> counts(List<Record<CartCount>> records) {
    return records.stream().map(Record::value)
        .sorted(Comparator.comparing(CartCount::cartId).thenComparing(CartCount::windowStart)).toList();
  }

  @Test
  void countsEachCartsEventsInWindowsOfTheirOwnTime() {
    CartCounter counter = new CartCounter();
    var result = FlinkHarness.of(counter)
        .input(counter.in, List.of(event("a", 1, 10), event("b", 1, 70), event("a", 2, 65), event("a", 3, 20)))
        .run();
    assertEquals(
        List.of(
            new CartCount("a", T0, T0 + 60_000, 2),
            new CartCount("a", T0 + 60_000, T0 + 120_000, 1),
            new CartCount("b", T0 + 60_000, T0 + 120_000, 1)),
        counts(result.records(counter.out)));
  }

  @Test
  void theWindowIsAParameter() {
    CartCounter counter = new CartCounter();
    var result = FlinkHarness.of(counter).parameter("window", "5m")
        .input(counter.in, List.of(event("a", 1, 10), event("a", 2, 250)))
        .run();
    assertEquals(List.of(new CartCount("a", T0, T0 + 300_000, 2)), counts(result.records(counter.out)));
  }
}
```

- `FlinkHarness.of(streamlet)` makes a harness with every parameter at its default, and
  `parameter(key, value)` sets one; a key the streamlet does not declare is refused at once, and a
  value not of its type when the job runs.
- `input(inlet, records)` gives an inlet its records; an inlet given none reads nothing. A `Record` is
  a key, a value, headers and a timestamp: `Record.of(key, value, timestamp)` has no headers. The
  timestamp is what Kafka's would be; a job whose windows use another time, as the counter's use the
  event's own `at`, says so in the watermark strategy it reads its inlet with.
- `run()` builds the streamlet's job on a local Flink with a parallelism of 1, runs it to the end of
  its inputs, and returns a `Result`. The end of a bounded input closes every window still open, so
  each window's result is in it.
- `result.records(outlet)` is what the outlet was given, in the order it was given it. Sort before
  comparing when the order across keys does not matter, as the counter's test does.

A job that throws fails the run with `FlinkHarness.Failed`, whose cause is what the streamlet threw;
the job is not restarted.

The harness takes no checkpoints and talks to no Kafka, so it proves the job's logic and its windows,
not its state across a restart or a new image, nor its transactions. The cluster suite proves those
for the sample's counter; [Write a Flink streamlet](flink-streamlet.md#state-and-a-new-image) says
which changes a new image may make to its state.

## Check the descriptor beside the tests

A streamlet whose committed descriptor differs from its declaration is refused by `flow verify`
and the operator alike. Run the check in the same build as the tests: the streamlet's main, given
`--descriptor-check <file>`, exits `1` with the difference. The sample's builds run it as a task:

```bash
sbt cartCleaner/test cartCleaner/descriptorCheck
sbt cartCounter/test cartCounter/descriptorCheck
```

`--descriptor <file>` rewrites the file from the declaration after a deliberate change to a port, a
contract, a parameter or the name; [Descriptor](../reference/descriptor.md#writing-and-checking-it)
shows both.
