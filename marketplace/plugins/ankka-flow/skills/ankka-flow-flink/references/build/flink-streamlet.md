# Write a Flink streamlet

> Declare a Flink streamlet's ports and parameters in Java, build its job against Flink's DataStream API, keep state through a new image, test it with the harness, and package its image.

Source: https://flow.ankka.cloud/build/flink-streamlet/
A Flink streamlet is a Java class that extends `FlinkStreamlet`. It declares its inlets, outlets
and parameters as fields built with the base class's factories, and implements one method,
`build`, which reads its inlets as Flink `DataStream`s, transforms them with Flink's own API, and
writes the results to its outlets. The platform runs it as a Flink job, keeps its state in
checkpoints, and connects every port to its Kafka topic from the settings it is deployed with.

The SDK is `com.thinkmorestupidless:ankka-flow-flink` on Maven Central, with the test harness in
`com.thinkmorestupidless:ankka-flow-flink-testkit`. Both are plain Java libraries for Java 17 or
later, written against Flink 2.2. Flink's own modules are provided by the Flink image the job runs
in, so a build declares them `provided`.

## Start a project

Any Java build that can produce a jar. With sbt:

```scala
libraryDependencies ++= Seq(
  "com.thinkmorestupidless" % "ankka-flow-flink"         % "<version>",
  "com.thinkmorestupidless" % "ankka-flow-flink-testkit" % "<version>" % Test,
  "org.apache.flink"        % "flink-streaming-java"     % "2.2.1"     % Provided
)
```

With Maven:

```xml
<dependencies>
  <dependency>
    <groupId>com.thinkmorestupidless</groupId>
    <artifactId>ankka-flow-flink</artifactId>
    <version><!-- the release of flow you use --></version>
  </dependency>
  <dependency>
    <groupId>com.thinkmorestupidless</groupId>
    <artifactId>ankka-flow-flink-testkit</artifactId>
    <version><!-- the release of flow you use --></version>
    <scope>test</scope>
  </dependency>
  <dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-java</artifactId>
    <version>2.2.1</version>
    <scope>provided</scope>
  </dependency>
</dependencies>
```

## Declare the streamlet

This is the counter from
[`samples/cart-pipeline/counter`](https://github.com/thinkmorestupidless/ankka-flow/blob/main/samples/cart-pipeline/counter):
one inlet of clean cart events, one outlet of counts, and one parameter, the window's length. It
counts each cart's events in tumbling windows of the time each event carries.

```java
import com.thinkmorestupidless.ankka.flow.flink.Codec;
import com.thinkmorestupidless.ankka.flow.flink.Contract;
import com.thinkmorestupidless.ankka.flow.flink.FlinkContext;
import com.thinkmorestupidless.ankka.flow.flink.FlinkStreamlet;
import com.thinkmorestupidless.ankka.flow.flink.FlinkStreamlets;
import com.thinkmorestupidless.ankka.flow.flink.Inlet;
import com.thinkmorestupidless.ankka.flow.flink.Outlet;
import com.thinkmorestupidless.ankka.flow.flink.Parameter;
import com.thinkmorestupidless.ankka.flow.flink.Record;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/** Counts each cart's events in tumbling windows of the time each event carries. */
public final class CartCounter extends FlinkStreamlet {

  final Inlet<CleanCart> in = inlet("in", Contract.of("clean-carts.v1"), Codec.json(CleanCart.class));
  final Outlet<CartCount> out = outlet("out", Contract.of("cart-counts.v1"), Codec.json(CartCount.class));
  final Parameter<Duration> window = durationParameter("window", "1m", "The length of each counting window.");

  public CartCounter() {
    super("cart-counter", "Counts each cart's events in windows of the time they carry.");
  }

  @Override
  public void build(FlinkContext ctx) {
    var events = ctx.read(in, WatermarkStrategy.<Record<CleanCart>>forBoundedOutOfOrderness(Duration.ofSeconds(10))
        .withTimestampAssigner((r, previous) -> r.value().at())
        .withIdleness(Duration.ofSeconds(30)));
    var counts = events
        .keyBy(r -> r.value().cartId())
        .window(TumblingEventTimeWindows.of(ctx.get(window)))
        .aggregate(new Count(), new Emit())
        .returns(TypeInformation.of(new TypeHint<Record<CartCount>>() {}))
        .uid("count");
    ctx.write(out, counts);
  }

  /** Adds one to the window's tally for each event. */
  static final class Count implements AggregateFunction<Record<CleanCart>, Tally, Long> {
    private static final long serialVersionUID = 1L;

    @Override
    public Tally createAccumulator() {
      return new Tally();
    }

    @Override
    public Tally add(Record<CleanCart> event, Tally tally) {
      tally.count++;
      return tally;
    }

    @Override
    public Long getResult(Tally tally) {
      return tally.count;
    }

    @Override
    public Tally merge(Tally a, Tally b) {
      a.count += b.count;
      return a;
    }
  }

  /** One record per cart and window, keyed by the cart, at the window's last millisecond. */
  static final class Emit extends ProcessWindowFunction<Long, Record<CartCount>, String, TimeWindow> {
    private static final long serialVersionUID = 1L;

    @Override
    public void process(String cartId, Context c, Iterable<Long> counts, Collector<Record<CartCount>> o) {
      long n = counts.iterator().next();
      TimeWindow w = c.window();
      o.collect(Record.of(cartId, new CartCount(cartId, w.getStart(), w.getEnd(), n), w.maxTimestamp()));
    }
  }

  public static void main(String[] args) throws Exception {
    FlinkStreamlets.run(new CartCounter(), args);
  }
}
```

- `inlet` and `outlet` take the port's name, its [contract](../concepts/contracts.md) and the codec
  that turns a record's bytes into the value type and back. `Codec.json(type)` reads and writes
  JSON with Jackson; `Codec.string()` and `Codec.bytes()` pass text and bytes through.
- Each parameter has a type, a default and a description, and is read in `build` with
  `ctx.get(parameter)`. The value comes from the blueprint's deploy-time configuration.
- `ctx.read(inlet, watermarks)` is a `DataStream<Record<A>>`. A record carries its key, its value,
  its headers and its Kafka timestamp. The watermark strategy decides which time the job's windows
  use: here the event's own `at`, a bounded ten seconds out of order, with a partition that is quiet
  for thirty seconds not holding the others back.
- `ctx.write(outlet, stream)` writes a `DataStream<Record<B>>`: each record's key is the Kafka key
  and its timestamp the Kafka timestamp.
- `main` hands the streamlet to `FlinkStreamlets.run`. In an image it builds and runs the job; given
  `--descriptor <file>` it writes the streamlet's descriptor instead, and `--descriptor-check <file>`
  fails when the file differs from the declaration.

## State and a new image

Flink keeps the state of every keyed operator and window in checkpoints. When a streamlet's pods
fail, or a new image of it is deployed, the job resumes from the last checkpoint and reads its
inlets again from the offsets that checkpoint recorded.

A new image can read the state of the old one when:

- every stateful operator keeps its `uid` (the counter's window is `uid("count")`);
- a state type that is a POJO (a public class with a public no-argument constructor and public
  fields, or getters and setters) only gains or loses fields; a field is never retyped or renamed;
- a state type that is not a POJO or an Avro type is not changed at all, since Flink stores it with
  Kryo.

An image that cannot read the state leaves the job failed and the pipeline's status with the
condition `StateUnreadable`. The state store is left as it was: deploy the previous image again, or
[reset](../deploy/reset.md) the streamlet to start without state.

## Test it

`FlinkHarness` runs the job on a local Flink with the inputs given for each inlet, and returns what
each outlet wrote once the inputs are read and every window has closed.

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

## Package the image

The image is Flink's own, at the Flink version the streamlet was built against, with the
streamlet's jar at `/opt/flink/usrlib/streamlet.jar` and each dependency Flink does not provide
beside it in `usrlib/`. See [Images](images.md).
