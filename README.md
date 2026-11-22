<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/brand/ankka-flow-lockup-white.png">
  <img src="docs/assets/brand/ankka-flow-lockup-black.png" alt="ankka-flow" width="400">
</picture>

[![ci](https://github.com/thinkmorestupidless/ankka-flow/actions/workflows/ci.yml/badge.svg?event=pull_request)](https://github.com/thinkmorestupidless/ankka-flow/actions/workflows/ci.yml) [![license](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

[![java 21+](https://img.shields.io/badge/java-21%2B-007396?logo=openjdk&logoColor=white)](docs/get-started/install.md) [![maven central](https://img.shields.io/maven-central/v/com.thinkmorestupidless/ankka-flow-pekko-streams_3?label=maven%20central&logo=apachemaven&logoColor=white)](https://central.sonatype.com/artifact/com.thinkmorestupidless/ankka-flow-pekko-streams_3)

Streaming pipelines beside [ankka](https://github.com/thinkmorestupidless/ankka). A pipeline is a
graph of **streamlets**, each with typed inlets and outlets, wired by a **blueprint** over Kafka
topics. A streamlet is written against one engine's own API, **Pekko Streams** for a stage that
cleans, reshapes or routes each record, **Apache Flink** for one that keeps windows or state, and the
platform runs it on that engine, creates its topics, checks every contract before anything runs, and
reports each stage's state and lag. A blueprint reads and writes an ankka project's topics by name.

![ankka-flow on Kubernetes: a developer or CI job writes an AnkkaFlow resource with flow generate and applies it. The operator, in namespace ankka-flow, watches AnkkaFlow resources in every namespace, reads the kafka-cluster Secrets, creates the pipeline's managed topics in Kafka, and runs each streamlet by its engine: a Pekko Streams streamlet as a Deployment, a Flink streamlet as a FlinkDeployment the Flink Kubernetes Operator runs, its checkpoints in a state store. Each streamlet reads and writes Kafka itself, with the credentials of its own topics only.](docs/assets/diagrams/platform.svg)

It descends from [Cloudflow](https://github.com/lightbend/cloudflow) by way of the
[thinkmorestupidless fork](https://github.com/thinkmorestupidless/cloudflow), which moved it to
Apache Pekko. Neither is a dependency: six pieces were carried over with their tests (see `NOTICE`).

## A pipeline of two engines

The sample in [`samples/cart-pipeline`](samples/cart-pipeline) cleans a topic of cart events with a
Pekko Streams streamlet and counts each cart's events per minute with a Flink one. The cleaner drops
what is not a cart event and passes the rest on:

<!-- include: samples/cart-pipeline/cleaner/src/main/scala/cart/cleaner/Cleaner.scala#cleaner -->
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

The counter keeps a window per cart, by the time each event carries:

<!-- include: samples/cart-pipeline/counter/src/main/java/cart/counter/CartCounter.java#counter -->
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

The blueprint wires them over one topic:

<!-- include: samples/cart-pipeline/blueprint.conf -->
```hocon
# The sample pipeline: the shop's cart events, cleaned, then counted per cart in windows of time.
blueprint {
  name = carts
  streamlets {
    cleaner = cart-cleaner
    counter = cart-counter
  }
  topics {
    cart-events {
      managed    = false
      topic.name = "shop.cart-events.v1"
      cluster    = shop
      consumers  = [cleaner.in]
      consumer-config { auto.offset.reset = earliest }
    }
    clean-carts {
      producers  = [cleaner.out]
      consumers  = [counter.in]
      partitions = 3
    }
    cart-counts {
      producers  = [counter.out]
      partitions = 3
    }
  }
}
```

`flow verify` checks the blueprint against the streamlets' descriptors: every inlet connected, every
topic's contract agreed by the ports on it, whatever their engines.
[Your first streamlet](https://flow.ankka.cloud/get-started/first-streamlet/) runs this pipeline on a
laptop and [Write a blueprint](https://flow.ankka.cloud/build/blueprints/) builds the blueprint line by
line.

## On a laptop

```bash
docker compose up -d kafka
sbt cli/stage
cli/target/universal/stage/bin/flow local samples/cart-pipeline/blueprint.conf \
  --descriptors samples/cart-pipeline/flow --bootstrap-servers localhost:9092 -o /tmp/carts
FLOW_CONFIG_DIR=/tmp/carts/cleaner sbt cartCleaner/run     # terminal 1
FLOW_CONFIG_DIR=/tmp/carts/counter sbt cartCounter/run     # terminal 2
samples/cart-pipeline/produce.sh localhost:9092 50          # 50 cart events, 5 of them malformed
samples/cart-pipeline/verify.sh localhost:9092              # the counts per cart per minute
```

## On Kubernetes

```bash
just up                                              # kind, both operators, MinIO, a development Kafka
brew install thinkmorestupidless/tap/ankka-flow      # the flow CLI; or the release archive for your platform
flow generate samples/cart-pipeline/blueprint.conf --descriptors samples/cart-pipeline/flow \
  --image cleaner=sample-cart-cleaner:latest --image counter=sample-cart-counter:latest \
  -n shop | kubectl apply -f -
```

## Read next

The documentation is at **[flow.ankka.cloud](https://flow.ankka.cloud/)**, built from [`docs/`](docs/)
and published as a site, `llms.txt` and agent skills for the ankka marketplace. Start with
[Pipelines and streamlets](https://flow.ankka.cloud/concepts/pipelines/),
[Choosing an engine](https://flow.ankka.cloud/concepts/engines/) and
[your first streamlet](https://flow.ankka.cloud/get-started/first-streamlet/).

The specification, plan and research behind the engines are in
[`specs/009-engine-streamlets/`](specs/009-engine-streamlets/spec.md). `just docs` builds the
documentation into `target/docs-site`; `just docs-serve` serves it with live reload.
