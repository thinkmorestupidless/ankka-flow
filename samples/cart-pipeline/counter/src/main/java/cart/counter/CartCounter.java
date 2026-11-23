package cart.counter;

// docs:start counter
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

  // docs:start ports
  final Inlet<CleanCart> in = inlet("in", Contract.of("clean-carts.v1"), Codec.json(CleanCart.class));
  final Outlet<CartCount> out = outlet("out", Contract.of("cart-counts.v1"), Codec.json(CartCount.class));
  // docs:end ports
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
// docs:end counter
