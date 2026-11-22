package cart.counter;

import com.thinkmorestupidless.ankka.flow.flink.Codec;
import com.thinkmorestupidless.ankka.flow.flink.Contract;
import com.thinkmorestupidless.ankka.flow.flink.FlinkContext;
import com.thinkmorestupidless.ankka.flow.flink.FlinkStreamlet;

/** The `cart-counter` descriptor fixture's declaration; the sample's streamlet has its logic. */
public final class CartCounter extends FlinkStreamlet {
  public CartCounter() {
    super("cart-counter", "Counts each cart's events in windows of the time they carry.");
    inlet("in", Contract.of("clean-carts.v1"), Codec.bytes());
    outlet("out", Contract.of("cart-counts.v1"), Codec.bytes());
    durationParameter("window", "1m", "The length of each counting window.");
  }

  @Override
  public void build(FlinkContext ctx) {}
}
