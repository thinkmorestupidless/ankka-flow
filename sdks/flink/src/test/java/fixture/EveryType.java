package fixture;

import com.thinkmorestupidless.ankka.flow.flink.Codec;
import com.thinkmorestupidless.ankka.flow.flink.Contract;
import com.thinkmorestupidless.ankka.flow.flink.FlinkContext;
import com.thinkmorestupidless.ankka.flow.flink.FlinkStreamlet;

/** The `every-type` descriptor fixture, declared in the Flink SDK. */
public final class EveryType extends FlinkStreamlet {
  public EveryType() {
    super("every-type", "Every parameter type, and \"unicode\": café ✓");
    inlet("in", Contract.of("every.v1"), Codec.bytes());
    stringParameter("a-string", "hello", "A string.");
    intParameter("an-integer", 42, "An integer.");
    doubleParameter("a-double", 0.5, "A double.");
    booleanParameter("a-boolean", true, "A boolean.");
    durationParameter("a-duration", "100 ms", "A duration.");
    memorySizeParameter("a-memory-size", "1 MiB", "A memory size.");
    stringParameter("required", null, "Required: no default, so it must be set at deploy time.");
  }

  @Override
  public void build(FlinkContext ctx) {}
}
