package com.thinkmorestupidless.ankka.flow.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.junit.jupiter.api.Test;

/** The main's three modes: write the descriptor, compare it, and build the job. */
class FlinkStreamletsTest {

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();

  private Optional<Integer> mode(String... args) {
    return FlinkStreamlets.descriptorMode(new cart.counter.CartCounter(), List.of(args),
        new PrintStream(out, true, StandardCharsets.UTF_8), new PrintStream(err, true, StandardCharsets.UTF_8));
  }

  @Test
  void descriptorWritesAndDescriptorCheckCompares() throws Exception {
    Path file = Files.createTempDirectory("d").resolve("flow/counter.json");
    assertEquals(Optional.of(1), mode("--descriptor-check", file.toString()), "missing");
    assertEquals(Optional.of(0), mode("--descriptor", file.toString()));
    assertEquals(DescriptorWriter.write(new cart.counter.CartCounter()), Files.readString(file));
    assertEquals(Optional.of(0), mode("--descriptor-check", file.toString()));
    Files.writeString(file, "{}\n");
    assertEquals(Optional.of(1), mode("--descriptor-check", file.toString()));
    assertTrue(err.toString().contains("write it with --descriptor"), err.toString());
  }

  @Test
  void noArgumentsRunsAndAnythingElseIsUsage() {
    assertEquals(Optional.empty(), mode());
    assertEquals(Optional.of(2), mode("--descriptor"));
    assertEquals(Optional.of(2), mode("--run"));
  }

  @Test
  void localSettingsBuildTheJobOnTheLocalEnvironment() {
    Settings local = Settings.parse(SettingsTest.EXAMPLE.replace("local=false", "local=true"));
    var env = FlinkStreamlets.environment(new Passthrough(), local);
    assertTrue(env instanceof LocalStreamEnvironment, env.getClass().getName());
    assertEquals(10_000L, env.getCheckpointConfig().getCheckpointInterval(), "offsets are committed");
    String plan = env.getExecutionPlan();
    assertTrue(plan.contains("inlet-in") && plan.contains("outlet-out"), plan);
  }

  @Test
  void settingsComeFromTheDirectoryTheEnvironmentNames() throws Exception {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> FlinkStreamlets.settings(Map.of()));
    assertTrue(e.getMessage().contains("FLOW_CONFIG_DIR"), e.getMessage());
    Path dir = Files.createTempDirectory("cfg");
    Files.writeString(dir.resolve("streamlet.properties"), SettingsTest.EXAMPLE);
    assertEquals("counter", FlinkStreamlets.settings(Map.of("FLOW_CONFIG_DIR", dir.toString())).streamlet());
  }

  static final class Passthrough extends FlinkStreamlet {
    final Inlet<byte[]> in = inlet("in", Contract.of("clean-carts.v1"), Codec.bytes());
    final Outlet<byte[]> out = outlet("out", Contract.of("cart-counts.v1"), Codec.bytes());
    final Parameter<java.time.Duration> window = durationParameter("window", "1m");

    Passthrough() {
      super("passthrough");
    }

    @Override
    public void build(FlinkContext ctx) {
      ctx.get(window);
      ctx.write(out, ctx.read(in));
    }
  }
}
