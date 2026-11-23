package com.thinkmorestupidless.ankka.flow.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** `streamlet.properties` as the Flink SDK reads it (contracts/streamlet-settings.md). */
class SettingsTest {

  static final String EXAMPLE = String.join("\n",
      "pipeline=carts",
      "streamlet=counter",
      "namespace=shop",
      "local=false",
      "",
      "parameter.window=1m",
      "",
      "inlet.in.topic=carts.clean-carts",
      "inlet.in.group=carts.counter.in",
      "inlet.in.client-id=carts.counter.in",
      "inlet.in.kafka.bootstrap.servers=kafka.kafka.svc:9092",
      "inlet.in.kafka.isolation.level=read_committed",
      "",
      "outlet.out.topic=carts.cart-counts",
      "outlet.out.client-id=carts.counter.out",
      "outlet.out.delivery=exactly-once",
      "outlet.out.transactional-id-prefix=flow.shop.carts.counter.out.e1",
      "outlet.out.kafka.bootstrap.servers=kafka.kafka.svc:9092",
      "outlet.out.kafka.transaction.timeout.ms=900000",
      "");

  @Test
  void theContractsExampleParses() {
    Settings s = Settings.parse(EXAMPLE);
    assertEquals("carts", s.pipeline());
    assertEquals("counter", s.streamlet());
    assertEquals("shop", s.namespace());
    assertEquals(false, s.local());
    assertEquals(Map.of("window", "1m"), s.parameters());
    Settings.InletSettings in = s.inlet("in").orElseThrow();
    assertEquals("carts.clean-carts", in.topic());
    assertEquals("carts.counter.in", in.group());
    assertEquals("carts.counter.in", in.clientId());
    assertEquals(
        Map.of("bootstrap.servers", "kafka.kafka.svc:9092", "isolation.level", "read_committed"),
        in.kafka());
    Settings.OutletSettings out = s.outlet("out").orElseThrow();
    assertEquals("carts.cart-counts", out.topic());
    assertEquals(Settings.Delivery.EXACTLY_ONCE, out.delivery());
    assertEquals("flow.shop.carts.counter.out.e1", out.transactionalIdPrefix().orElseThrow());
    assertEquals("900000", out.kafka().get("transaction.timeout.ms"));
  }

  @Test
  void aMissingKeyIsNamed() {
    for (String key : new String[] {"pipeline", "local", "inlet.in.group", "outlet.out.delivery",
        "outlet.out.transactional-id-prefix"}) {
      String text = EXAMPLE.lines().filter(l -> !l.startsWith(key + "=")).reduce("", (a, b) -> a + b + "\n");
      IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> Settings.parse(text));
      assertEquals(Settings.missing(key), e.getMessage(), key);
    }
  }

  @Test
  void anUnknownKeyIsRefused() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> Settings.parse(EXAMPLE + "inlet.in.batch=1\n"));
    assertTrue(e.getMessage().contains("inlet.in.batch"), e.getMessage());
  }

  @Test
  void theFileIsReadFromItsDirectory() throws Exception {
    Path dir = Files.createTempDirectory("settings");
    Files.writeString(dir.resolve("streamlet.properties"), EXAMPLE, StandardCharsets.ISO_8859_1);
    assertEquals("counter", Settings.read(dir).streamlet());
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> Settings.read(Files.createTempDirectory("empty")));
    assertTrue(e.getMessage().contains("streamlet.properties"), e.getMessage());
  }
}
