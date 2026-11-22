package com.thinkmorestupidless.ankka.flow.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.junit.jupiter.api.Test;

/** The Kafka source and sink the SDK builds for a port, from its settings and nothing else. */
class ConnectorsTest {

  private final Settings settings = Settings.parse(SettingsTest.EXAMPLE);
  private final Settings.InletSettings in = settings.inlet("in").orElseThrow();
  private final Settings.OutletSettings out = settings.outlet("out").orElseThrow();

  @Test
  void aSourceReadsWithItsGroupFromTheCommittedOffsetsAndCommitsOnCheckpoint() {
    Properties p = Connectors.sourceProperties(in);
    assertEquals("carts.counter.in", p.getProperty("group.id"));
    assertEquals("carts.counter.in", p.getProperty("client.id.prefix"));
    assertEquals("true", p.getProperty("commit.offsets.on.checkpoint"));
    assertEquals("false", p.getProperty("enable.auto.commit"));
    assertEquals("kafka.kafka.svc:9092", p.getProperty("bootstrap.servers"));
    assertEquals("read_committed", p.getProperty("isolation.level"));
    assertEquals(Connectors.Reset.EARLIEST, Connectors.reset(in));
    assertEquals(
        Connectors.Reset.LATEST,
        Connectors.reset(new Settings.InletSettings("in", "t", "g", "c", Map.of("auto.offset.reset", "latest"))));
    var source = Connectors.source(in, Codec.string());
    assertEquals(Boundedness.CONTINUOUS_UNBOUNDED, source.getBoundedness());
  }

  @Test
  void anExactlyOnceOutletWritesInTransactionsUnderItsPrefix() {
    assertEquals(DeliveryGuarantee.EXACTLY_ONCE, Connectors.guarantee(out));
    assertEquals(Optional.of("flow.shop.carts.counter.out.e1"), Connectors.transactionalIdPrefix(out));
    Properties p = Connectors.sinkProperties(out);
    assertEquals("900000", p.getProperty("transaction.timeout.ms"));
    assertEquals("carts.counter.out", p.getProperty("client.id"));
    assertNotNull(Connectors.sink(out, Codec.string()));
  }

  @Test
  void anAtLeastOnceOutletHasNeitherGuaranteeNorPrefix() {
    var alo = new Settings.OutletSettings("out", "t", "c", Settings.Delivery.AT_LEAST_ONCE, Optional.empty(),
        Map.of("bootstrap.servers", "k:9092"));
    assertEquals(DeliveryGuarantee.AT_LEAST_ONCE, Connectors.guarantee(alo));
    assertEquals(Optional.empty(), Connectors.transactionalIdPrefix(alo));
    assertNotNull(Connectors.sink(alo, Codec.string()));
  }

  @Test
  void theTokenEndpointsTheSettingsNameAreAllowed() {
    String key = Connectors.ALLOWED_URLS;
    String before = System.getProperty(key);
    try {
      System.clearProperty(key);
      Connectors.allowTokenEndpoints(settings);
      assertEquals(null, System.getProperty(key), "no endpoint, nothing set");
      var token = new Settings.InletSettings("in", "t", "g", "c",
          Map.of("sasl.oauthbearer.token.endpoint.url", "https://auth.example/token"));
      var other = new Settings.OutletSettings("out", "t", "c", Settings.Delivery.AT_LEAST_ONCE, Optional.empty(),
          Map.of("sasl.oauthbearer.token.endpoint.url", "https://other.example/token"));
      Connectors.allowTokenEndpoints(new Settings("p", "s", "n", false, Map.of(),
          java.util.List.of(token), java.util.List.of(other)));
      assertEquals("https://auth.example/token,https://other.example/token", System.getProperty(key));
    } finally {
      if (before == null) System.clearProperty(key);
      else System.setProperty(key, before);
    }
  }

  @Test
  void aFlinkStreamletRefusesSettingsThatDoNotDeclareItsPorts() {
    FlinkStreamlet counter = new cart.counter.CartCounter();
    assertEquals(Optional.empty(), FlinkStreamlets.check(counter, settings));
    var missing = new Settings("p", "s", "n", false, Map.of("window", "1m"), java.util.List.of(), settings.outlets());
    assertEquals(Optional.of(Settings.missing("inlet.in.topic")), FlinkStreamlets.check(counter, missing));
    var noParam = new Settings("p", "s", "n", false, Map.of(), settings.inlets(), settings.outlets());
    assertEquals(Optional.of(Settings.missing("parameter.window")), FlinkStreamlets.check(counter, noParam));
    var extra = new Settings("p", "s", "n", false, Map.of("window", "1m", "z", "1"), settings.inlets(), settings.outlets());
    assertTrue(FlinkStreamlets.check(counter, extra).orElseThrow().contains("parameter 'z'"));
    assertThrows(IllegalArgumentException.class, () -> FlinkStreamlets.contextParameters(counter, Map.of("window", "soon")));
  }
}
