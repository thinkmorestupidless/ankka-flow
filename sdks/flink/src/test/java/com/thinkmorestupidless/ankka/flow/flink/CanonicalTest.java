package com.thinkmorestupidless.ankka.flow.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/** Every contract fixture's schema fingerprints to the value ankka states for it. */
class CanonicalTest {

  static Path root() {
    return Path.of(System.getProperty("flow.repo.root", ".")).toAbsolutePath().normalize();
  }

  @Test
  void everyFingerprintIsAnkkas() throws Exception {
    JsonNode fixtures =
        new ObjectMapper().readTree(root().resolve("descriptor/fixtures/contracts/fingerprints.json").toFile());
    int checked = 0;
    for (JsonNode f : fixtures) {
      String schema = f.get("schema").toString();
      assertEquals(
          f.get("fingerprint").asText(),
          Canonical.fingerprint(schema.getBytes(StandardCharsets.UTF_8)),
          f.get("name").asText());
      checked++;
    }
    assertEquals(6, checked);
  }

  @Test
  void numbersAreWrittenAsECMAScriptPrintsThem() {
    assertEquals("1e+21", Canonical.number(1e21));
    assertEquals("123456789012345680000", Canonical.number(123456789012345680000d));
    assertEquals("1e-7", Canonical.number(1e-7));
    assertEquals("0.002", Canonical.number(0.002));
    assertEquals("-1.5", Canonical.number(-1.5));
    assertEquals("0", Canonical.number(-0.0));
  }

  @Test
  void theSampleSchemaFingerprintIsTheDescriptorModules() throws Exception {
    byte[] schema = Files.readAllBytes(root().resolve("samples/cart-pipeline/schemas/cart-events.v1.json"));
    String fixture = Files.readString(root().resolve("descriptor/fixtures/descriptors/cart-cleaner.json"));
    assertEquals(true, fixture.contains(Canonical.fingerprint(schema)));
  }
}
