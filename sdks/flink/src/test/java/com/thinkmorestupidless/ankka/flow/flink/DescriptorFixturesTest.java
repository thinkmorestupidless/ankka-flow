package com.thinkmorestupidless.ankka.flow.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/** The two `flink` descriptor fixtures, declared in the Java API, write exactly their bytes. */
class DescriptorFixturesTest {

  private static final Path FIXTURES = CanonicalTest.root().resolve("descriptor/fixtures/descriptors");
  private static final Map<String, Supplier<FlinkStreamlet>> DECLARED =
      Map.of("every-type", fixture.EveryType::new, "cart-counter", cart.counter.CartCounter::new);

  private static String fixture(String name) throws Exception {
    return Files.readString(FIXTURES.resolve(name + ".json"));
  }

  @Test
  void everyFlinkFixtureIsWrittenByteForByte() throws Exception {
    for (var e : DECLARED.entrySet()) {
      assertEquals(
          fixture(e.getKey()),
          DescriptorWriter.write(e.getValue().get(), "fixture", "0.0.0"),
          e.getKey());
    }
  }

  @Test
  void everyFlinkFixtureIsCoveredAndOnlyThose() throws Exception {
    Set<String> flink = new TreeSet<>();
    try (Stream<Path> files = Files.list(FIXTURES)) {
      for (Path f : (Iterable<Path>) files::iterator) {
        if (Files.readString(f).contains("\"engine\": \"flink\"")) {
          flink.add(f.getFileName().toString().replace(".json", ""));
        }
      }
    }
    assertEquals(new TreeSet<>(DECLARED.keySet()), flink);
  }

  @Test
  void theSdkWritesItsOwnNameAndTheEntryClass() {
    String d = DescriptorWriter.write(new cart.counter.CartCounter());
    assertTrue(d.contains("\"name\": \"ankka-flow-flink\""), d);
    assertTrue(d.contains("\"entry_class\": \"cart.counter.CartCounter\""), d);
    assertTrue(d.contains("\"version\": \"" + FlinkStreamlet.FLINK_VERSION + "\""), d);
  }

  private static void refused(String problem, FlinkStreamlet s) {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> DescriptorWriter.write(s));
    assertTrue(e.getMessage().contains(problem), e.getMessage());
  }

  static class Bad extends FlinkStreamlet {
    Bad(String name) {
      super(name);
    }

    Bad() {
      this("bad");
    }

    @Override
    public void build(FlinkContext ctx) {}
  }

  @Test
  void aDeclarationTheDescriptorRefusesIsRefusedWhereItIsWritten() {
    refused("port 'in' is declared 2 times", new Bad() {
      {
        inlet("in", Contract.of("x.v1"), Codec.bytes());
        outlet("in", Contract.of("x.v1"), Codec.bytes());
      }
    });
    refused("outlet name 'Out' must match", new Bad() {
      {
        outlet("Out", Contract.of("x.v1"), Codec.bytes());
      }
    });
    refused("contract name 'X v1' must match", new Bad() {
      {
        inlet("in", Contract.of("X v1"), Codec.bytes());
      }
    });
    refused("nowhere.json", new Bad() {
      {
        inlet("in", Contract.of("x.v1", "nowhere.json"), Codec.bytes());
      }
    });
    refused("streamlet name '-bad'", new Bad("-bad"));
    refused("parameter 'n' is declared 2 times", new Bad() {
      {
        stringParameter("n", null);
        stringParameter("n", null);
      }
    });
  }

  @Test
  void aDefaultOfTheWrongFormIsRefusedWhereItIsDeclared() {
    assertThrows(IllegalArgumentException.class, () -> new Bad() {
      {
        durationParameter("d", "soon");
      }
    });
    assertThrows(IllegalArgumentException.class, () -> new Bad() {
      {
        memorySizeParameter("m", "lots");
      }
    });
  }

  @Test
  void durationsAndSizesParseAsTheDescriptorReadsThem() {
    assertEquals(Duration.ofMillis(100), Parameter.duration("100 ms"));
    assertEquals(Duration.ofMinutes(1), Parameter.duration("1m"));
    assertEquals(Duration.ofSeconds(30), Parameter.duration("30 seconds"));
    assertEquals(Duration.ofMillis(250), Parameter.duration("250"));
    assertEquals(1024L * 1024, Parameter.memorySize("1 MiB"));
    assertEquals(512L * 1024, Parameter.memorySize("512k"));
    assertEquals(2_000_000L, Parameter.memorySize("2MB"));
    assertEquals(10L, Parameter.memorySize("10"));
  }
}
