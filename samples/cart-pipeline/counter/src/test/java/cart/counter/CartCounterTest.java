package cart.counter;

import static org.junit.jupiter.api.Assertions.assertEquals;

// docs:start counter-test
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
// docs:end counter-test
