package com.thinkmorestupidless.ankka.flow.flink.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.thinkmorestupidless.ankka.flow.flink.Codec;
import com.thinkmorestupidless.ankka.flow.flink.Contract;
import com.thinkmorestupidless.ankka.flow.flink.FlinkContext;
import com.thinkmorestupidless.ankka.flow.flink.FlinkStreamlet;
import com.thinkmorestupidless.ankka.flow.flink.Inlet;
import com.thinkmorestupidless.ankka.flow.flink.Outlet;
import com.thinkmorestupidless.ankka.flow.flink.Parameter;
import com.thinkmorestupidless.ankka.flow.flink.Record;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

/** The harness runs a streamlet's own job over bounded inputs, with no broker and no cluster. */
class FlinkHarnessTest {

  /** Counts each key's records in tumbling windows of the time each record carries. */
  static final class Counter extends FlinkStreamlet {
    final Inlet<String> in = inlet("in", Contract.of("words.v1"), Codec.string());
    final Outlet<String> out = outlet("out", Contract.of("counts.v1"), Codec.string());
    final Parameter<Duration> window = durationParameter("window", "1m");

    Counter() {
      super("counter");
    }

    @Override
    public void build(FlinkContext ctx) {
      Duration size = ctx.get(window);
      var records = ctx.read(in, WatermarkStrategy.<Record<String>>forBoundedOutOfOrderness(Duration.ofMinutes(5))
          .withTimestampAssigner((r, t) -> r.timestamp()));
      var counts = records
          .keyBy(Record::key)
          .window(TumblingEventTimeWindows.of(size))
          .process(new ProcessWindowFunction<Record<String>, Record<String>, String, TimeWindow>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void process(String key, Context c, Iterable<Record<String>> rs, Collector<Record<String>> o) {
              long n = 0;
              for (Record<String> ignored : rs) n++;
              o.collect(Record.of(key, c.window().getStart() + ":" + n, c.window().maxTimestamp()));
            }
          })
          .returns(TypeInformation.of(new TypeHint<Record<String>>() {}));
      ctx.write(out, counts);
    }
  }

  private static final long T0 = 1_767_225_600_000L;

  private static Record<String> at(String key, long minutes, int seconds) {
    return Record.of(key, "x", T0 + minutes * 60_000 + seconds * 1000L);
  }

  private static List<String> sorted(List<Record<String>> rs) {
    return rs.stream().sorted(Comparator.comparing(Record<String>::key).thenComparing(Record::value))
        .map(r -> r.key() + "@" + r.value()).toList();
  }

  @Test
  void groupsRecordsIntoWindowsByTheTimeEachRecordCarries() {
    Counter c = new Counter();
    // given out of order: the window is the record's time, not its place in the input
    var result = FlinkHarness.of(c)
        .input(c.in, List.of(at("a", 1, 10), at("b", 0, 5), at("a", 0, 30), at("a", 0, 50), at("b", 1, 59),
            at("a", 1, 0)))
        .run();
    assertEquals(
        List.of("a@" + T0 + ":2", "a@" + (T0 + 60_000) + ":2", "b@" + T0 + ":1", "b@" + (T0 + 60_000) + ":1"),
        sorted(result.records(c.out)));
  }

  @Test
  void aParameterIsApplied() {
    Counter c = new Counter();
    var result = FlinkHarness.of(c).parameter("window", "2m")
        .input(c.in, List.of(at("a", 0, 1), at("a", 1, 1), at("a", 2, 1)))
        .run();
    assertEquals(List.of("a@" + T0 + ":2", "a@" + (T0 + 120_000) + ":1"), sorted(result.records(c.out)));
  }

  @Test
  void anInletGivenNoInputIsEmpty() {
    Counter c = new Counter();
    assertEquals(List.of(), FlinkHarness.of(c).run().records(c.out));
  }

  @Test
  void aParameterTheStreamletDoesNotDeclareIsRefused() {
    assertThrows(IllegalArgumentException.class, () -> FlinkHarness.of(new Counter()).parameter("size", "1"));
    assertThrows(IllegalArgumentException.class,
        () -> FlinkHarness.of(new Counter()).parameter("window", "soon").run());
  }

  static final class Thrower extends FlinkStreamlet {
    final Inlet<String> in = inlet("in", Contract.of("words.v1"), Codec.string());
    final Outlet<String> out = outlet("out", Contract.of("words.v1"), Codec.string());

    Thrower() {
      super("thrower");
    }

    @Override
    public void build(FlinkContext ctx) {
      ctx.write(out, ctx.read(in).map(r -> {
        if (r.value().equals("bad")) throw new IllegalStateException("cannot take " + r.value());
        return r;
      }).returns(TypeInformation.of(new TypeHint<Record<String>>() {})));
    }
  }

  @Test
  void aStreamletThatThrowsFailsTheRunWithItsCause() {
    Thrower t = new Thrower();
    FlinkHarness.Failed e = assertThrows(FlinkHarness.Failed.class,
        () -> FlinkHarness.of(t).input(t.in, List.of(Record.of("k", "ok", 1L), Record.of("k", "bad", 2L))).run());
    assertTrue(e.getCause() instanceof IllegalStateException, String.valueOf(e.getCause()));
    assertEquals("cannot take bad", e.getCause().getMessage());
  }

  @Test
  void headersAndKeysSurviveTheJob() {
    Thrower t = new Thrower();
    var h = new com.thinkmorestupidless.ankka.flow.flink.Header("trace", new byte[] {1, 2});
    var r = new Record<>(null, "ok", List.of(h), 5L);
    assertEquals(List.of(r), FlinkHarness.of(t).input(t.in, List.of(r)).run().records(t.out));
  }
}
