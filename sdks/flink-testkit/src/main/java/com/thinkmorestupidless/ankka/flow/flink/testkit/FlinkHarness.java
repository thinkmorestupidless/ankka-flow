package com.thinkmorestupidless.ankka.flow.flink.testkit;

import com.thinkmorestupidless.ankka.flow.flink.FlinkContext;
import com.thinkmorestupidless.ankka.flow.flink.FlinkStreamlet;
import com.thinkmorestupidless.ankka.flow.flink.FlinkStreamlets;
import com.thinkmorestupidless.ankka.flow.flink.Inlet;
import com.thinkmorestupidless.ankka.flow.flink.Outlet;
import com.thinkmorestupidless.ankka.flow.flink.Parameter;
import com.thinkmorestupidless.ankka.flow.flink.Record;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

/**
 * Runs a Flink streamlet's own job on a local environment over bounded inputs, each record
 * carrying its timestamp, and returns what each outlet was given. No broker, no cluster.
 *
 * <pre>{@code
 * var result = FlinkHarness.of(counter).parameter("window", "2m").input(counter.in, records).run();
 * result.records(counter.out);
 * }</pre>
 */
public final class FlinkHarness {

  /** The job failed; the cause is what the streamlet threw. */
  public static final class Failed extends RuntimeException {
    private static final long serialVersionUID = 1L;

    Failed(String streamlet, Throwable cause) {
      super(streamlet + " failed: " + cause, cause);
    }
  }

  /** What each outlet was given, in the order it was given it. */
  public static final class Result {
    private final Map<String, List<Record<?>>> byOutlet;

    Result(Map<String, List<Record<?>>> byOutlet) {
      this.byOutlet = byOutlet;
    }

    @SuppressWarnings("unchecked")
    public <A> List<Record<A>> records(Outlet<A> outlet) {
      List<Record<?>> rs = byOutlet.get(outlet.name());
      if (rs == null) return List.of();
      List<Record<A>> out = new ArrayList<>();
      for (Record<?> r : rs) out.add((Record<A>) r);
      return out;
    }
  }

  private final FlinkStreamlet streamlet;
  private final Map<String, String> parameters = new HashMap<>();
  private final Map<String, List<? extends Record<?>>> inputs = new HashMap<>();

  private FlinkHarness(FlinkStreamlet streamlet) {
    this.streamlet = streamlet;
  }

  public static FlinkHarness of(FlinkStreamlet streamlet) {
    return new FlinkHarness(streamlet);
  }

  /** A parameter's value as text of its type, as {@code streamlet.properties} would give it. */
  public FlinkHarness parameter(String key, String value) {
    if (streamlet.parameters().stream().noneMatch(p -> p.key().equals(key))) {
      throw new IllegalArgumentException("parameter '" + key + "' is not declared by " + streamlet.name());
    }
    parameters.put(key, value);
    return this;
  }

  public <A> FlinkHarness input(Inlet<A> inlet, List<Record<A>> records) {
    if (!streamlet.inlets().contains(inlet)) {
      throw new IllegalArgumentException("inlet '" + inlet.name() + "' is not declared by " + streamlet.name());
    }
    inputs.put(inlet.name(), List.copyOf(records));
    return this;
  }

  public Result run() {
    Map<String, Object> values = FlinkStreamlets.contextParameters(streamlet, parameters);
    Configuration conf = new Configuration();
    conf.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, conf);
    Map<String, CloseableIterator<? extends Record<?>>> collected = new LinkedHashMap<>();
    streamlet.build(new BoundedContext(env, values, collected));
    Map<String, List<Record<?>>> out = new LinkedHashMap<>();
    try {
      if (!collected.isEmpty()) env.execute("harness-" + streamlet.name());
      for (var e : collected.entrySet()) {
        List<Record<?>> rs = new ArrayList<>();
        CloseableIterator<? extends Record<?>> it = e.getValue();
        try {
          it.forEachRemaining(rs::add);
        } finally {
          it.close();
        }
        out.put(e.getKey(), rs);
      }
    } catch (Exception e) {
      throw new Failed(streamlet.name(), rootCause(e));
    }
    return new Result(out);
  }

  private static Throwable rootCause(Throwable t) {
    Throwable c = t;
    while (c.getCause() != null && c.getCause() != c) c = c.getCause();
    return c;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static <A> TypeInformation<Record<A>> recordType() {
    return (TypeInformation<Record<A>>) (TypeInformation) new GenericTypeInfo<>(Record.class);
  }

  private final class BoundedContext implements FlinkContext {
    private final StreamExecutionEnvironment env;
    private final Map<String, Object> values;
    private final Map<String, CloseableIterator<? extends Record<?>>> collected;

    BoundedContext(StreamExecutionEnvironment env, Map<String, Object> values,
        Map<String, CloseableIterator<? extends Record<?>>> collected) {
      this.env = env;
      this.values = values;
      this.collected = collected;
    }

    @Override
    public StreamExecutionEnvironment env() {
      return env;
    }

    @Override
    public <A> DataStream<Record<A>> read(Inlet<A> inlet) {
      return read(inlet, WatermarkStrategy.<Record<A>>noWatermarks().withTimestampAssigner((r, t) -> r.timestamp()));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <A> DataStream<Record<A>> read(Inlet<A> inlet, WatermarkStrategy<Record<A>> watermarks) {
      List<Record<A>> records = (List<Record<A>>) inputs.getOrDefault(inlet.name(), List.of());
      // Through the inlet's codec, as the Kafka source would decode the bytes it reads.
      List<Record<A>> decoded = new ArrayList<>();
      for (Record<A> r : records) decoded.add(r.withValue(inlet.codec().decode(inlet.codec().encode(r.value()))));
      TypeInformation<Record<A>> type = FlinkHarness.<A>recordType();
      DataStream<Record<A>> source = decoded.isEmpty()
          // Flink's collection source refuses an empty collection: one element, never let through.
          ? env.fromSequence(0, 0).name("inlet-" + inlet.name()).filter(n -> false)
              .map(n -> (Record<A>) null).returns(type)
          : env.fromData(decoded, type).name("inlet-" + inlet.name());
      return source.assignTimestampsAndWatermarks(watermarks);
    }

    @Override
    public <A> void write(Outlet<A> outlet, DataStream<Record<A>> stream) {
      collected.put(outlet.name(), stream.collectAsync());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <A> A get(Parameter<A> parameter) {
      if (!values.containsKey(parameter.key())) {
        throw new IllegalArgumentException("parameter '" + parameter.key() + "' is not declared by " + streamlet.name());
      }
      return (A) values.get(parameter.key());
    }
  }
}
