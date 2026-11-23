package com.thinkmorestupidless.ankka.flow.flink;

import java.util.Map;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** The context a deployed streamlet is built with: each port a Kafka source or sink. */
final class KafkaContext implements FlinkContext {
  private final StreamExecutionEnvironment env;
  private final Settings settings;
  private final Map<String, Object> parameters;

  KafkaContext(StreamExecutionEnvironment env, Settings settings, Map<String, Object> parameters) {
    this.env = env;
    this.settings = settings;
    this.parameters = parameters;
  }

  @Override
  public StreamExecutionEnvironment env() {
    return env;
  }

  @Override
  public <A> DataStream<Record<A>> read(Inlet<A> inlet) {
    return read(inlet, WatermarkStrategy.noWatermarks());
  }

  @Override
  public <A> DataStream<Record<A>> read(Inlet<A> inlet, WatermarkStrategy<Record<A>> watermarks) {
    Settings.InletSettings in = settings.inlet(inlet.name()).orElseThrow(
        () -> new IllegalArgumentException(Settings.missing("inlet." + inlet.name() + ".topic")));
    // The uid keeps the source's state (its offsets) across a changed job.
    return env.fromSource(Connectors.source(in, inlet.codec()), watermarks, "inlet-" + inlet.name(),
            Connectors.<A>recordType())
        .uid("flow-inlet-" + inlet.name());
  }

  @Override
  public <A> void write(Outlet<A> outlet, DataStream<Record<A>> stream) {
    Settings.OutletSettings out = settings.outlet(outlet.name()).orElseThrow(
        () -> new IllegalArgumentException(Settings.missing("outlet." + outlet.name() + ".topic")));
    stream.sinkTo(Connectors.sink(out, outlet.codec()))
        .name("outlet-" + outlet.name())
        .uid("flow-outlet-" + outlet.name());
  }

  @Override
  @SuppressWarnings("unchecked")
  public <A> A get(Parameter<A> parameter) {
    if (!parameters.containsKey(parameter.key())) {
      throw new IllegalArgumentException("parameter '" + parameter.key() + "' is not declared by this streamlet");
    }
    return (A) parameters.get(parameter.key());
  }
}
