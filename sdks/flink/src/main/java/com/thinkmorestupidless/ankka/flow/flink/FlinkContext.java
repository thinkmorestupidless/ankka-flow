package com.thinkmorestupidless.ankka.flow.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * What a Flink streamlet's {@code build} is given: its environment, a stream per inlet, a sink per
 * outlet, and its parameters' values. The topics, groups and credentials are the SDK's.
 */
public interface FlinkContext {
  StreamExecutionEnvironment env();

  /** The inlet's records with their Kafka timestamps, and no watermarks. */
  <A> DataStream<Record<A>> read(Inlet<A> inlet);

  /** The inlet's records with the watermarks the strategy gives. */
  <A> DataStream<Record<A>> read(Inlet<A> inlet, WatermarkStrategy<Record<A>> watermarks);

  <A> void write(Outlet<A> outlet, DataStream<Record<A>> stream);

  /** The parameter's value, typed as declared. */
  <A> A get(Parameter<A> parameter);
}
