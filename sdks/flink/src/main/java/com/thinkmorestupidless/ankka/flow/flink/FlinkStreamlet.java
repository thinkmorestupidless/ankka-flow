package com.thinkmorestupidless.ankka.flow.flink;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * A streamlet on Flink: a name, its inlets, outlets and parameters declared as fields, and the
 * job {@link #build} wires from them. The main names it: {@code FlinkStreamlets.run(new
 * CartCounter(), args)}.
 */
public abstract class FlinkStreamlet {

  /** The Flink version this SDK is built for, as a descriptor names it. */
  public static final String FLINK_VERSION = "2.2";

  private final String name;
  private final String description;
  private final List<Inlet<?>> inlets = new ArrayList<>();
  private final List<Outlet<?>> outlets = new ArrayList<>();
  private final List<Parameter<?>> parameters = new ArrayList<>();

  protected FlinkStreamlet(String name) {
    this(name, "");
  }

  protected FlinkStreamlet(String name, String description) {
    this.name = name;
    this.description = description == null ? "" : description;
  }

  public final String name() {
    return name;
  }

  public final String description() {
    return description;
  }

  public final List<Inlet<?>> inlets() {
    return List.copyOf(inlets);
  }

  public final List<Outlet<?>> outlets() {
    return List.copyOf(outlets);
  }

  public final List<Parameter<?>> parameters() {
    return List.copyOf(parameters);
  }

  /** The class Flink runs: this streamlet's. */
  public String entryClass() {
    return getClass().getName();
  }

  protected final <A> Inlet<A> inlet(String name, Contract contract, Codec<A> codec) {
    Inlet<A> i = new Inlet<>(name, contract, codec);
    inlets.add(i);
    return i;
  }

  protected final <A> Outlet<A> outlet(String name, Contract contract, Codec<A> codec) {
    Outlet<A> o = new Outlet<>(name, contract, codec);
    outlets.add(o);
    return o;
  }

  private <A> Parameter<A> declare(Parameter<A> p) {
    parameters.add(p);
    return p;
  }

  private static String text(Object value) {
    return value == null ? null : value.toString();
  }

  /** A string; {@code null} as the default makes it required. */
  protected final Parameter<String> stringParameter(String key, String defaultValue) {
    return stringParameter(key, defaultValue, "");
  }

  protected final Parameter<String> stringParameter(String key, String defaultValue, String description) {
    return declare(new Parameter<>(key, Parameter.Type.STRING, defaultValue, description, t -> t));
  }

  protected final Parameter<Integer> intParameter(String key, Integer defaultValue) {
    return intParameter(key, defaultValue, "");
  }

  protected final Parameter<Integer> intParameter(String key, Integer defaultValue, String description) {
    return declare(
        new Parameter<>(key, Parameter.Type.INTEGER, text(defaultValue), description, Parameter::integer));
  }

  protected final Parameter<Double> doubleParameter(String key, Double defaultValue) {
    return doubleParameter(key, defaultValue, "");
  }

  protected final Parameter<Double> doubleParameter(String key, Double defaultValue, String description) {
    return declare(
        new Parameter<>(key, Parameter.Type.DOUBLE, text(defaultValue), description, Parameter::decimal));
  }

  protected final Parameter<Boolean> booleanParameter(String key, Boolean defaultValue) {
    return booleanParameter(key, defaultValue, "");
  }

  protected final Parameter<Boolean> booleanParameter(String key, Boolean defaultValue, String description) {
    return declare(
        new Parameter<>(key, Parameter.Type.BOOLEAN, text(defaultValue), description, Parameter::bool));
  }

  /** A duration, its default as text: {@code 100 ms}, {@code 1m}, {@code 30s}. */
  protected final Parameter<Duration> durationParameter(String key, String defaultValue) {
    return durationParameter(key, defaultValue, "");
  }

  protected final Parameter<Duration> durationParameter(String key, String defaultValue, String description) {
    return declare(
        new Parameter<>(key, Parameter.Type.DURATION, defaultValue, description, Parameter::duration));
  }

  /** A size in bytes, its default as text: {@code 1 MiB}, {@code 512k}. */
  protected final Parameter<Long> memorySizeParameter(String key, String defaultValue) {
    return memorySizeParameter(key, defaultValue, "");
  }

  protected final Parameter<Long> memorySizeParameter(String key, String defaultValue, String description) {
    return declare(
        new Parameter<>(key, Parameter.Type.MEMORY_SIZE, defaultValue, description, Parameter::memorySize));
  }

  /** Wires the job: read the inlets, transform, write the outlets. */
  public abstract void build(FlinkContext ctx);
}
