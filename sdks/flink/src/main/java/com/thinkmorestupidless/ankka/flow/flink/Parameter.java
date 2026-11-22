package com.thinkmorestupidless.ankka.flow.flink;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A declared setting of a streamlet, read in the job with {@code ctx.get(parameter)}. Its default
 * is kept as the descriptor carries it, as text: {@code 100}, {@code 0.5}, {@code true}, {@code
 * 100 ms}, {@code 1 MiB}.
 */
public final class Parameter<A> implements Serializable {
  private static final long serialVersionUID = 1L;

  /** The descriptor's parameter types, as it writes them. */
  public enum Type {
    STRING,
    INTEGER,
    DOUBLE,
    BOOLEAN,
    DURATION,
    MEMORY_SIZE;

    String typeName() {
      return name().toLowerCase(Locale.ROOT).replace('_', ' ');
    }
  }

  /** A value's parser; serializable so a job can carry its parameters. */
  @FunctionalInterface
  interface Parser<A> extends Serializable {
    A parse(String text);
  }

  private final String key;
  private final Type type;
  private final String defaultValue;
  private final String description;
  private final Parser<A> parser;

  Parameter(String key, Type type, String defaultValue, String description, Parser<A> parser) {
    this.key = key;
    this.type = type;
    this.defaultValue = defaultValue;
    this.description = description == null ? "" : description;
    this.parser = parser;
    if (defaultValue != null) {
      try {
        parser.parse(defaultValue);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("parameter '" + key + "': " + e.getMessage(), e);
      }
    }
  }

  public String key() {
    return key;
  }

  public Type type() {
    return type;
  }

  public Optional<String> defaultValue() {
    return Optional.ofNullable(defaultValue);
  }

  public String description() {
    return description;
  }

  /** The value of this parameter in {@code text}, failing naming the key when it is not one. */
  public A parse(String text) {
    try {
      return parser.parse(text);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("parameter '" + key + "': " + e.getMessage(), e);
    }
  }

  @Override
  public String toString() {
    return "Parameter(" + key + ", " + type.typeName() + ")";
  }

  private static IllegalArgumentException notA(String kind, String text) {
    return new IllegalArgumentException("'" + text + "' is not a " + kind);
  }

  static Integer integer(String text) {
    try {
      return Integer.valueOf(text.trim());
    } catch (NumberFormatException e) {
      throw notA("integer", text);
    }
  }

  static Double decimal(String text) {
    try {
      return Double.valueOf(text.trim());
    } catch (NumberFormatException e) {
      throw notA("double", text);
    }
  }

  static Boolean bool(String text) {
    return switch (text.trim()) {
      case "true" -> true;
      case "false" -> false;
      default -> throw notA("boolean", text);
    };
  }

  private static final Pattern QUANTITY = Pattern.compile("([+-]?[0-9]+(?:\\.[0-9]+)?(?:[eE][+-]?[0-9]+)?)\\s*([a-zA-Z]*)");

  private static final Map<String, Long> NANOS = Map.ofEntries(
      Map.entry("ns", 1L), Map.entry("nano", 1L), Map.entry("nanos", 1L),
      Map.entry("nanosecond", 1L), Map.entry("nanoseconds", 1L),
      Map.entry("us", 1_000L), Map.entry("micro", 1_000L), Map.entry("micros", 1_000L),
      Map.entry("microsecond", 1_000L), Map.entry("microseconds", 1_000L),
      Map.entry("", 1_000_000L), Map.entry("ms", 1_000_000L), Map.entry("milli", 1_000_000L),
      Map.entry("millis", 1_000_000L), Map.entry("millisecond", 1_000_000L),
      Map.entry("milliseconds", 1_000_000L),
      Map.entry("s", 1_000_000_000L), Map.entry("second", 1_000_000_000L),
      Map.entry("seconds", 1_000_000_000L),
      Map.entry("m", 60_000_000_000L), Map.entry("minute", 60_000_000_000L),
      Map.entry("minutes", 60_000_000_000L),
      Map.entry("h", 3_600_000_000_000L), Map.entry("hour", 3_600_000_000_000L),
      Map.entry("hours", 3_600_000_000_000L),
      Map.entry("d", 86_400_000_000_000L), Map.entry("day", 86_400_000_000_000L),
      Map.entry("days", 86_400_000_000_000L));

  /** A duration as HOCON reads one: a number and a unit, milliseconds when there is none. */
  static Duration duration(String text) {
    Matcher m = QUANTITY.matcher(text.trim());
    if (!m.matches() || !NANOS.containsKey(m.group(2))) throw notA("duration", text);
    BigDecimal nanos = new BigDecimal(m.group(1)).multiply(BigDecimal.valueOf(NANOS.get(m.group(2))));
    return Duration.ofNanos(nanos.longValue());
  }

  /** A size in bytes as HOCON reads one: K is 1024, kB 1000, bytes when there is no unit. */
  static Long memorySize(String text) {
    Matcher m = QUANTITY.matcher(text.trim());
    if (!m.matches()) throw notA("memory size", text);
    Long unit = sizeUnit(m.group(2));
    if (unit == null) throw notA("memory size", text);
    return new BigDecimal(m.group(1)).multiply(BigDecimal.valueOf(unit)).longValue();
  }

  private static Long sizeUnit(String u) {
    if (u.isEmpty() || u.equals("B") || u.equals("b") || u.equals("byte") || u.equals("bytes")) {
      return 1L;
    }
    String[] powers = {"k", "m", "g", "t", "p", "e"};
    String[] si = {"kilo", "mega", "giga", "tera", "peta", "exa"};
    String[] iec = {"kibi", "mebi", "gibi", "tebi", "pebi", "exbi"};
    for (int i = 0; i < powers.length; i++) {
      long binary = 1L << (10 * (i + 1));
      long decimal = (long) Math.pow(1000, i + 1);
      String P = powers[i].toUpperCase(Locale.ROOT);
      String p = powers[i];
      if (u.equals(P) || u.equals(p) || u.equals(P + "i") || u.equals(P + "iB")
          || u.equals(iec[i] + "byte") || u.equals(iec[i] + "bytes")) {
        return binary;
      }
      if (u.equals(P + "B") || (i == 0 && u.equals("kB")) || u.equals(si[i] + "byte")
          || u.equals(si[i] + "bytes")) {
        return decimal;
      }
    }
    return null;
  }
}
