package com.thinkmorestupidless.ankka.flow.flink;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * {@code streamlet.properties}, as the operator and {@code flow local} write it: the streamlet's
 * names, its parameters' values, and each port's topic and Kafka properties. Read as the
 * descriptor module's {@code Settings} reads it, with the same messages.
 */
public record Settings(
    String pipeline,
    String streamlet,
    String namespace,
    boolean local,
    Map<String, String> parameters,
    List<InletSettings> inlets,
    List<OutletSettings> outlets) {

  public static final String FILE_NAME = "streamlet.properties";
  public static final String DIRECTORY_VARIABLE = "FLOW_CONFIG_DIR";

  public Settings {
    parameters = Map.copyOf(parameters);
    inlets = List.copyOf(inlets);
    outlets = List.copyOf(outlets);
  }

  /** How an outlet writes. */
  public enum Delivery {
    AT_LEAST_ONCE("at-least-once"),
    EXACTLY_ONCE("exactly-once");

    private final String wire;

    Delivery(String wire) {
      this.wire = wire;
    }

    public String wire() {
      return wire;
    }

    static Optional<Delivery> fromWire(String text) {
      for (Delivery d : values()) if (d.wire.equals(text)) return Optional.of(d);
      return Optional.empty();
    }
  }

  public record InletSettings(String port, String topic, String group, String clientId, Map<String, String> kafka) {
    public InletSettings {
      kafka = Map.copyOf(kafka);
    }
  }

  public record OutletSettings(
      String port,
      String topic,
      String clientId,
      Delivery delivery,
      Optional<String> transactionalIdPrefix,
      Map<String, String> kafka) {
    public OutletSettings {
      kafka = Map.copyOf(kafka);
    }
  }

  public Optional<InletSettings> inlet(String port) {
    return inlets.stream().filter(i -> i.port().equals(port)).findFirst();
  }

  public Optional<OutletSettings> outlet(String port) {
    return outlets.stream().filter(o -> o.port().equals(port)).findFirst();
  }

  public static String missing(String key) {
    return FILE_NAME + " has no key " + key;
  }

  public static String unknown(String key) {
    return FILE_NAME + " has a key flow does not know: " + key;
  }

  /** The file in {@code dir}. */
  public static Settings read(Path dir) {
    Path file = dir.resolve(FILE_NAME);
    if (!Files.isRegularFile(file)) throw new IllegalArgumentException(FILE_NAME + " is not in " + dir);
    try {
      return parse(new String(Files.readAllBytes(file), StandardCharsets.ISO_8859_1));
    } catch (IOException e) {
      throw new IllegalArgumentException(file + " could not be read: " + e.getMessage(), e);
    }
  }

  /** The text of a file (ISO 8859-1 with {@code \\u} escapes). */
  public static Settings parse(String text) {
    Properties props = new Properties();
    try {
      props.load(new StringReader(text));
    } catch (IOException | IllegalArgumentException e) {
      throw new IllegalArgumentException(FILE_NAME + " is not a properties file: " + e.getMessage(), e);
    }
    Map<String, String> all = new TreeMap<>();
    for (String k : props.stringPropertyNames()) all.put(k, props.getProperty(k));
    return fromMap(all);
  }

  private static final Set<String> NAMES = Set.of("pipeline", "streamlet", "namespace", "local");
  private static final Set<String> INLET_KEYS = Set.of("topic", "group", "client-id");
  private static final Set<String> OUTLET_KEYS = Set.of("topic", "client-id", "delivery", "transactional-id-prefix");

  private static boolean portKey(String kind, String key, Set<String> own) {
    if (!key.startsWith(kind + ".")) return false;
    String rest = key.substring(kind.length() + 1);
    int dot = rest.indexOf('.');
    if (dot < 0) return false;
    rest = rest.substring(dot + 1);
    return own.contains(rest) || (rest.startsWith("kafka.") && rest.length() > "kafka.".length());
  }

  static Settings fromMap(Map<String, String> given) {
    TreeMap<String, String> all = new TreeMap<>(given);
    for (String k : all.keySet()) {
      if (!NAMES.contains(k) && !k.startsWith("parameter.") && !portKey("inlet", k, INLET_KEYS)
          && !portKey("outlet", k, OUTLET_KEYS)) {
        throw new IllegalArgumentException(unknown(k));
      }
    }
    String pipeline = req(all, "pipeline");
    String streamlet = req(all, "streamlet");
    String namespace = req(all, "namespace");
    String localText = req(all, "local");
    boolean local = switch (localText) {
      case "true" -> true;
      case "false" -> false;
      default -> throw new IllegalArgumentException(FILE_NAME + ": local is '" + localText + "', not true or false");
    };
    List<InletSettings> inlets = new ArrayList<>();
    for (String p : ports(all, "inlet")) {
      inlets.add(new InletSettings(p, req(all, "inlet." + p + ".topic"), req(all, "inlet." + p + ".group"),
          req(all, "inlet." + p + ".client-id"), under(all, "inlet." + p + ".kafka.")));
    }
    List<OutletSettings> outlets = new ArrayList<>();
    for (String p : ports(all, "outlet")) {
      String topic = req(all, "outlet." + p + ".topic");
      String clientId = req(all, "outlet." + p + ".client-id");
      String wire = req(all, "outlet." + p + ".delivery");
      Delivery delivery = Delivery.fromWire(wire).orElseThrow(() -> new IllegalArgumentException(
          FILE_NAME + ": outlet." + p + ".delivery is '" + wire + "', not one of at-least-once, exactly-once"));
      String prefixKey = "outlet." + p + ".transactional-id-prefix";
      Optional<String> prefix = Optional.ofNullable(all.get(prefixKey));
      if (delivery == Delivery.EXACTLY_ONCE && prefix.isEmpty()) throw new IllegalArgumentException(missing(prefixKey));
      if (delivery == Delivery.AT_LEAST_ONCE && prefix.isPresent()) {
        throw new IllegalArgumentException(FILE_NAME + ": " + prefixKey + " is given for an at-least-once outlet");
      }
      outlets.add(new OutletSettings(p, topic, clientId, delivery, prefix, under(all, "outlet." + p + ".kafka.")));
    }
    return new Settings(pipeline, streamlet, namespace, local, under(all, "parameter."), inlets, outlets);
  }

  private static String req(Map<String, String> all, String key) {
    String v = all.get(key);
    if (v == null) throw new IllegalArgumentException(missing(key));
    return v;
  }

  private static Map<String, String> under(Map<String, String> all, String prefix) {
    Map<String, String> out = new TreeMap<>();
    all.forEach((k, v) -> {
      if (k.startsWith(prefix)) out.put(k.substring(prefix.length()), v);
    });
    return out;
  }

  private static Set<String> ports(Map<String, String> all, String kind) {
    Set<String> out = new TreeSet<>();
    for (String k : all.keySet()) {
      if (k.startsWith(kind + ".")) {
        String rest = k.substring(kind.length() + 1);
        int dot = rest.indexOf('.');
        out.add(dot < 0 ? rest : rest.substring(0, dot));
      }
    }
    return out;
  }
}
