package com.thinkmorestupidless.ankka.flow.flink;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * The descriptor a declaration writes (descriptor version 2.0), in the descriptor's canonical
 * layout, refused with every problem when it is not a valid one.
 */
public final class DescriptorWriter {
  private DescriptorWriter() {}

  public static final String DESCRIPTOR_VERSION = "2.0";
  public static final String SDK_NAME = "ankka-flow-flink";

  private static final Pattern STREAMLET_NAME = Pattern.compile("[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?");
  private static final Pattern PORT_NAME = Pattern.compile("[a-z][a-z0-9-]{0,62}");
  private static final Pattern PARAM_KEY = Pattern.compile("[a-z][a-z0-9-]*");
  private static final String CONTRACT_RULE = "[a-z0-9][a-z0-9._-]{0,98}[a-z0-9]";
  private static final Pattern CONTRACT_NAME = Pattern.compile(CONTRACT_RULE);

  /** This SDK's version, as its build wrote it. */
  public static String sdkVersion() {
    try (InputStream in = DescriptorWriter.class.getResourceAsStream("/ankka-flow-flink.properties")) {
      if (in == null) return "0.0.0";
      Properties p = new Properties();
      p.load(in);
      return p.getProperty("version", "0.0.0");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static String write(FlinkStreamlet s) {
    return write(s, SDK_NAME, sdkVersion());
  }

  public static String write(FlinkStreamlet s, String sdkName, String sdkVersion) {
    List<String> problems = new ArrayList<>();
    ClassLoader loader = Optional.ofNullable(s.getClass().getClassLoader())
        .orElse(ClassLoader.getSystemClassLoader());

    if (!STREAMLET_NAME.matcher(s.name()).matches()) {
      problems.add("streamlet name '" + s.name()
          + "' must be 1-63 of [a-z0-9-], not starting or ending with '-'");
    }
    if (s.entryClass().isBlank()) problems.add("a flink streamlet's descriptor names its entry class");

    Map<String, Integer> counts = new TreeMap<>();
    List<Object> inlets = ports("inlet", s.inlets().stream().map(i -> new Port(i.name(), i.contract())).toList(), loader, problems, counts);
    List<Object> outlets = ports("outlet", s.outlets().stream().map(o -> new Port(o.name(), o.contract())).toList(), loader, problems, counts);
    counts.forEach((n, c) -> {
      if (c > 1) problems.add("port '" + n + "' is declared " + c + " times");
    });

    Map<String, Integer> keys = new TreeMap<>();
    List<Object> params = new ArrayList<>();
    for (Parameter<?> p : s.parameters().stream()
        .sorted(Comparator.comparing(Parameter::key)).toList()) {
      keys.merge(p.key(), 1, Integer::sum);
      if (!PARAM_KEY.matcher(p.key()).matches()) {
        problems.add("parameter key '" + p.key() + "' must match [a-z][a-z0-9-]*");
      }
      Map<String, Object> m = new LinkedHashMap<>();
      m.put("key", p.key());
      if (!p.description().isEmpty()) m.put("description", p.description());
      if (p.type() != Parameter.Type.STRING) m.put("type", p.type().name());
      p.defaultValue().filter(d -> !d.isEmpty()).ifPresent(d -> m.put("default_value", d));
      params.add(m);
    }
    keys.forEach((k, c) -> {
      if (c > 1) problems.add("parameter '" + k + "' is declared " + c + " times");
    });

    if (!problems.isEmpty()) {
      throw new IllegalArgumentException("streamlet '" + s.name() + "' is not a valid declaration: "
          + String.join("; ", problems));
    }

    Map<String, Object> streamlet = new LinkedHashMap<>();
    streamlet.put("name", s.name());
    if (!s.description().isEmpty()) streamlet.put("description", s.description());
    streamlet.put("engine", "flink");
    streamlet.put("flink", Map.of("entry_class", s.entryClass(), "version", FlinkStreamlet.FLINK_VERSION));
    if (!inlets.isEmpty()) streamlet.put("inlets", inlets);
    if (!outlets.isEmpty()) streamlet.put("outlets", outlets);
    if (!params.isEmpty()) streamlet.put("config_parameters", params);

    Map<String, Object> root = new LinkedHashMap<>();
    root.put("descriptor_version", DESCRIPTOR_VERSION);
    root.put("sdk", Map.of("name", sdkName, "version", sdkVersion));
    root.put("streamlet", streamlet);
    return Canonical.pretty(root);
  }

  private record Port(String name, Contract contract) {}

  private static List<Object> ports(String kind, List<Port> ports, ClassLoader loader,
      List<String> problems, Map<String, Integer> counts) {
    List<Object> out = new ArrayList<>();
    for (Port p : ports.stream().sorted(Comparator.comparing(Port::name)).collect(Collectors.toList())) {
      counts.merge(p.name(), 1, Integer::sum);
      if (!PORT_NAME.matcher(p.name()).matches()) {
        problems.add(kind + " name '" + p.name() + "' must match [a-z][a-z0-9-]{0,62}");
      }
      if (!CONTRACT_NAME.matcher(p.contract().name()).matches()) {
        problems.add(kind + " '" + p.name() + "': contract name '" + p.contract().name()
            + "' must match " + CONTRACT_RULE);
      }
      Map<String, Object> contract = new LinkedHashMap<>();
      contract.put("name", p.contract().name());
      try {
        p.contract().fingerprint(loader).ifPresent(f -> contract.put("fingerprint", f));
      } catch (IllegalArgumentException e) {
        problems.add(e.getMessage());
      }
      out.add(Map.of("name", p.name(), "contract", contract));
    }
    return out;
  }
}
