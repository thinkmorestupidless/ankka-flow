package com.thinkmorestupidless.ankka.flow.flink;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main of a Flink streamlet's jar: {@code FlinkStreamlets.run(new CartCounter(), args)}.
 * {@code --descriptor <path>} writes its descriptor, {@code --descriptor-check <path>} compares
 * it, and no argument builds and executes the job from the {@code streamlet.properties} in the
 * directory {@code FLOW_CONFIG_DIR} names; with {@code local=true}, on Flink's local environment.
 */
public final class FlinkStreamlets {
  private FlinkStreamlets() {}

  private static final Logger log = LoggerFactory.getLogger(FlinkStreamlets.class);

  static final java.time.Duration LOCAL_CHECKPOINT_INTERVAL = java.time.Duration.ofSeconds(10);

  static final String LOCAL_NOTICE =
      "local=true: running on Flink's local environment; state is kept only while this process runs";

  public static void run(FlinkStreamlet streamlet, String[] args) throws Exception {
    Optional<Integer> code = descriptorMode(streamlet, List.of(args), System.out, System.err);
    if (code.isPresent()) {
      if (code.get() != 0) System.exit(code.get());
      return;
    }
    Settings settings;
    try {
      settings = settings(System.getenv());
      check(streamlet, settings).ifPresent(p -> {
        throw new IllegalArgumentException(p);
      });
    } catch (IllegalArgumentException e) {
      System.err.println(streamlet.name() + ": " + e.getMessage());
      System.exit(1);
      return;
    }
    environment(streamlet, settings).execute("flow-" + settings.pipeline() + "-" + settings.streamlet());
  }

  /** The job built and not executed, on the environment its settings call for. */
  static StreamExecutionEnvironment environment(FlinkStreamlet streamlet, Settings settings) {
    Connectors.allowTokenEndpoints(settings);
    StreamExecutionEnvironment env;
    if (settings.local()) {
      log.warn(LOCAL_NOTICE);
      env = StreamExecutionEnvironment.createLocalEnvironment();
      // Checkpoints in memory, only so that offsets are committed to the inlet's group and a
      // stopped command starts again from them, with no state.
      env.enableCheckpointing(LOCAL_CHECKPOINT_INTERVAL.toMillis());
    } else {
      env = StreamExecutionEnvironment.getExecutionEnvironment();
    }
    streamlet.build(new KafkaContext(env, settings, contextParameters(streamlet, settings.parameters())));
    return env;
  }

  /** The settings in the directory {@code FLOW_CONFIG_DIR} names in {@code env}. */
  static Settings settings(Map<String, String> env) {
    String dir = env.get(Settings.DIRECTORY_VARIABLE);
    if (dir == null) {
      throw new IllegalArgumentException(Settings.DIRECTORY_VARIABLE + " is not set; it names the directory of "
          + Settings.FILE_NAME);
    }
    return Settings.read(Path.of(dir));
  }

  /** The first problem with running the streamlet from these settings, if there is one. */
  public static Optional<String> check(FlinkStreamlet streamlet, Settings settings) {
    List<String> problems = new ArrayList<>();
    Set<String> inlets = streamlet.inlets().stream().map(Inlet::name).collect(Collectors.toSet());
    Set<String> outlets = streamlet.outlets().stream().map(Outlet::name).collect(Collectors.toSet());
    Set<String> keys = streamlet.parameters().stream().map(Parameter::key).collect(Collectors.toSet());
    streamlet.inlets().stream().map(Inlet::name).filter(p -> settings.inlet(p).isEmpty())
        .forEach(p -> problems.add(Settings.missing("inlet." + p + ".topic")));
    streamlet.outlets().stream().map(Outlet::name).filter(p -> settings.outlet(p).isEmpty())
        .forEach(p -> problems.add(Settings.missing("outlet." + p + ".topic")));
    streamlet.parameters().stream().map(Parameter::key).filter(k -> !settings.parameters().containsKey(k))
        .forEach(k -> problems.add(Settings.missing("parameter." + k)));
    settings.inlets().stream().map(Settings.InletSettings::port).filter(p -> !inlets.contains(p))
        .forEach(p -> problems.add(Settings.FILE_NAME + " gives inlet '" + p + "', which " + streamlet.name()
            + " does not declare"));
    settings.outlets().stream().map(Settings.OutletSettings::port).filter(p -> !outlets.contains(p))
        .forEach(p -> problems.add(Settings.FILE_NAME + " gives outlet '" + p + "', which " + streamlet.name()
            + " does not declare"));
    new TreeSet<>(settings.parameters().keySet()).stream().filter(k -> !keys.contains(k))
        .forEach(k -> problems.add(Settings.FILE_NAME + " gives parameter '" + k + "', which " + streamlet.name()
            + " does not declare"));
    return problems.stream().findFirst();
  }

  /** Each declared parameter's value parsed from its text, its default when the text is absent. */
  public static Map<String, Object> contextParameters(FlinkStreamlet streamlet, Map<String, String> values) {
    Map<String, Object> out = new HashMap<>();
    for (Parameter<?> p : streamlet.parameters()) {
      String text = values.containsKey(p.key()) ? values.get(p.key()) : p.defaultValue().orElse(null);
      if (text == null) throw new IllegalArgumentException(Settings.missing("parameter." + p.key()));
      out.put(p.key(), p.parse(text));
    }
    return out;
  }

  /** The exit code of a descriptor mode, or empty when the arguments ask to run the streamlet. */
  static Optional<Integer> descriptorMode(FlinkStreamlet streamlet, List<String> args, PrintStream out,
      PrintStream err) {
    if (args.isEmpty()) return Optional.empty();
    if (args.size() == 2 && (args.get(0).equals("--descriptor") || args.get(0).equals("--descriptor-check"))) {
      String text;
      try {
        text = DescriptorWriter.write(streamlet);
      } catch (IllegalArgumentException e) {
        err.println(streamlet.name() + ": " + e.getMessage());
        return Optional.of(2);
      }
      Path file = Path.of(args.get(1));
      try {
        if (args.get(0).equals("--descriptor")) {
          Path parent = file.toAbsolutePath().getParent();
          if (parent != null) Files.createDirectories(parent);
          Files.writeString(file, text, StandardCharsets.UTF_8);
          out.println("wrote " + file);
          return Optional.of(0);
        }
        if (Files.isRegularFile(file) && Files.readString(file, StandardCharsets.UTF_8).equals(text)) {
          out.println(file + " is current");
          return Optional.of(0);
        }
        err.println(file + " differs from what " + streamlet.name() + " declares; write it with --descriptor");
        return Optional.of(1);
      } catch (IOException e) {
        err.println(file + ": " + e.getMessage());
        return Optional.of(2);
      }
    }
    err.println("usage: <main> [--descriptor <path> | --descriptor-check <path>]; with no arguments it runs the streamlet");
    return Optional.of(2);
  }
}
