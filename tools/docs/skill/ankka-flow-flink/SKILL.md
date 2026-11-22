---
name: ankka-flow-flink
description: Write, test and package an ankka-flow streamlet on Apache Flink in Java with the ankka-flow-flink SDK and ankka-flow-flink-testkit — a class extending FlinkStreamlet, inlet and outlet with a Contract and a Codec, typed parameters, build(FlinkContext) reading inlets as DataStream<Record<A>> with or without a WatermarkStrategy and writing outlets, event-time windows, keyed state and operator uids, FlinkStreamlets.run with --descriptor and --descriptor-check, FlinkHarness, the image on Flink's own with the jar in /opt/flink/usrlib, provided Flink modules, checkpoints and the checkpoint interval, parallelism, delivery once between managed topics and at least once elsewhere, what a new image may change in state, StateUnreadable and resetting to a new state epoch. Use when the task is Java code for a stage with windows, state, late records or joins, its tests, its descriptor or its image, or a Flink job that cannot restore its state; a per-record stage is ankka-flow-pekko-streams.
pages:
  - build/flink-streamlet.md
  - build/testing.md
  - build/images.md
  - reference/flink-sdk.md
  - reference/descriptor.md
  - reference/streamlet-settings.md
  - concepts/engines.md
  - concepts/contracts.md
  - concepts/delivery.md
  - deploy/configuration.md
  - deploy/reset.md
  - get-started/first-streamlet.md
---

# Writing a Flink streamlet

A Flink streamlet is a Java class extending `com.thinkmorestupidless.ankka.flow.flink.FlinkStreamlet`.
It declares its inlets, outlets and parameters as fields built with the base class's factories and
implements `build(FlinkContext ctx)`, which reads its inlets as Flink `DataStream`s, transforms them
with Flink's own API, and writes the results to its outlets. The platform runs it as a Flink job in a
`FlinkDeployment`, checkpoints its state to the installation's state store, and connects every port
to its topic from the settings it is deployed with. The SDK is
`com.thinkmorestupidless:ankka-flow-flink`, the harness `com.thinkmorestupidless:ankka-flow-flink-testkit`;
plain Java libraries for Java 17 or later, written against Flink 2.2.

## Rules

1. **Declare everything as fields.** `inlet(name, contract, codec)` and `outlet(name, contract, codec)`
   return `Inlet<A>` and `Outlet<A>`; `stringParameter`, `intParameter`, `doubleParameter`,
   `booleanParameter`, `durationParameter` (default as text such as `"1m"`, read as
   `java.time.Duration`) and `memorySizeParameter` (`"64MiB"`, read as a `Long` of bytes) take a key, a
   default and an optional description. The constructor calls `super(name, description)`. Fields appear
   in the descriptor in declaration order.
2. **Contracts and codecs.** `Contract.of(name)` or `Contract.of(name, schemaResource)` (a classpath
   schema whose fingerprint goes into the descriptor). `Codec.json(Type.class)` reads and writes JSON
   with Jackson, by field or record component names; `Codec.string()` and `Codec.bytes()` pass text and
   bytes through. A record the inlet's codec cannot decode fails the job, which restarts and meets it
   again; to drop malformed input, read `Codec.bytes()` and decode in the job.
3. **`build` wires the job through the context.** `ctx.read(inlet)` is a `DataStream<Record<A>>` with
   no watermarks; `ctx.read(inlet, watermarks)` takes a `WatermarkStrategy<Record<A>>`, needed for
   event-time windows (the sample: `forBoundedOutOfOrderness`, `withTimestampAssigner` from the event's
   own time, `withIdleness` so a quiet partition does not hold the others back).
   `ctx.write(outlet, stream)` takes a `DataStream<Record<B>>`; `ctx.get(parameter)` the parameter's
   value; `ctx.env()` the `StreamExecutionEnvironment`. Never build a Kafka source or sink yourself.
4. **A record is key, value, headers and timestamp.** `Record<A>(key, value, headers, timestamp)`;
   `Record.of(key, value, timestamp)` makes one with no headers, `withValue(other)` keeps the rest. On
   write, the key is the Kafka key and the timestamp the Kafka timestamp: give a window's result the
   window's `maxTimestamp()`, as the counter does. Key by what ordering downstream needs.
5. **Give an operator producing `Record<B>` its type.** Java erases the generic, so the sample ends its
   window with `.returns(TypeInformation.of(new TypeHint<Record<CartCount>>() {}))`.
6. **Every stateful operator has a stable `uid`.** The sample's window is `.uid("count")`; the SDK gives
   each source and sink one made from the port's name. A new image resumes from the old one's state
   only when every stateful operator keeps its `uid` and port names are kept.
7. **State types change only as Flink allows.** A POJO (public class, public no-argument constructor,
   public fields or getters and setters, like the sample's `Tally`) may gain or lose fields, never
   retype or rename one. A type that is not a POJO or Avro is stored with Kryo and must not change at
   all. An image that cannot read the state leaves the job failing and the pipeline `Degraded` with
   `StateUnreadable`, the store untouched: deploy the previous image again, or reset the streamlet to
   start with no state at a new state epoch.
8. **Delivery is per outlet's topic.** Into a topic the pipeline manages, the outlet writes in a Kafka
   transaction committed with each checkpoint, so each record is counted once and a window written
   once; readers see it one checkpoint interval later. Into an unmanaged or ankka topic it is at least
   once: a restart may write again what it wrote since the last checkpoint. Inlets' groups are
   committed on each checkpoint, so lag trails by up to one interval.
9. **Checkpoints, parallelism and replicas are deploy-time settings.** `flow.streamlets.<name>.flink
   { checkpoint-interval = 10s, job-manager { cpu, memory }, task-manager { cpu, memory } }` and
   `parallelism` (default 1) in a `--conf` file; `replicas` is `1` (running) or `0` (suspended, state
   kept). Nothing in the streamlet's code configures checkpointing or restarts.
10. **`FlinkStreamlets.run(streamlet, args)` is the main.** The class `entryClass()` names (the
    streamlet's own unless overridden) has `public static void main` calling it. With no arguments it
    reads `streamlet.properties` from `FLOW_CONFIG_DIR`, refuses settings that do not match the
    declaration (exit 1), builds the job and runs it; with `local=true`, as `flow local` writes, on a
    local Flink in the process with in-memory checkpoints every ten seconds and state that lasts only
    while it runs. `--descriptor <file>` writes the descriptor (engine `flink`, the entry class and Flink
    `2.2`); `--descriptor-check <file>` exits non-zero when the committed file differs.
11. **Flink's modules are `provided`.** `flink-streaming-java` (2.2.x) and the connector base are in
    Flink's image; a second copy breaks class loading. In sbt, `run` and `runMain` leave `provided`
    modules off the classpath: point them at `Compile / fullClasspath` and add `flink-clients` as
    `provided`, as the sample's build does, to run the main or write the descriptor on a laptop.
12. **Test with `FlinkHarness` first.** `FlinkHarness.of(streamlet).parameter("window", "5m")
    .input(streamlet.in, List.of(Record.of(...)))` then `.run()` runs the whole job on a local Flink, at
    parallelism 1 with no restarts, over bounded inputs; every window closes once the inputs are read.
    `result.records(streamlet.out)` is what the outlet wrote. A failing job throws
    `FlinkHarness.Failed` with its root cause. Set a record's Kafka timestamp apart from its event time
    to prove the watermarks use the event's own.
13. **The image is Flink's own.** `FROM` the Flink image of the version built against (the sample:
    `flink:2.2.1-java17`), the streamlet's jar at `/opt/flink/usrlib/streamlet.jar`, and every
    dependency Flink does not provide (the SDK, the Kafka connector, Jackson) beside it in `usrlib/`,
    owned by `flink:flink`. No ports, no settings, no credentials.

## Before writing

- Does the stage really need windows, state, late records or a join? If not, it is a Pekko Streams
  streamlet.
- Which time do the windows use, and is there a watermark strategy with idleness for quiet partitions?
- Is every stateful operator given a `uid`, and is every state type a POJO that can evolve?
- Which outlets are on managed topics (once) and which on unmanaged or ankka topics (at least once)?
- Is the descriptor regenerated and committed after changing the declaration or the entry class?

## Mistakes to check for

- `ctx.read(inlet)` with no watermark strategy feeding an event-time window that then never fires.
- A stateful operator without a `uid`, a changed `uid`, or a renamed port, then a new image deployed
  over existing state.
- A state type retyped or renamed, or a non-POJO state class edited, expecting the state to survive.
- Flink modules bundled into `usrlib/`, or the SDK or the Kafka connector left out of it.
- Checkpointing, restart strategy or Kafka sources and sinks configured in `build`.
- Flink `replicas` above 1 to scale it; `parallelism` scales a Flink job.
- Expecting once-only writes into an ankka topic, or immediate visibility downstream of a managed one.
- Flink SQL, a query-only streamlet or a Python job, none of which exists; or Scala on Flink's Java
  API, which is neither documented nor tested.
- A stale `flow/<name>.json`, or one edited by hand.
