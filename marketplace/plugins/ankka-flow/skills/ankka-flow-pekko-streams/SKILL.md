---
name: ankka-flow-pekko-streams
description: Write, test and package an ankka-flow streamlet on Pekko Streams in Scala with the ankka-flow-pekko-streams SDK — an object extending PekkoStreamlet, inlet and outlet with a Contract and a Codec (json with jsoniter-scala, string, bytes), typed parameters (stringParameter, intParameter, doubleParameter, booleanParameter, durationParameter, memorySizeParameter), logic(ctx) with from(inlet) stages written with Pekko Streams operators over FlowWithContext, emits as Emit, Seq or Option, dropping a record, calling out with mapAsync and ctx.system, commit after the write and at-least-once delivery, PekkoStreamlets.run with --descriptor and --descriptor-check, PekkoHarness and its committed positions, running on a laptop with flow local and FLOW_CONFIG_DIR, and the image with sbt-native-packager. Use when the task is Scala code for a per-record stage (filter, clean, reshape, route, enrich), its tests, its descriptor or its image; a stage that needs windows or state is ankka-flow-flink.
---

# Writing a Pekko Streams streamlet

A Pekko Streams streamlet is a Scala `object` extending
`com.thinkmorestupidless.ankka.flow.pekko.PekkoStreamlet(name, description)`. It declares its ports
and parameters as `val`s built with the base class's factories and implements
`logic(ctx: Context): Logic`, giving each inlet a stage written with Pekko Streams' own operators.
The SDK reads each inlet's topic, runs the stage, writes the emits to the outlets, and commits each
record's offset once everything made from it is written. The artifact is
`"com.thinkmorestupidless" %% "ankka-flow-pekko-streams"`, for Scala 3.3 or later on Java 21, with
Pekko 1.7.

## Rules

1. **Declare everything; discover nothing.** `inlet(name, contract, codec)` and
   `outlet(name, contract, codec)` declare ports; `stringParameter`, `intParameter` (an `Int`),
   `doubleParameter`, `booleanParameter`, `durationParameter` (default as text such as `"1m"`, read as
   a `FiniteDuration`) and `memorySizeParameter` (text such as `"64MiB"`, read as a `Long` of bytes)
   take `key`, `default` and `description`. They are `val`s on the object, registered in declaration
   order, which is the descriptor's order. No scanning, no annotations.
2. **A contract is a name, optionally with a schema.** `Contract("clean-carts.v1")`, or
   `Contract("cart-events.v1", schema = "cart-events.v1.json")` with the schema a classpath resource
   whose fingerprint goes into the descriptor. Every port on a topic must agree with this one.
3. **Pick the codec for what a bad record should do.** `Codec.json[A]` needs a jsoniter-scala
   `JsonValueCodec[A]` in scope (`JsonCodecMaker.make`); `Codec.string` is UTF-8; `Codec.bytes` passes
   bytes through. A record a typed codec cannot decode fails the stream, and the record is read again
   forever. To drop undecodable input, read `Codec.bytes` and decode in the stage, as the sample's
   cleaner does.
4. **A stage is a `FlowWithContext`.** `from(inlet) { stage => … }` receives a `Stage[A]`, records with
   each offset carried as context; use its operators (`map`, `filter`, `collect`, `mapConcat`,
   `mapAsync`, `via`) so every element keeps its offset. Its elements must be an `Emit`, a `Seq[Emit]`
   or an `Option[Emit]`. Several inlets join their stages with `and`; `Logic.empty` gives none.
5. **Emit through the outlet.** `out(key, value)` makes an emit with that `String` key;
   `out(record)` emits a `Record[A](key: Option[String], value, headers)`, and `record.withValue(b)`
   keeps the key and headers. Keep the key to keep per-key ordering into the next topic.
6. **Drop by emitting nothing.** `Nil` or `None` writes nothing and still lets the offset be
   committed; a record removed with `filter` is committed with the next one. Throwing for a record
   stops the inlet's stream and restarts it from the last commit after a backoff of one to thirty
   seconds, indefinitely, so the partition makes no progress until the code changes.
7. **Delivery is at least once.** An offset is committed only after every emit of its record, and of
   every record before it on the partition, is acknowledged by the broker. After a failure or a
   rebalance, records whose results were written but not committed are processed again; logic and
   readers downstream must tolerate repeats.
8. **No windows, timers or state.** Nothing survives a restart or a rebalance, and the SDK adds none.
   Replicas share each inlet's consumer group, so a pod sees only some partitions. A stage needing
   state across records is a Flink streamlet.
9. **Calls out go through `mapAsync`.** `ctx.system` is the streamlet's `ActorSystem`; `ctx(parameter)`
   reads a parameter's deployed value, or its default; both are given only to `logic`.
10. **`PekkoStreamlets.run(streamlet, args)` is the main.** With no arguments it reads
    `streamlet.properties` from `FLOW_CONFIG_DIR`, refuses settings that do not match the declared
    ports and parameters (`<streamlet>: <key> is missing`, exit 1), and runs one stream per inlet; on a
    stop the streams drain. `--descriptor <file>` writes the descriptor; `--descriptor-check <file>`
    exits non-zero when the committed file differs. Run the check in the build:
    `sbt "runMain <main> --descriptor-check flow/<name>.json"`.
11. **Test with `PekkoHarness` first.** `PekkoHarness(Streamlet, "key" -> "value")` sets parameters as
    text (the rest at their defaults); `.run(Streamlet.in -> Seq(Record(Some("k"), value)))` runs each
    inlet's stage with no broker. `result(Streamlet.out)` is what the outlet was written, in order;
    `result.committed` (or `committedOf(inlet)`) the positions whose offsets would be committed —
    those the stage returned an element for, an empty `Seq` or `None` included. Records go through the
    inlet's codec and emits through the outlet's, so a codec failure fails the run with
    `PekkoHarness.Failed` (its `inlet`, `position` and `committed`).
12. **The image holds a JVM and the streamlet only.** sbt-native-packager's `JavaAppPackaging` and
    `DockerPlugin`, a Java 21 base such as `eclipse-temurin:21-jre`, `Compile / mainClass` set to the
    object calling `PekkoStreamlets.run`, and an slf4j binding writing to standard output (Logback in
    the sample). No ports, no health check (the SDK writes `/tmp/flow/ready` and `/tmp/flow/alive`, the
    pod's probes), no broker address, no credentials. A tag with `+` replaces it with `-`.
13. **On a laptop, `flow local` writes the settings.** Run it over the blueprint with
    `--bootstrap-servers` and `-o <dir>`, then start the main with
    `FLOW_CONFIG_DIR=<dir>/<streamlet>`; a fork in sbt needs the variable forwarded
    (`run / envVars`).

## Before writing

- Does the stage need anything beyond the record in hand? If so, it is a Flink streamlet.
- What is each port's contract, and does it match the other ports on its topic?
- Which records are dropped, which fail the stream, and is that intended? Does the codec decide it?
- Does every emit that needs downstream ordering keep the record's key?
- Is the descriptor regenerated and committed after changing the declaration?

## Mistakes to check for

- Mutable state, caches of results, or counters across records in the object; nothing survives a
  restart and replicas do not share it.
- A typed `Codec.json` on an input that may hold malformed records, so one bad record stalls its
  partition.
- `mapAsync` calls that are not idempotent, given at-least-once redelivery.
- A Kafka client, producer or consumer in the streamlet's code; the SDK owns Kafka.
- A port or parameter declared in a `def`, a conditional or `logic`, so the descriptor differs from
  what runs.
- A stale `flow/<name>.json`, or one edited by hand.
- An image that exposes a port, adds a health check, or bakes in settings.

## Reference files

Open the one a task needs; each is one topic and stands alone.

### Get started

- `references/get-started/first-streamlet.md` — Run the sample cart pipeline on a laptop — a Pekko Streams streamlet that cleans cart events and a Flink streamlet that counts them — against Kafka in a container, with flow local writing each streamlet's settings.

### Concepts

- `references/concepts/engines.md` — Which need selects a Flink streamlet, which a Pekko Streams streamlet, and which an ankka consumer instead of a pipeline, and what each promises about delivery.
- `references/concepts/delivery.md` — What each engine promises about delivery — a Pekko Streams streamlet at least once, committing only after the write; a Flink streamlet once between managed topics — and what happens when a streamlet fails.
- `references/concepts/contracts.md` — What a port's contract is — a name and, optionally, a schema's fingerprint, as ankka names one — and how two ports are matched by it before anything runs.

### Build

- `references/build/pekko-streams-streamlet.md` — Declare a Pekko Streams streamlet's ports and parameters in Scala, write each inlet's stage with Pekko Streams' own operators, test it with the harness, and package its image.
- `references/build/testing.md` — Test a Pekko Streams streamlet's stages with PekkoHarness and a Flink streamlet's job with FlinkHarness, each over records in memory with no Kafka and no cluster, and check the descriptor beside the tests.
- `references/build/images.md` — Package a streamlet as a container image for its engine — a Java runtime for a Pekko Streams streamlet, Flink's own image for a Flink streamlet — and make it available to a cluster.

### Reference

- `references/reference/descriptor.md` — The descriptor file an SDK writes from a streamlet's declaration — its engine, ports, contracts and parameters in canonical JSON — and every rule flow and the operator check it against.
- `references/reference/streamlet-settings.md` — The file streamlet.properties a streamlet is given at run time — its names, parameters, and the topic, group, client id, delivery and Kafka properties of each of its ports — and the rules both SDKs read it by.
- `references/reference/pekko-streams-sdk.md` — The types of ankka-flow-pekko-streams: PekkoStreamlet's port and parameter factories, Stage, Logic, Emit, codecs, contracts, records, PekkoStreamlets' modes and PekkoHarness.
