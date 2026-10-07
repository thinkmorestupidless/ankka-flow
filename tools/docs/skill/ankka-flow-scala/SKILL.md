---
name: ankka-flow-scala
description: Write, test and package an ankka-flow streamlet in Scala with the ankka-flow-sdk artifact — declaring a Streamlet with inlet, outlet and typed parameter factories, the process(batch) method and its emits, acknowledging, skipping and failing a batch, Serve.run, writing and checking flow/descriptor.json with the Descriptor command, the testkit Harness, the local laptop loop with the sidecar in docker compose, and the image built with sbt-native-packager. Use when the task is Scala code for a streamlet, its tests, its descriptor, its image, or running it on a laptop. Also mapping events to graph deltas (ankka.graph-delta.v1) for the Neo4j merge sink with graphDeltaOutlet, which keys each delta by its element.
pages:
  - build/scala-streamlet.md
  - build/testing.md
  - build/images.md
  - reference/scala-sdk.md
  - reference/descriptor.md
  - reference/sidecar.md
  - concepts/contracts.md
  - concepts/delivery.md
  - get-started/first-streamlet.md
  - reference/graph-deltas.md
---

# Writing a streamlet in Scala

A Scala streamlet is a subclass of `com.thinkmorestupidless.ankka.flow.sdk.Streamlet` that declares
its ports and parameters as `val`s built with the base class's factories and implements
`process(batch)`, returning emits. `Serve.run` runs it on `127.0.0.1:$FLOW_PROCESS_PORT`, where the
sidecar in the same pod finds it. The SDK writes the streamlet's descriptor from the declaration; the
descriptor is what a blueprint is checked against and what the sidecar compares with the running
process before it starts. The artifact is `"com.thinkmorestupidless" %% "ankka-flow-sdk"`, for Scala
3.3 or later on Java 21 or later, at the ankka-flow release whose sidecar runs it.

## Rules

1. **Declare everything; discover nothing.** Ports (`inlet`, `outlet`, `graphDeltaOutlet`) and
   parameters (`parameter.string`, `.integer`, `.double`, `.boolean`, `.duration`, `.memorySize`) are
   `val`s built with the base class's factories, which register them in declaration order. There is
   no classpath scanning, no annotation and no macro. A name declared twice, or one the protocol
   refuses, throws `IllegalArgumentException` where it is declared.
2. **The descriptor is written, committed and checked.**
   `sbt "runMain com.thinkmorestupidless.ankka.flow.sdk.Descriptor <class> flow/descriptor.json"` writes
   it; with `--check` it exits 1 when the committed file differs. The streamlet class has a public
   no-argument constructor. Change a port, contract or parameter and the descriptor changes; the
   sidecar refuses to start a process whose declaration differs from the deployed descriptor.
3. **`process` is synchronous and per batch.** It is called on a worker thread, never twice at once
   for one partition, possibly concurrently for different partitions. Returning acknowledges the
   batch; throwing fails it, and the batch is redelivered from the last commit. Emits are held by the
   sidecar until the ack and discarded on a failure. An emit to an undeclared outlet fails the batch
   with `UndeclaredOutlet`.
4. **Skip by not emitting.** A record the streamlet does not want — including one it cannot decode —
   is skipped by returning no emit for it. Throwing for it stalls the partition until the code changes.
5. **Bytes in, bytes out.** `record.value` is the bytes Kafka held, `record.key` an
   `Option[Array[Byte]]`. `outlet.emit(record)` forwards the same key, headers and value;
   `outlet.emit(record.copy(value = …))` replaces a part; `outlet.emit(value, key, headers)` builds a
   new record. Keep the key to keep per-key ordering downstream.
6. **Typed parameters.** `config(parameter)` returns `Long` for `integer` and `memorySize`, `Double`,
   `Boolean`, `String`, or `FiniteDuration` for `duration`. A default is the value or its protocol
   text (`"100 ms"`, `"1 MiB"`); with none, the value must be set at deploy time.
7. **Tolerate repeats.** Delivery is at least once; a record may arrive again after a failure.
8. **The process gets `FLOW_PROCESS_PORT` and nothing else.** No Kafka address, no credentials, no
   ports to expose, no probes. The image holds the streamlet's classes, the SDK and a Java runtime.
   The SDK logs through slf4j with no binding; the image adds one so the sidecar's refusals appear in
   the pod's log.
9. **Test with the Harness first.** `com.thinkmorestupidless.ankka.flow.sdk.testkit.Harness` calls
   `process` with batches it builds and applies the protocol's rules, with no Kafka, sidecar or gRPC.
   Its members and its hash partitioner are the Python harness's.
10. **Deltas for the graph sink go through `graphDeltaOutlet`.** Declare
   `val deltas = graphDeltaOutlet("deltas")` and return `deltas.node(id = …, version = …, labels = …,
   properties = …, source = Some(record))`, `.edge(id, version, type, fromId, toId, …)`,
   `.tombstoneNode(…)` or `.tombstoneEdge(…)`. Each delta is the element's whole state, with a version
   that rises with the source entity. The outlet sets the record key to the element key (`node:<id>`
   or `edge:<id>`), which the sink requires, and throws `IllegalArgumentException` for what the sink
   would refuse. `source` keeps the input's headers and tells the Harness the record was not skipped.
   In a test, `graph.read(emitted)` parses a record back and checks its key.
11. **A project starts with `flow init <name>`.** It writes `build.sbt`, the streamlet, its suite,
   `flow/descriptor.json` with `sbt descriptor` and `sbt descriptorCheck`, a blueprint, the image
   build and the laptop compose file, depending on the SDK at the `flow` release that wrote it.

## Before writing

- What is each inlet's and outlet's schema name, and does it match the topic's other ports?
- Which records are skipped, which fail the batch, and is that intended?
- Does the key survive every emit that needs downstream ordering?
- Is the descriptor regenerated and committed after changing the declaration?
- Is the project on Scala 3.3 or later and Java 21 or later, with the SDK at the sidecar's release?

## Mistakes to check for

- Copying a sample instead of running `flow init`, so the SDK points at a path or a version the
  project does not build against.
- A Kafka client in streamlet code or in its dependencies; the sidecar owns Kafka.
- Throwing on a malformed record when the intent was to drop it.
- Per-partition or per-key state held in memory across batches, or shared mutable state in the
  streamlet that is not thread-safe.
- A port or parameter declared in a `def` or a conditional, or a streamlet whose constructor takes
  arguments, so the Descriptor command or `Serve.main` cannot construct it.
- `Serve` bound to anything but loopback, or an image that exposes a port.
- A stale `flow/descriptor.json`, or one edited by hand.
- Graph deltas built by hand with `outlet.emit(value, key = …)`: a key that is the bare id, or the
  input record's key, is refused by the sink. Use `graphDeltaOutlet`.
