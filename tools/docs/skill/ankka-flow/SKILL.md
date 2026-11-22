---
name: ankka-flow
description: What ankka-flow is and how a pipeline behaves — streamlets with typed inlets and outlets wired by a blueprint over Kafka topics, the two engines (Pekko Streams for per-record stages, Apache Flink for windows and state) and how to choose between them, descriptors, contracts matched by name and fingerprint, managed, unmanaged and ankka topics, Kafka names, consumer groups, delivery per engine (at least once with commit after the write; once between managed topics with checkpoints), never skipping, and when a design is an ankka consumer rather than a flow. Use for designing a pipeline, choosing an engine for a stage, choosing between ankka and ankka-flow, writing or reviewing a blueprint, wiring an ankka project's topics, or any ankka-flow question that is not specifically writing a Pekko Streams streamlet (ankka-flow-pekko-streams), a Flink streamlet (ankka-flow-flink) or deploying and operating (ankka-flow-deploy); load it first when unsure which skill applies.
pages:
  - index.md
  - concepts/pipelines.md
  - concepts/engines.md
  - concepts/delivery.md
  - concepts/contracts.md
  - concepts/topics.md
  - get-started/install.md
  - get-started/first-streamlet.md
  - get-started/coding-agents.md
  - build/blueprints.md
  - build/ankka-topics.md
  - reference/blueprint.md
  - reference/descriptor.md
  - reference/limitations.md
  - reference/glossary.md
---

# ankka-flow

ankka-flow runs streaming pipelines beside ankka. A pipeline is a graph of **streamlets**, each with
named, typed inlets and outlets, connected by Kafka **topics** according to a **blueprint**. A
streamlet is written against one engine's own API — Pekko Streams in Scala, or Apache Flink in Java —
and shipped as an image of its own; the platform runs it on that engine, creates the pipeline's
topics, and reports its state and each inlet's lag. Each engine reads and writes Kafka itself, with
the credentials of its own topics only.

## Rules

1. **A single reaction to a topic is an ankka consumer, not a flow.** Reach for ankka-flow when a
   design needs several of: a graph of stages whose contracts are checked against each other before
   anything runs, several typed outlets per stage, topics the platform creates and owns, per-key
   ordering through a chain, windows and state across records, lag per stage, or a rebuild from the
   start of the inputs.
2. **Choose the engine per stage, from what the stage needs.** Windows, state kept across records,
   late records or joins across streams: a Flink streamlet. Each record handled on its own (filter,
   clean, reshape, route to one of several outlets, enrich by a call): a Pekko Streams streamlet. A
   Pekko Streams streamlet has no windows, timers or state that survives a restart; never design one
   that needs them. One blueprint wires streamlets of both engines over one topic.
3. **The descriptor is the streamlet as the platform sees it.** Each SDK writes it from the
   declaration (`--descriptor <file>` on the streamlet's main, checked with `--descriptor-check`):
   name, engine (`pekko-streams` or `flink`), ports, contracts, parameters, in canonical JSON. A
   blueprint is verified against descriptors, never code, so `flow verify` needs no image, network or
   engine. A descriptor is never written or edited by hand.
4. **Ports connect by contract, checked before anything runs.** A contract is a name such as
   `cart-events.v1` and, optionally, a schema on the classpath whose fingerprint
   (`sha256:` and the hex SHA-256 of the schema in RFC 8785 canonical form, as ankka computes it) goes
   into the descriptor. Two contracts agree when the names are equal and, where both state one, the
   fingerprints are equal. Nothing checks a record against its schema in flight; the port's codec
   decodes it. A new version is a new name.
5. **A blueprint names streamlets and topics; never engines or images.** `blueprint.streamlets` maps a
   name in this pipeline to a descriptor's `streamlet.name` (one descriptor may run twice under two
   names). `blueprint.topics.<id>` lists `producers` (outlets) and `consumers` (inlets) as
   `<streamlet>.<port>`. Every inlet is on exactly one topic; an outlet is on at most one, and an
   unconnected outlet is a note, not an error.
6. **Three kinds of topic.** A **managed** topic (the default) is the pipeline's: the operator creates
   it once, named `<pipeline>.<id>` unless `topic.name` says otherwise, and never alters it. An
   **unmanaged** topic (`managed = false`) is someone else's: only read, never created, altered,
   deleted or produced to; it names `cluster` or `bootstrap.servers` and almost always `topic.name`.
   An **ankka topic** (`ankka = "<project>/<name>"`) is an ankka project's: Kafka name
   `<project>.<name>`, read and written as a registered machine the project grants each topic to,
   never created; it takes no `managed = true`, `topic.name` or `bootstrap.servers`.
7. **Check ankka topics against the project.** `flow verify --ankka-topics <project>=<file>`, the file
   from `ankka projects topics list -p <project> -o json`, checks every port's contract against the
   project's declaration; without the file the blueprint verifies with a note that it was not
   checked. The machine, its grants and the topic declarations are made with `ankka`, never with
   `flow`.
8. **Delivery depends on the engine and the topic.** A Pekko Streams streamlet is at least once:
   each record's offset is committed only after everything it produced, and everything before it on
   the partition, is acknowledged by the broker. A Flink streamlet counts each record once into topics
   the pipeline manages (Kafka transactions committed with each checkpoint, readers on
   `read_committed`, so downstream sees its records one checkpoint interval late), and at least once
   into an unmanaged or ankka topic. Every reader of an at-least-once topic tolerates repeats.
9. **Nothing is skipped by the platform.** A record that fails is retried from the last commit or
   checkpoint, indefinitely; its partition stops advancing and the inlet's lag grows. Dropping a
   record is the streamlet's own decision: emit nothing for it.
10. **Order is per key, through partitions.** Records with one key are on one partition and read in
    offset order; a streamlet that keeps the key when it emits keeps that order into the next topic.
11. **Consumer groups are per inlet.** `<pipeline>.<streamlet>.<inlet>`, prefixed
    `ankka.machine.<organization>.<name>.` on a machine's cluster. Two streamlets reading one topic each
    read every record; a Pekko Streams streamlet's replicas share their groups. A new group starts at
    the earliest offset.
12. **The resource says what runs.** `flow generate` merges deploy-time configuration into the
    `AnkkaFlow` resource; the operator adds only the Kafka cluster settings (from `kafka-cluster-<name>`
    Secrets) and, for Flink, the state store (`flink-state`). Credentials never appear in a blueprint,
    a resource or an image.
13. **`flow` is installed, not built.** `brew install thinkmorestupidless/tap/ankka-flow`, or a
    release's `ankka-flow-cli-<version>-<platform>.tar.gz` verified with its `.sha256`, for macOS
    arm64 and x64 and Linux arm64 and x64; one native binary, no JVM. Building it from source is for
    changing ankka-flow or another platform.
14. **A streamlet starts from the sample, not a template.** `samples/cart-pipeline` holds a Pekko
    Streams cleaner and a Flink counter with their tests, descriptors, blueprint and image builds; a
    project depends on the SDK from Maven Central (`ankka-flow-pekko-streams`, or `ankka-flow-flink`
    with `ankka-flow-flink-testkit`).
15. **`flow mcp` gives a coding agent the CLI as tools.** `verify_blueprint`, `generate_resource`,
    `flow_version`, `search_docs` and `read_doc` need no cluster; `list_pipelines`, `get_pipeline`,
    `pipeline_logs` and `pipeline_lag` read, and `apply_pipeline` and `reset_pipeline` change, only the
    cluster `flow.toml`'s `context` and `namespace` name, never kubectl's current context.

## Before answering

- Is this a pipeline at all, or one ankka consumer? Apply rule 1 first.
- Which engine does each stage need? Does any Pekko Streams stage need a window, a timer or state?
- Which topics are managed, unmanaged or ankka's? Unmanaged topics have no producers in the blueprint.
- Does every inlet's contract agree with every outlet's on its topic, and with the ankka project's?
- Which outlets are at least once, and does every reader of them tolerate a record twice?
- Does a limitation apply? Check `references/reference/limitations.md` before promising a feature.

## Mistakes to check for

- A Pekko Streams streamlet holding per-key or per-partition state in memory, or a window built by
  hand; it is a Flink streamlet.
- Expecting once-only delivery from a Pekko Streams streamlet, or from a Flink streamlet into an ankka
  or unmanaged topic.
- An engine, an image or a Kafka credential written into a blueprint.
- A blueprint that produces to an unmanaged topic, or leaves an inlet connected to nothing.
- An ankka topic with `managed = true`, `topic.name` or `bootstrap.servers`, or an ankka topic
  modelled as a plain unmanaged one so it cannot be written.
- A hand-written or hand-edited descriptor, or one not regenerated after a port, contract or
  parameter changed.
- Proposing Avro, Protobuf, a schema registry, a dead-letter topic, "skip after N retries", Flink SQL,
  a Python streamlet, Spark, or an HTTP or gRPC ingress; none exists.
- A guide or script that builds `flow` from source instead of installing it, or asks for a JVM to run it.
- Running `kubectl` against the shell's current context instead of the cluster `flow.toml` names.
