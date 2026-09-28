# Tasks: ankka-flow version one

**Input**: Design documents from `/specs/001-version-one/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/, quickstart.md

**Tests**: included. The spec requires them: every piece carried from the Cloudflow fork arrives
with its real-Kafka test (SC-007), the commit-after-write guard must be proven by a mutation
(SC-006), the conformance suite and the descriptor fixtures are deliverables (FR-002, FR-026), and
each story's independent test is an automated suite in quickstart.md. Carried tests are rewritten
as munit with their assertions and Lightbend headers intact.

**Organization**: by user story. US1 is the whole feature in miniature and is the MVP.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: can run in parallel (different files, no dependency on an unfinished task)
- **[Story]**: US1–US5 from spec.md
- Paths are repository-relative. Scala packages are `com.thinkmorestupidless.ankka.flow.<module>`;
  `.../` below abbreviates `src/main/scala/com/thinkmorestupidless/ankka/flow/` (or `src/test/scala/...`).

## Path Conventions

Six sbt projects at the root (`protocol/`, `blueprint/`, `crd/`, `sidecar/`, `operator/`, `cli/`),
the Python SDK at `sdks/python/`, the sample at `samples/cart-router/`, installation at
`kustomization/`, documentation at `docs/`. See plan.md *Project Structure*.

---

## Phase 1: Setup (the repository)

**Purpose**: an sbt build on ankka's conventions, the repository's own rules, and the design doc
brought in line with research.md.

- [X] T001 Create `build.sbt` with projects `protocol`, `blueprint`, `crd`, `sidecar`, `operator`, `cli` and `root` aggregating them; `commonSettings` (Scala 3.9.0, `-deprecation -feature -unchecked -Wunused:all -Wvalue-discard -source:3.7`, `javacOptions --release 21`, `Test / fork := true`, `Test / parallelExecution := false`, `Global / concurrentRestrictions += Tags.limit(Tags.Test, 1)`, munit framework, forwarding `flow.cluster.tests`, `flow.conformance.target`, `flow.mutation`, `flow.fixtures.regenerate` to the forked test JVM); `dockerSettings` (`eclipse-temurin:21-jre`, `dockerUpdateLatest`, `dockerRepository` from `DOCKER_REPOSITORY`, OCI labels, `Docker / version` with `+`→`-`); artifact names `ankka-flow-<project>`; `publish / skip` on `sidecar`, `operator`, `cli`; aliases `buildAll` and `mutationCheck` (the latter filled in by T049). Add `project/build.properties` (`sbt.version=1.12.15`), `project/plugins.sbt` (sbt-native-packager 1.11.7, sbt-scalafmt 2.6.2, sbt-ci-release 1.12.1, sbt-buildinfo 0.13.2, sbt-protoc 1.0.6 with ScalaPB compilerplugin 0.11.11) and `project/Dependencies.scala` with `object V` (pekko 1.7.0, pekkoKafka 1.2.0, kafkaClients 3.9.2, jsoniter 2.40.1, fabric8 7.9.0, jackson 2.21.4, decline 2.6.2, munit 1.3.6, testcontainers 1.21.4, logback 1.6.3) per plan.md *Technical Context*.
- [X] T002 [P] Add `.scalafmt.conf` (ankka's: version 3.9.4, scala3 dialect, maxColumn 100, align more, Asterisk docstrings, RedundantBraces/RedundantParens, convertToNewSyntax, removeOptionalBraces false), `.githooks/pre-commit` (scalafmt check of staged `.scala`/`.sbt`), and `.gitignore` (`target/`, `.bsp/`, `.venv/`, `sdks/python/src/ankka_flow/_proto/`, `samples/cart-router/flow/descriptor.json` is **committed**, so not ignored).
- [X] T003 [P] Create `Justfile` with recipes `default`, `fmt`, `hooks`, `test` (`sbt -Dflow.cluster.tests=off test`), `test-all` (`caffeinate -i sbt test`), `images` (`sbt docker:publishLocal`), `kafka-up`/`kafka-down` (root compose), `cluster-create`/`cluster-delete`/`cluster-status`/`deploy`/`up`/`down`/`render` (filled by T092), `docs`.
- [X] T004 [P] Write `CLAUDE.md`: what ankka-flow is and its relation to ankka and Cloudflow; the commands (from `Justfile` and quickstart.md tiers); the principles table from plan.md *Constitution Check* as this repository's rules; the module dependency direction; "no pekko-http, no pekko-grpc, no Avro, no spray-json, no ScalaTest"; the carried-code rule (Lightbend header stays, test comes with it); the `-Dflow.*` switches; the symlink rule for `kustomization/`.
- [X] T005 [P] Add `NOTICE` naming the files derived from Cloudflow (Copyright 2016–2026 Lightbend Inc., Apache 2.0) with the list maintained as pieces land; confirm `LICENSE` is Apache 2.0.
- [X] T006 [P] Create `.github/workflows/ci.yml` with a `changes` job (`dorny/paths-filter@v4`; filters `scala` for `build.sbt`, `project/**`, `protocol/**`, `blueprint/**`, `crd/**`, `sidecar/**`, `operator/**`, `cli/**`; `sdk-python` for `protocol/**`, `sdks/python/**`, `samples/**`; `docs` for `docs/**`), a `build` job (`ubuntu-24.04`, temurin 21, `sbt/setup-sbt@v1`, `sbt -Dflow.cluster.tests=off scalafmtCheckAll scalafmtSbtCheck compile test`), and empty `sdk-python` and `docs` jobs to be filled by T117 and T120.
- [X] T007 [P] Create root `docker-compose.yml` with one `kafka` service (`apache/kafka:3.9.1`, KRaft single node, listeners `PLAINTEXT://kafka:9092` and `EXTERNAL://localhost:9094`, `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`) for repository development.
- [X] T008 [P] Update `notes/design/version-one.md` to match research.md: grpc-java with ScalaPB (R1), `FLOW_` variables and no callback service (R2), JSON-only contracts (R4), canonical descriptor JSON (R5), files for probes and the JMX agent for metrics (R7), the stall event (R9), `flow.ankka.thinkmorestupidless.com` (R11), the CLI/operator precedence split (R12), Events from the operator (R13); remove Avro and pekko-grpc.

---

## Phase 2: Foundational (the protocol)

**Purpose**: the artefact every other module and every SDK consumes. Nothing story-specific can
start without it.

**⚠️ CRITICAL**: no user story work begins until this phase compiles and its fixtures pass.

- [X] T009 Write `protocol/src/main/protobuf/ankka/flow/v1/payload.proto` exactly as `contracts/protocol.md` (`Record`, `Header`, `Error`, `Problem`, `Problems`, `Empty`; package `ankka.flow.v1`).
- [X] T010 [P] Write `protocol/src/main/protobuf/ankka/flow/v1/discovery.proto` (`Discovery` service, `SidecarInfo`, `Spec`, `SdkInfo`, `StreamletDescriptor`, `Port`, `Contract`, `ConfigParameter`, `ConfigType`) per `contracts/protocol.md`.
- [X] T011 [P] Write `protocol/src/main/protobuf/ankka/flow/v1/streamlet.proto` (`Streamlet.Run`, `ToProcess`, `Start`, `PortBinding`, `Batch`, `InputRecord`, `Stop`, `FromProcess`, `Emit`, `Ack`, `Fail`) per `contracts/protocol.md`.
- [X] T012 Configure the `protocol` project in `build.sbt`: `Compile / PB.targets := Seq(scalapb.gen(grpc = true) -> (Compile / sourceManaged).value / "scalapb")`, own `scalacOptions := Seq("-encoding", "UTF-8", "-source:3.3")`, dependencies `scalapb-runtime-grpc`, `grpc-netty-shaded`, `grpc-stub` at ScalaPB's pinned versions, jsoniter-scala core and macros; run `sbt protocol/compile` and record in research.md item 1 that it is warning-free.
- [X] T013 [P] Create `protocol/.../protocol/Fingerprint.scala` carrying `fingerprint(schemaName) = Base64(SHA-256(UTF-8 bytes))` and `Format = "json"` from the fork's `core/cloudflow-json/src/main/scala/cloudflow/streamlets/json/Json.scala` with its Lightbend header, and `protocol/src/test/.../protocol/FingerprintSuite.scala` carrying `JsonSpec`'s assertions (same name equal, different name different, format `json`) as munit.
- [X] T014 [P] Create `protocol/.../protocol/ProtocolVersion.scala` (`Current = "1.0"`, `parse`, `compatible(sidecar, sdk): Either[String, Unit]` refusing another major or a later minor naming both) and `protocol/src/test/.../protocol/ProtocolVersionSuite.scala`.
- [X] T015 Create `protocol/.../protocol/DescriptorJson.scala`: jsoniter read/write of `Spec` to the canonical form in `contracts/descriptor.md` (snake_case proto names, sorted keys, ports sorted by name, parameters by key, enums as names, defaults omitted, 2-space indent, trailing LF); `write(spec): String`, `read(json): Either[String, Spec]`.
- [X] T016 Create `protocol/.../protocol/DescriptorValidation.scala`: `validate(spec): Vector[Problem]` with every rule in `contracts/descriptor.md` *Validation* (name regex, port uniqueness across inlets and outlets, format `json`, fingerprint recomputed, parameter key regex and uniqueness, default parses as type using Typesafe Config's duration and memory-size parsers, protocol version shape); `compare(deployed, discovered): Vector[Problem]` naming every field that differs.
- [X] T017 [P] Write `protocol/DESCRIPTOR.md` (the canonical rules, the example, the fixture table, validation) and `protocol/README.md` (the version rule, the conversation summary, the *Rules the messages do not state* list) from `contracts/protocol.md` and `contracts/descriptor.md`.
- [X] T018 Write the five declarations `protocol/fixtures/declarations/{minimal,cart-router,every-type,many-ports,sink,conformance}.md` (six, including `conformance` from `contracts/conformance.md`) and `protocol/src/test/.../protocol/DescriptorFixturesSuite.scala` that builds each `Spec` in Scala, writes `protocol/fixtures/descriptors/<name>.json` when `-Dflow.fixtures.regenerate=on`, and otherwise fails on any byte difference; run it once with regenerate to create the six fixture files and commit them.
- [X] T019 Write `protocol/src/test/.../protocol/DescriptorJsonSuite.scala`: every fixture parses and re-writes byte for byte; each validation rule has a failing case; `compare` names a changed fingerprint, a missing outlet and a changed parameter default.

**Checkpoint**: `sbt protocol/test` green; six fixtures committed; the protocol directory is complete.

---

## Phase 3: User Story 1 — A streamlet in another language, run on a laptop (Priority: P1) 🎯 MVP

**Goal**: the sidecar, the Python SDK and the sample cart router, running from one compose file,
with fan-out, acknowledgement and recovery proven against real Kafka.

**Independent Test**: quickstart.md tier 5's laptop loop: fifty CloudEvents over ten cart ids,
the router killed after twenty and restarted, every event on exactly its outlet topic, each cart
in order on one partition, headers intact (`verify.py` exits 0). Automated equivalent: tier 3's
`InletGraphKafkaSuite` and `RestartKafkaSuite` with the Scala double.

### Sidecar

- [X] T020 [US1] Configure the `sidecar` project in `build.sbt` (`dependsOn(protocol)`, pekko-actor-typed, pekko-stream, pekko-connectors-kafka, kafka-clients pinned, logback, `JavaAppPackaging`, `DockerPlugin`, `name := "ankka-flow-sidecar"`, `Compile / mainClass := Some("com.thinkmorestupidless.ankka.flow.sidecar.Main")`, test deps munit, pekko testkit, testcontainers-kafka) and create `sidecar/.../sidecar/Settings.scala` reading the environment table in `contracts/sidecar.md` (property, then env, then default).
- [X] T021 [P] [US1] Create `sidecar/.../sidecar/StreamletConfig.scala` parsing `streamlet.conf` into `StreamletConfig`, `InletConfig`, `OutletConfig`, `BatchSettings` (data-model.md), refusing inlet/outlet names absent from the descriptor, and `sidecar/src/test/.../sidecar/StreamletConfigSuite.scala` over the example in `contracts/sidecar.md`.
- [X] T022 [P] [US1] Create `sidecar/.../sidecar/Descriptor.scala`: load `descriptor.json` from `FLOW_CONFIG_DIR` with `DescriptorJson`, validate with `DescriptorValidation`, expose `compare(discovered)`.
- [X] T023 [US1] Create `sidecar/.../sidecar/Discovery.scala`: `ManagedChannelBuilder.forAddress(...).usePlaintext()` to `FLOW_PROCESS_ADDRESS`; `Discover` with backoff 500 ms doubling to 10 s within `FLOW_DISCOVERY_TIMEOUT`, retried forever; version check, compare, validate; on problems `ReportError(Problems)` then return `Left(problems)`; log every attempt at `info`.
- [X] T024 [US1] Create `sidecar/.../sidecar/BatchProcessor.scala` with `InputBatch`, `EmittedRecord`, `Outcome` (`Acked(emits) | Failed(error)`) and `trait BatchProcessor { def process(batch: InputBatch): Future[Outcome] }` (the FR-018 seam), plus `DoubleProcessor` under `src/test` for graph tests without gRPC.
- [X] T025 [US1] Create `sidecar/.../sidecar/Conversation.scala`: opens one `Run` stream on a channel, sends `Start`, `send(batch): Future[Outcome]` correlating by `batch_id`, buffers `Emit`s until `Ack`, `revoke(batchId)` (drops later messages for it silently), detects every violation in `contracts/protocol.md` *Failing the stream* and completes `failed: Future[Throwable]` once, `stop()` sends `Stop` and half-closes; a `RemoteProcessor(conversation)` implementing `BatchProcessor`.
- [X] T026 [P] [US1] Create `sidecar/src/test/.../sidecar/ProcessDouble.scala`: a Scala process serving `Discovery` and `Streamlet` on an ephemeral loopback port with grpc-java, scriptable per input key (echo, fan, skip, fail, late, rogue-outlet, double-ack, emit-after-ack, unknown-batch, multiply, unkeyed, header-echo, silent), recording every message received, with `start()`, `stop()`, `restart()`, and a configurable `Spec` (so it can describe a different streamlet or protocol version).
- [X] T027 [US1] Write `sidecar/src/test/.../sidecar/ConversationSuite.scala` against the double: emits buffered until ack and delivered with key, headers (including a binary header) and value intact; one batch in flight per partition enforced by the caller and two partitions interleave; `Fail` completes `failed`; each violation (rogue outlet, double ack, emit after ack, unknown batch) completes `failed` once; a revoked batch's ack is dropped; `Stop` completes the stream; a 5 MiB single record is refused before sending, naming inlet, partition, offset.
- [X] T028 [P] [US1] Create `sidecar/.../sidecar/CommitAfterWrite.scala` carrying `sinkCommittingAfter` from the fork's `core/cloudflow-pekko/src/main/scala/cloudflow/pekkostream/PekkoStreamletLogic.scala` (lines ~277–326) as a standalone `Flow[(T, Committable)] → Sink` using `CommitterSettings.withCommitWhen(CommitWhen.OffsetFirstObserved)`, with its Lightbend header and ScalaDoc; add a `flow.mutation=commit-first` test-only switch that reorders commit before write (used by T049).
- [X] T029 [P] [US1] Create `sidecar/.../sidecar/Producers.scala`: one `SendProducer` per outlet from `OutletConfig` (client id, bootstrap, connection and producer config; `enable.idempotence=true` left at default), `send(EmittedRecord): Future[RecordMetadata]` building a `ProducerRecord` with the given key (or none), headers in order, value; `close()`.
- [X] T030 [US1] Create `sidecar/.../sidecar/InletGraph.scala`: per inlet `Consumer.committablePartitionedSource` (group, client id, `auto.offset.reset` from config, default `earliest`) → per partition sub-source: record-size guard, `groupedWeightedWithin(maxBytes, maxRecords, maxWait)` → `InputBatch` → `mapAsync(1)(processor.process)` → on `Acked` produce every emit and await all → `CommitAfterWrite` → commit; on `Failed` fail the graph; on sub-source completion revoke the in-flight batch; `DrainingControl`; expose `subscribed: Future[Unit]` (every partition assigned once).
- [X] T031 [P] [US1] Create `sidecar/.../sidecar/Probes.scala`: `ready()`/`notReady()` create and delete `${FLOW_STATE_DIR}/ready`; a scheduled touch of `alive` every second.
- [X] T032 [P] [US1] Create `sidecar/.../sidecar/Stalls.scala` (per (inlet, partition) oldest uncommitted batch time; `stalledSeconds`; one-shot warning when it first passes `FLOW_STALL_WARNING_AFTER`, cleared on commit) and `sidecar/.../sidecar/EventSink.scala` with `trait EventSink { def warning(reason, note) }` and `LogEventSink` (the Kubernetes sink is T089).
- [X] T033 [US1] Create `sidecar/.../sidecar/Supervisor.scala`: the conversation lifecycle state machine from data-model.md (Connecting → Verified → Running → Backoff → Connecting; Exit 1 on refusal): discovery, build `Conversation`, `Producers`, one `InletGraph` per inlet, `ready` when all subscribed, watch `Conversation.failed` and graph failures, on failure void in-flight, `notReady`, drain with commits disabled, close, back off 500 ms doubling to `FLOW_RECONNECT_MAX_BACKOFF`, repeat; `Stop` and drain on SIGTERM.
- [X] T034 [US1] Create `sidecar/.../sidecar/Main.scala` (load `Settings`, `Descriptor`, `StreamletConfig`; refuse on parse or validation errors; start `ActorSystem`, `Supervisor`; exit code from the supervisor), `sidecar/src/main/resources/application.conf` (pekko kafka committer defaults, dispatcher) and `logback.xml` (stdout, one line per event, no record values).
- [X] T035 [US1] Write `sidecar/src/test/.../sidecar/SupervisorSuite.scala` against the double with `DoubleProcessor`-free wiring but no Kafka (an in-memory `InletGraph` stub): discovery with protocol `99.0`, a different streamlet name, a duplicate port → `ReportError` received by the double, exit code 1, every problem named; `ready` absent until the double answers then present; a `Fail` → `ready` removed, a new `Start` with a new `conversation_id` after backoff; the stall warning logged once after the threshold.
- [X] T036 [P] [US1] Create `sidecar/src/test/.../sidecar/RecordKafkaSuite.scala` carrying the fork's `core/cloudflow-pekko-tests/src/test/scala/cloudflow/pekkostream/scaladsl/RecordKafkaSpec.scala` (10 keys × 5 records, `ce_type`/`ce_id`/binary `raw` headers, a relay adding `stage=relay`, header order, one partition per key, more than one partition used) as munit over `Producers` and a plain `KafkaConsumer`, with testcontainers `org.testcontainers.kafka.KafkaContainer("apache/kafka:3.9.1")`; keep the Lightbend header.
- [X] T037 [P] [US1] Create `sidecar/src/test/.../sidecar/SinkCommittingAfterKafkaSuite.scala` carrying `SinkCommittingAfterKafkaSpec` (20 records, the write fails at record 12, committed offset ≤ 12, nothing at or past 12 written, second run writes and commits all 20) as munit over `CommitAfterWrite`; header kept. Add the `mutationCheck` alias in `build.sbt`: runs this suite with `-Dflow.mutation=commit-first` and asserts it **fails** (SC-006).
- [X] T038 [US1] Write `sidecar/src/test/.../sidecar/InletGraphKafkaSuite.scala`: the full graph with `RemoteProcessor` and the double; fifty records over ten keys, the double routes odd/even cart ids to two outlets; the double is `stop()`ped after twenty acks and `restart()`ed; every record on exactly its outlet, each key in order on one partition, headers intact (S1.1–S1.4); then two graphs in one group on a 4-partition topic, one stopped mid-batch: the revoked batch is not committed by the stopped one and is read by the other (rebalance edge case). Records research.md items 3, 4 and 10 as answered.
- [X] T039 [US1] Write `sidecar/src/test/.../sidecar/RestartKafkaSuite.scala`: the double stopped for 5 s while records flow → `ready` gone within 2 s, in-flight batches voided, graph rebuilt with a new conversation id, no record lost and none committed twice past the boundary (edge cases: process restarted, sidecar restarted as a fresh `Supervisor` against the same group).
- [X] T040 [US1] Finish the sidecar image: `sidecar/image/` placeholder for `prometheus.yaml` (filled by T105), `Universal / mappings` for it, `dockerExposedPorts := Seq(2050)`, `bashScriptExtraDefines` for `FLOW_*` defaults; `sbt sidecar/docker:publishLocal` produces `ankka-flow-sidecar:<version>`.

### Python SDK

- [X] T041 [P] [US1] Create `sdks/python/pyproject.toml` (name `ankka-flow`, hatchling, `dynamic = ["version"]` from `src/ankka_flow/__init__.py`, deps `grpcio>=1.84,<2`, `protobuf>=6,<8`, dev group `grpcio-tools`, `pytest>=8`, `mypy>=1.13`, `types-protobuf`, `[project.scripts] descriptor = "ankka_flow._descriptor:main"`, `conformance = "ankka_flow._conformance:main"`, `[tool.uv] package = true`, `[tool.hatch.build] artifacts = ["src/ankka_flow/_proto/**"]`, pytest `testpaths`, mypy strict with `_proto` excluded), `sdks/python/scripts/proto.py` (copies `protocol/{src/main/protobuf,fixtures,DESCRIPTOR.md,README.md}` into `sdks/python/proto/`, runs `grpc_tools.protoc` with `--python_out --pyi_out --grpc_python_out` into `src/ankka_flow/_proto/`, rewrites imports to `ankka_flow._proto.ankka.flow.v1`), `src/ankka_flow/py.typed`, `sdks/python/README.md`; run `uv sync && uv run python scripts/proto.py` and commit `proto/`.
- [X] T042 [P] [US1] Create `sdks/python/src/ankka_flow/records.py` (`Record`, `Batch`, `Emit` frozen dataclasses), `ports.py` (`JsonInlet`, `JsonOutlet` with `contract` computed by `Base64(SHA-256(schema_name))`, `outlet.emit(record | value=, key=, headers=)`), `parameters.py` (`StringParameter`, `IntegerParameter`, `DoubleParameter`, `BooleanParameter`, `DurationParameter` → `timedelta`, `MemorySizeParameter` → `int`, each parsing HOCON-style duration and size strings; `Config` typed access).
- [X] T043 [US1] Create `sdks/python/src/ankka_flow/streamlet.py`: `Streamlet` base collecting ports and parameters from class attributes, `name`, `description`, `config: Config` set on `Start`, abstract `process(batch) -> Iterable[Emit]`; refuses at class creation a duplicate port name or parameter key.
- [X] T044 [US1] Create `sdks/python/src/ankka_flow/descriptor.py` (`spec_for(streamlet) -> Spec`; `write(spec) -> str` by `json.dumps(MessageToDict(..., preserving_proto_field_name=True), sort_keys=True, indent=2, ensure_ascii=False) + "\n"`) and `_descriptor.py` (`main`: resolve `FLOW_STREAMLET` or `[tool.ankka-flow] streamlet` from `pyproject.toml`, import, write `flow/descriptor.json`; `--check` exits 1 on difference; `--out` path).
- [X] T045 [US1] Write `sdks/python/tests/test_descriptor_fixtures.py`: declare each of the six fixture streamlets from `proto/fixtures/declarations/*.md` in Python (with `sdk.version` pinned to `0.0.0` in the test) and assert `write(spec_for(s))` equals the fixture bytes (FR-002, S5.2).
- [X] T046 [US1] Create `sdks/python/src/ankka_flow/server.py`: `grpc.server` with a `ThreadPoolExecutor`, bound to `127.0.0.1:${FLOW_PROCESS_PORT}` only; `Discovery` servicer answering `Discover` with the descriptor and logging `ReportError` problems at `error`; `Streamlet.Run` servicer: waits for `Start`, applies `config_json`, runs each `Batch` on a worker thread calling `process`, streams each `Emit` as yielded then `Ack`, or `Fail` on exception; refuses an emit to an undeclared outlet locally (raises → `Fail`); a second `Run` ends the first; `Stop` completes; `serve(streamlet)` blocking with SIGTERM handling; a test-only `ANKKA_FLOW_BREAK=ack-first` switch that sends `Ack` before emits (used by SC-005).
- [X] T047 [US1] Create `sdks/python/src/ankka_flow/json.py` (`loads(bytes)`, `dumps(obj) -> bytes`) and `__init__.py` exporting `Streamlet`, `JsonInlet`, `JsonOutlet`, the six parameters, `Record`, `Batch`, `Emit`, `serve`, `PROTOCOL_VERSION = "1.0"`, `__version__ = "0.0.0"`.
- [X] T048 [US1] Write `sdks/python/tests/test_server.py` with a scripted sidecar double in Python (a grpc client that opens `Run`, sends `Start` and `Batch`es): emits precede the ack with key, headers and value intact; an exception → `Fail`; nothing before `Start`; two partitions run concurrently (a `late` key on one does not delay the other); an undeclared outlet → `Fail`; a second `Run` supersedes the first; `Stop` completes; `ANKKA_FLOW_BREAK=ack-first` reverses the order. Records research.md item 7 as answered.
- [X] T049 [US1] Create `sdks/python/src/ankka_flow/testkit/__init__.py` with `Harness(streamlet, config=)`: `inlet(name).put(key=, value=, headers=)`, `run(partitions=)` building one batch per partition in order and applying the protocol's rules, `outlet(name).records`, `skipped`, `failures`; and `sdks/python/tests/test_harness.py` covering routing, skipping, an exception recorded as a failure, an undeclared outlet refused.
- [X] T050 [US1] Create the project template `sdks/python/template/` per `contracts/python-sdk.md`: `pyproject.toml`, `Dockerfile` (`python:3.12-slim`, uv, `CMD python -m {{module}}.main`, no `EXPOSE`), `blueprint.conf`, `docker-compose.yml` (kafka `apache/kafka:3.9.1` with 9092/9094 listeners; `sidecar` from `${FLOW_SIDECAR_IMAGE:-ghcr.io/thinkmorestupidless/ankka-flow-sidecar:{{version}}}`, `FLOW_PROCESS_ADDRESS=host.docker.internal:9010`, `extra_hosts`, `./flow:/etc/flow/config:ro`, `2050:2050`), `flow/streamlet.conf`, `src/{{module}}/{__init__,main,streamlet}.py` (an echo streamlet), `tests/test_streamlet.py`, `README.md` with the laptop loop.

### Sample

- [X] T051 [US1] Create `samples/cart-router/` from the template: `pyproject.toml` (`[tool.ankka-flow] streamlet = "cart_router.router:CartRouter"`, dev deps `kafka-python`), `src/cart_router/router.py` (the `CartRouter` of `contracts/python-sdk.md`: inlet `in`, outlets `valid` and `review`, `review-threshold`), `src/cart_router/main.py`, `tests/test_router.py` with the harness, `Dockerfile`; run `uv run descriptor` and commit `flow/descriptor.json`; assert it equals `protocol/fixtures/descriptors/cart-router.json`.
- [X] T052 [P] [US1] Write `samples/cart-router/blueprint.conf` (unmanaged `cart-events` with `topic.name = "shop.cart-events.v1"`, managed `valid-carts` and `review-carts` with 3 partitions), `samples/cart-router/flow/streamlet.conf` for the compose network (`kafka:9092`, groups `cart.router.in`, client ids), and `samples/cart-router/docker-compose.yml`.
- [X] T053 [P] [US1] Write `samples/cart-router/produce.py` (fifty CloudEvents over ten cart ids to `localhost:9094`, keyed by cart id, headers `ce_type`, `ce_id`, `ce_source`) and `samples/cart-router/verify.py` (reads both outlet topics from earliest; asserts every event on exactly the expected outlet by total, each cart's events in order on one partition, all three headers intact; exit 0/1).
- [X] T054 [US1] Run quickstart.md tier 5's laptop loop end to end (compose up, router, produce, kill after twenty, restart, verify) and fix whatever breaks; record the outcome in `samples/cart-router/README.md`.

**Checkpoint**: `sbt 'sidecar/test'` (tiers 2–3), `sbt mutationCheck`, `cd sdks/python && uv run mypy && uv run pytest`, and `verify.py` all green. This is the MVP.

---

## Phase 4: User Story 2 — A blueprint verified before it runs (Priority: P2)

**Goal**: the carried blueprint verification over descriptor files, the `AnkkaFlow` resource
model, and `flow verify` / `flow generate` with no language runtime.

**Independent Test**: quickstart.md tier 1: `sbt blueprint/test 'cli/testOnly *VerifySuite
*GenerateSuite' 'crd/testOnly *CrdSchemaSuite'`; a `cart-events.v1` outlet against a
`cart-events.v2` inlet is refused naming both, renaming one verifies, the emitted resource carries
every descriptor, image and mapping.

### Blueprint (carried from `core/cloudflow-blueprint`)

- [X] T055 [US2] Configure the `blueprint` project in `build.sbt` (`dependsOn(protocol)`, Typesafe Config at pekko's pinned version, munit) and create `blueprint/.../blueprint/BlueprintProblem.scala` carrying the sealed trait and `toMessage` from the fork's `core/cloudflow-blueprint/src/main/scala/cloudflow/blueprint/BlueprintProblem.scala` minus volume-mount and class-name problems, plus `UnsupportedFormat`, `FingerprintMismatch`, `UnmanagedTopicHasProducers`, `MissingImage`, `UnmanagedTopicWithoutBrokers`; Lightbend header kept.
- [X] T056 [P] [US2] Create `blueprint/.../blueprint/Topic.scala` carrying `Topic.scala` (name and cluster validation, producer/consumer port checks, `checkCompatibility` reduced to equal `format` and `fingerprint`, refusing any format but `json`; managed default name `<pipeline>.<id>`; unmanaged topics refuse producers); header kept.
- [X] T057 [P] [US2] Create `blueprint/.../blueprint/StreamletRef.scala` carrying `StreamletRef.verify` resolving a **descriptor name** against `Map[String, Spec]` (`StreamletDescriptorNotFound`, `InvalidStreamletName`); header kept.
- [X] T058 [P] [US2] Create `blueprint/.../blueprint/VerifiedBlueprint.scala` carrying `VerifiedBlueprint`, `VerifiedPortPath`, `VerifiedStreamlet`, `VerifiedTopic`, `VerifiedInlet`, `VerifiedOutlet` over `Spec`/`Port`; header kept.
- [X] T059 [US2] Create `blueprint/.../blueprint/Blueprint.scala` carrying `Blueprint.parseString`/`parseConfig`/`verify`/`verified`, the section and topic keys, `verifyPortsConnected` (unconnected inlets refuse; unconnected outlets are notes), `verifyPortsBoundToManyTopics`, `verifyNoDuplicateStreamletNames`, reading `blueprint.name`; header kept.
- [X] T060 [P] [US2] Create `blueprint/.../blueprint/TopicSettings.scala` (`TopicSettings`, `BatchSettings` with defaults 100 / 1 MiB / 100 ms, `fromConfig`, `merge(over)`) and `blueprint/.../blueprint/Overrides.scala` (`--conf` HOCON: `flow.topics.<id>` partial settings, `flow.streamlets.<name>.replicas` and `.config.<key>` typed by the descriptor's parameters; unknown names are problems; `apply(verified, overrides)`).
- [X] T061 [US2] Create test helpers `blueprint/src/test/.../blueprint/BlueprintBuilder.scala` and `DescriptorBuilder.scala` carrying the fork's builders adapted to `Spec` (headers kept), then `BlueprintSuite.scala` and `BlueprintParserSuite.scala` carrying every assertion of `BlueprintSpec` and `BlueprintParserSpec` that still applies (drop volume mounts, class names, Avro/Proto) as munit, plus cases for the five new problems.
- [X] T062 [P] [US2] Create `blueprint/src/test/.../blueprint/UnmanagedTopicSuite.scala` carrying `deployment/UnmanagedTopicSpec.scala` (a `managed = false` topic with `topic.name`, `bootstrap.servers`, `consumer-config` and consumers only verifies; its port mapping carries the name, brokers and config) and `JsonContractSuite.scala` carrying `JsonSchemaVerificationSpec` (same name connects, different name refuses, other format never); headers kept.
- [X] T063 [P] [US2] Write `blueprint/src/test/.../blueprint/OverridesSuite.scala`: topic partitions overridden, a streamlet parameter overridden and type-checked, unknown topic and unknown key refused, `replicas` applied.

### CRD

- [X] T064 [US2] Configure the `crd` project in `build.sbt` (fabric8 `kubernetes-client`, `jackson-module-scala`, no other dependency) and create `crd/.../crd/AnkkaFlow.scala` (`@Group("flow.ankka.thinkmorestupidless.com") @Version("v1alpha1") @Kind("AnkkaFlow") @Plural("ankkaflows") @ShortNames(Array("aflow"))`, `AnkkaFlowSpec`, `OnDelete`, `StreamletSpec` with `descriptor: JsonNode`, `TopicSpec`, `AnkkaFlowStatus`, `StreamletStatus`, `TopicStatus` per data-model.md, `NON_ABSENT`, `sameReport`) and `AnkkaFlowDefinition.scala` (`group`, `version`, `kind`, `plural`, `crdName`, `manifestResource = "/ankka-flow/crd/ankkaflow.yaml"`, `FlowSerialization` Jackson mapper with `DefaultScalaModule`).
- [X] T065 [US2] Write `kustomization/components/crd/ankkaflow.yaml` (the CRD with the full openAPI schema for spec and status, `x-kubernetes-preserve-unknown-fields` on `descriptor`, `subresources.status`, printer columns `PIPELINE`, `PHASE`, `READY`, `AGE`) and `kustomization/components/crd/kustomization.yaml` (`kind: Component`); create the symlink `crd/src/main/resources/ankka-flow/crd/ankkaflow.yaml → ../../../../../../kustomization/components/crd/ankkaflow.yaml`.
- [X] T066 [US2] Write `crd/src/test/.../crd/CrdSchemaSuite.scala`: every field of the spec and status case classes is declared in the YAML schema, and a round trip of the resource in `contracts/resource-and-operator.md` through `FlowSerialization` preserves the descriptor object.

### CLI

- [X] T067 [US2] Configure the `cli` project in `build.sbt` (`dependsOn(blueprint, crd, protocol)`, decline, fabric8 client, `JavaAppPackaging`, `executableScriptName := "flow"`, `Docker / publishLocal := {}`) and create `cli/.../cli/Main.scala` (decline `Command("flow", …)` composing `verify`, `generate`, `reset`, `version`; `run(args, out, err): Int` with exit codes 0/1/2) and `Version.scala` (`flow <BuildInfo.version>, protocol 1.0`).
- [X] T068 [P] [US2] Create `cli/.../cli/Descriptors.scala` (`load(dir): Either[Vector[String], Map[String, Spec]]` reading every `*.json` with `DescriptorJson` and `DescriptorValidation`, messages prefixed `descriptor <file>:`) and `Images.scala` (`--images` HOCON map and repeated `--image name=ref`; `MissingImage` for generate).
- [X] T069 [US2] Create `cli/.../cli/Verify.scala`: options per `contracts/cli.md`; load descriptors, parse blueprint, apply overrides, collect every problem in one pass, print notes for unconnected outlets, print `verified: n streamlets, m topics`; the message table in `contracts/cli.md` is the contract for wording.
- [X] T070 [US2] Create `cli/.../cli/ResourceWriter.scala` (`write(verified, overrides, images, pipeline, version, namespace): AnkkaFlow` — descriptors embedded as canonical JSON nodes, `inlets`/`outlets` port→topic id maps, topics with resolved settings and managed names, `replicas` default 1, `protocolVersion`) and `Generate.scala` (everything `Verify` does, then YAML to `-o` or stdout; `--pipeline` from `blueprint.name` or file name; `--version` from `git describe --tags --always --dirty` or `unversioned`).
- [X] T071 [US2] Create `cli/src/test/resources/blueprints/` with `cart/` (blueprint, three descriptors, `images.conf`, `overrides.conf`) and one blueprint per refusal row in `contracts/cli.md` (`contract-mismatch`, `unsupported-format`, `unconnected-inlet`, `unknown-port`, `unknown-streamlet`, `duplicate-streamlet`, `bound-twice`, `unmanaged-with-producers`, `unmanaged-no-brokers`, `bad-topic-name`, `missing-parameter`, `bad-parameter-type`, `unknown-override`, `hand-edited-fingerprint`).
- [X] T072 [US2] Write `cli/src/test/.../cli/VerifySuite.scala` (each blueprint above is refused with exactly its message and all problems come in one pass; `cart-events.v1` vs `.v2` refused naming both, renamed verifies; the cart blueprint verifies with the note for `review-carts`) and `GenerateSuite.scala` (the emitted resource parses as `AnkkaFlow`, carries every descriptor byte-equal to its file, every image, every binding, resolved partitions from `overrides.conf`, `replicas` 3 for the router, managed name `cart.valid-carts`, `unmanaged` name `shop.cart-events.v1`).

**Checkpoint**: tier 1 green; `sbt cli/stage` yields a `flow` that verifies `samples/cart-router/blueprint.conf` against `samples/cart-router/flow`.

---

## Phase 5: User Story 3 — Deployed like any other workload (Priority: P3)

**Goal**: the operator creates topics, renders two-container Deployments from the resource,
resolves Kafka settings through the cluster secret, reports status and Events, reconciles changes,
and is installable on kind.

**Independent Test**: quickstart.md tier 4's `FlowClusterSuite` (k3s): topics created, pods and
pipeline `Ready`, records from an unmanaged topic through the router to both outlets, `replicas: 3`
shares partitions, a changed parameter rolls only the router.

- [X] T073 [US3] Configure the `operator` project in `build.sbt` (`dependsOn(crd, blueprint, protocol)`, fabric8, kafka-clients, logback, `JavaAppPackaging`, `DockerPlugin`, `name := "ankka-flow-operator"`, main class, test deps munit, testcontainers `k3s` and `kafka`, fabric8 `kubernetes-server-mock`) and create `operator/.../operator/Settings.scala` per `contracts/resource-and-operator.md` (property → env → default; `sidecarImage: Option[String]`).
- [X] T074 [P] [US3] Create `operator/.../operator/Labels.scala` (`flow.ankka.thinkmorestupidless.com/pipeline`, `/streamlet`, `/generation`, `/config-hash`, `/kafka-cluster`, `/reset-offsets`, `/reset-offsets-done`, `app.kubernetes.io/managed-by = ankka-flow`, `app.kubernetes.io/name`) and `Names.scala` (`flow-<pipeline>-<streamlet>`, `flow-<pipeline>`, `kafka-cluster-<name>`, group and client ids).
- [X] T075 [P] [US3] Create `operator/.../operator/Action.scala`: `enum Action` with `EnsureTopic(ResolvedTopic)`, `EnsureSecret(Secret)`, `EnsureServiceAccount`, `EnsureRole`, `EnsureRoleBinding`, `ApplyDeployment(Deployment)`, `DeleteDeployment(ns, name)`, `DeleteSecret(ns, name)`, `ResetGroup(ResetTarget)`, `MarkResetDone(id)`, `RecordEvent(reason, eventType, note)`, `SetStatus(AnkkaFlowStatus)`, `NoAction`, each with `describe`.
- [X] T076 [P] [US3] Create `operator/.../operator/TopicResolution.scala`: `KafkaCluster` from a `kafka-cluster-<name>` Secret; `resolve(topicSpec, clusters): Either[String, ResolvedTopic]` (resource settings win, then the named or `default` cluster; a managed topic with no partitions or replicas after that is a refusal; a cluster named but absent is a refusal); carries the create-managed/skip-unmanaged logic of the fork's `core/cloudflow-operator/src/main/scala/cloudflow/operator/action/TopicActions.scala` with its header.
- [X] T077 [P] [US3] Create `operator/.../operator/StreamletFiles.scala`: render `descriptor.json` (the embedded node through `DescriptorJson`) and `streamlet.conf` (`contracts/sidecar.md` shape from the resolved topics, bindings, config, batching) for one streamlet; `hash = SHA-256 hex of both`.
- [X] T078 [US3] Create `operator/.../operator/Rendering.scala`: pure `render(resource, settings, observed): Either[Vector[String], Vector[Action]]` implementing steps 1–3 and 5 of `contracts/resource-and-operator.md` *Reconcile*: refusals (no sidecar image, cluster missing, unresolved managed topic, invalid descriptor, bindings not matching ports) → `SetStatus(Failed)` + `RecordEvent` per problem; `EnsureTopic` per managed topic and `TopicDiffers`/`TopicSettingsIgnored`/`TopicMissing` events from `observed`; per streamlet the Secret, SA/Role/RoleBinding (events create), and the Deployment with exactly the container table (sidecar env, mount, `metrics` port, exec probes, `preStop`; process with `FLOW_PROCESS_PORT` only), `RollingUpdate` 1/0, labels, config-hash and prometheus annotations, owner reference; `DeleteDeployment`/`DeleteSecret` for labelled Deployments not in the spec with `StreamletRemoved`; `StreamletRolled` when the hash changed; status via `LifecycleRules`.
- [X] T079 [P] [US3] Create `operator/.../operator/LifecycleRules.scala` (`phase(resource, observed): AnkkaFlowStatus` — `Ready` iff every streamlet ready == desired and every topic exists; `Pending` while rolling; `Degraded` otherwise; `Failed` on refusal; `observedGeneration`, `lastTransitionTime` only on phase change) and `Events.scala` (build an `events.k8s.io/v1` `Event` regarding the `AnkkaFlow` with `reportingController flow.ankka.thinkmorestupidless.com/operator`, the operator pod name, the reason table).
- [X] T080 [US3] Create `operator/.../operator/Executor.scala` (`trait Executor { execute(Action); observe(PipelineRef): Observed }`) and `Fabric8Executor.scala` (server-side apply with field manager `ankka-flow-operator` and `forceConflicts`; `editStatus` skipped when `sameReport`; Events created; `observe` reads Deployments and pod counts by label and the cluster Secrets from `FLOW_KAFKA_CLUSTERS_NAMESPACE`).
- [X] T081 [US3] Create `operator/.../operator/KafkaExecutor.scala`: `Admin` cache keyed by bootstrap servers + connection config (Cloudflow's `KafkaAdmins`, header kept); `EnsureTopic` creates with `NewTopic(name, partitions, replicas).configs(topicConfig)` when absent and describes when present, returning the difference for `observed.topics`; unmanaged topics are described only.
- [X] T082 [US3] Create `operator/.../operator/WorkQueue.scala` (`PipelineRef`, backoff from `Settings`), `Operator.scala` (informers on `AnkkaFlow` in any namespace and on Deployments labelled `managed-by: ankka-flow`, enqueueing refs), `PipelineReconciler.scala` (observe → render → execute → status; refusals never throw), `Main.scala` (client with `FlowSerialization`, `Operator.start()`, `awaitTermination`), `operator/src/main/resources/logback.xml`.
- [X] T083 [US3] Add `KubernetesEventSink` to `sidecar/.../sidecar/EventSink.scala`: when `KUBERNETES_SERVICE_HOST` is set, `POST /apis/events.k8s.io/v1/namespaces/<ns>/events` with the mounted token and CA over `java.net.http`, `regarding` the pod (namespace and name from the downward-API env `FLOW_POD_NAME`/`FLOW_POD_NAMESPACE`), reason `PartitionStalled`, type `Warning`; falls back to the log sink on any error. Add the two downward-API env vars to `Rendering` (T078). Record research.md item 9.
- [X] T084 [US3] Write `kustomization/components/operator/operator.yaml` (Namespace `ankka-flow`, ServiceAccount, ClusterRole and binding per `contracts/resource-and-operator.md` *RBAC*, Deployment `ankka-flow-operator:latest` with `FLOW_SIDECAR_IMAGE`, `FLOW_KAFKA_CLUSTERS_NAMESPACE`) and `kustomization.yaml`; symlink `operator/src/main/resources/ankka-flow/install/operator.yaml` into it; operator image settings in `build.sbt`.
- [X] T085 [P] [US3] Write `kustomization/components/kafka/` (a one-node `apache/kafka:3.9.1` KRaft StatefulSet and Service `kafka.kafka.svc:9092` in namespace `kafka`; the `kafka-cluster-default` Secret in `ankka-flow` pointing at it), `kustomization/overlays/local/kustomization.yaml` (crd, operator, kafka; `images:` pinning `ankka-flow-operator` and `FLOW_SIDECAR_IMAGE` via a ConfigMap replacement), and `kustomization/kind.yaml`.
- [X] T086 [P] [US3] Write `kustomization/deploy-local.sh` (`set -euo pipefail`; refuse any context but `kind-${ANKKA_FLOW_KIND_CLUSTER:-ankka}`; `sbt -batch docker:publishLocal`; `kind load docker-image` for `ankka-flow-operator`, `ankka-flow-sidecar`, `sample-cart-router`; apply the CRD; `kubectl kustomize overlays/local | kubectl apply --server-side --force-conflicts`; rollout wait) and fill the `Justfile` cluster recipes (`cluster := env_var_or_default("ANKKA_FLOW_KIND_CLUSTER", "ankka")`).
- [X] T087 [US3] Write `operator/src/test/.../operator/RenderingSuite.scala`: every row of the container table; the process container has no ports, probes, mounts or env but `FLOW_PROCESS_PORT`; no sidecar image → `Failed`, one `Refused` event, no other action (S3.6); config hash changes with a parameter change and not otherwise; a streamlet removed from the spec → `DeleteDeployment` + `StreamletRemoved`; a topic present with other partitions → `TopicDiffers` and no `EnsureTopic` change; `TopicSettingsIgnored`; a missing unmanaged topic → `TopicMissing` and `Degraded`; the cluster precedence (S3.4).
- [X] T088 [P] [US3] Write `operator/src/test/.../operator/TopicResolutionSuite.scala` carrying `TopicActionsSpec`'s cases (fresh deploy creates every managed topic; no topic the platform does not own is created and the managed ones still are; a named cluster supplies partitions) as munit over `resolve` and `render`, header kept; and `LifecycleRulesSuite.scala` + `StreamletFilesSuite.scala` (phase table; `streamlet.conf` parses back with `StreamletConfig` from the sidecar module — add `sidecar % "test->compile"` to the operator's test dependencies).
- [X] T089 [US3] Add to `build.sbt` a `sampleImage` task that runs `docker build -t sample-cart-router:<Docker/version> samples/cart-router` (dependency of `operator / Test / test` unless `-Dflow.cluster.tests=off`) and create `operator/src/test/.../operator/ClusterImages.scala` (import `docker save` tars into k3s via `ctr`, as ankka's).
- [X] T090 [US3] Write `operator/src/test/.../operator/FlowClusterSuite.scala` (k3s `rancher/k3s:v1.35.1-k3s1`, `munitIgnore` on `-Dflow.cluster.tests=off`): apply the Kafka manifests and cluster Secret; install the CRD from the classpath; run the operator in-process; apply the cart resource (generated by `ResourceWriter` from the sample's blueprint and descriptor with the imported images, `BuildInfo.version` tag); managed topics exist with 3 partitions, the unmanaged one is created by the test producer only (S3.1); pods `Ready` with two containers (S3.2); pipeline `Ready` (S3.3); fifty records to the unmanaged topic → both outlet topics fill; `replicas: 3` → three pods and the group has three members (S3.5); a changed `review-threshold` rolls only the router (S3.7, new pod-template hash, unchanged elsewhere); a pre-existing topic with 5 partitions → `TopicDiffers` event; events listed via `regarding.kind=AnkkaFlow`. Records research.md item 8.
- [X] T091 [US3] Write `samples/cart-router/README.md` section *On a kind cluster* (quickstart tier 6 commands) and `samples/cart-router/k8s/kafka-cluster-shop.yaml` (an example unmanaged-cluster Secret).

**Checkpoint**: `sbt operator/test` including `FlowClusterSuite` green; `just up` installs on kind.

---

## Phase 6: User Story 4 — Rebuilt from the start, and watched (Priority: P4)

**Goal**: consumer-group reset through the resource, and per-inlet lag in Prometheus under the
streamlet's name.

**Independent Test**: quickstart.md tier 4's reset cases and tier 3's `ConsumerLagKafkaSuite`:
after scale to zero and a reset every event is delivered again with one event per group; a reset
with pods running is refused; `records_lag` is scraped with `client_id = cart.router.in`.

- [X] T092 [US4] Create `crd/.../crd/ResetRequest.scala` carrying the fork's `core/cloudflow-crd/src/main/scala/cloudflow/crd/ResetOffsets.scala` (`Request(id, streamlets)`, `includes`, JSON round trip, `request`/`done`/`pending` over the `flow.ankka.thinkmorestupidless.com/reset-offsets` and `…-done` annotations, `groupId(pipeline, streamlet, inlet)`), header kept, and `crd/src/test/.../crd/ResetRequestSuite.scala` carrying `ResetOffsetsSpec`'s pure cases (round trip, pending/done, group name, target selection).
- [X] T093 [P] [US4] Create `operator/.../operator/ConsumerGroupReset.scala` carrying `ConsumerGroupReset.toEarliest(admin, groupId, topic): Future[Int]` and `GroupHasActiveMembers` from the fork (inline `KafkaFutureConverter`), header kept, and `operator/src/test/.../operator/ConsumerGroupResetSuite.scala` carrying `ConsumerGroupResetSpec` (moves a group to the start; refuses a group with an active member; a group that never committed reports its partitions) as munit with testcontainers Kafka.
- [X] T094 [US4] Extend `operator/.../operator/Rendering.scala` with step 4 of *Reconcile*: a pending request → `ResetRefused` event when any target's `replicas != 0` or pods remain (no marker); otherwise `ResetGroup(ResetTarget)` per target inlet and `MarkResetDone(id)`; targets default to every streamlet with an inlet. Extend `RenderingSuite` (S4.2 refusal, targets, marker only after actions).
- [X] T095 [US4] Extend `operator/.../operator/KafkaExecutor.scala` to execute `ResetGroup` over the target's resolved connection, producing `RecordEvent(ResetOffsets, Normal, "<group>: <n> partitions to earliest")` or `RecordEvent(ResetOffsetsFailed, Warning, <Kafka's message>)` (a `GroupHasActiveMembers` is a warning, never a pipeline failure), and `Fabric8Executor` to execute `MarkResetDone` by patching the done annotation.
- [X] T096 [P] [US4] Create `cli/.../cli/ResetGuards.scala` (carrying the guards of the fork's `core/cloudflow-cli/src/main/scala/cloudflow/cli/execution/ResetOffsetsExecution.scala`: named streamlet exists and has inlets; every target `replicas == 0` with the message in `contracts/cli.md`; no pods remain) and `Reset.scala` (read the `AnkkaFlow` and pods by label with fabric8, apply guards, patch the request annotation with a UUID, print the id).
- [X] T097 [P] [US4] Write `cli/src/test/.../cli/CliResetSuite.scala` against fabric8's `KubernetesMockServer` carrying `CliWorkflowSpec` lines 380–445 (request written; not scaled to 0 refused; pods remaining refused; unknown streamlet refused; no-inlet streamlet refused).
- [X] T098 [P] [US4] Create `sidecar/image/prometheus.yaml` carrying the fork's `core/cloudflow-sbt-plugin/src/main/resources/runtimes/pekko/prometheus.yaml` (records-lag-max before records-lag, records-consumed-rate, record-send-rate; `lowercaseOutputName`, `attrNameSnakeCase`) plus two rules for `ankka.flow:type=sidecar,inlet=*,partition=*` → `ankka_flow_sidecar_in_flight` and `ankka_flow_sidecar_stalled_seconds`; and `sidecar/src/test/.../sidecar/PrometheusRulesSuite.scala` carrying `PrometheusRulesSpec` (synthetic JMX names match; `records-lag-max` hits its own rule first; the two new rules match).
- [X] T099 [US4] Create `sidecar/.../sidecar/Metrics.scala`: register one `DynamicMBean` per (inlet, partition) under `ankka.flow:type=sidecar,inlet=<i>,partition=<p>` with attributes `InFlight` and `StalledSeconds` fed by `Stalls`; unregister on revocation.
- [X] T100 [US4] Finish the sidecar image for metrics in `build.sbt`: fetch `jmx_prometheus_javaagent` (current 1.x release; record the version in research.md item 5) into `Universal / mappings` at `/opt/flow/jmx_prometheus_javaagent.jar` with `prometheus.yaml` at `/opt/flow/prometheus.yaml`; `bashScriptExtraDefines` adds `-javaagent:/opt/flow/jmx_prometheus_javaagent.jar=${FLOW_METRICS_PORT:-2050}:/opt/flow/prometheus.yaml`.
- [X] T101 [P] [US4] Create `sidecar/src/test/.../sidecar/ConsumerLagKafkaSuite.scala` carrying `ConsumerLagKafkaSpec` (a generator and a reader through `InletGraph`; platform MBeans show producer metrics for `<pipeline>.<streamlet>.<outlet>` and consumer `records-lag` for `<pipeline>.<streamlet>.<inlet>` non-empty with max 0.0 after catch-up), header kept.
- [X] T102 [US4] Extend `operator/src/test/.../operator/FlowClusterSuite.scala`: `replicas: 0` for the router → no pods; `flow reset`'s annotation applied → one `ResetOffsets` event per group, done marker set, groups at earliest; `replicas: 1` → every record delivered again (S4.1, SC-004); a reset requested with pods running → `ResetRefused`, no marker (S4.2); the in-process operator restarted → no second reset (S4.3); `curl` the router pod's `:2050/metrics` → `kafka_consumer_consumer_fetch_manager_metrics_records_lag{client_id="cart.router.in",…}` present (S4.4).

**Checkpoint**: tiers 3 and 4 fully green including reset and lag; `flow reset` works against the kind cluster.

---

## Phase 7: User Story 5 — A second SDK can prove itself (Priority: P5)

**Goal**: a conformance suite that runs against the Scala reference in-process and against any
process on a port, with every conversation named, plus the fixtures and CI checks a third SDK
needs.

**Independent Test**: quickstart.md tier 5: `sbt 'sidecar/testOnly *ConformanceSuite'` passes
in-process; `uv run conformance` passes for Python; `ANKKA_FLOW_BREAK=ack-first uv run
conformance` fails exactly `run.emits-precede-ack`.

- [X] T103 [US5] Write `protocol/fixtures/conversations/*.json` (one per `run.*` case in `contracts/conformance.md`: the `Start` config, batches with base64 records and their keys) and confirm the `conformance` descriptor fixture from T018 matches the reference declaration; document the layout in `protocol/README.md`.
- [X] T104 [US5] Create `sidecar/src/test/.../sidecar/conformance/ConformanceReference.scala`: the Scala reference streamlet (`in`, `side`, `out`, `other`; `mode`, `factor`) implementing the key-driven behaviours of `contracts/conformance.md` over grpc-java on an ephemeral loopback port (reusing `ProcessDouble`'s server plumbing).
- [X] T105 [US5] Create `sidecar/src/test/.../sidecar/conformance/ConformanceTarget.scala` (`InProcess` starts the reference; `Sidecar(address)` dials `-Dflow.conformance.target`; `name`) and `ConformanceSuite.scala`: one munit test per row of *Conversations, by name* using `Conversation`/`RemoteProcessor` fed from the conversation fixtures; `violation.*` and `version.*` run against `ProcessDouble` and print `skipped (double-only)` for an external target; prints `conformance target: …`.
- [X] T106 [P] [US5] Create the Python reference in `sdks/python/src/ankka_flow/_conformance.py` (`reference_streamlet()` with the same declaration and key-driven behaviours; `main` serves it on `127.0.0.1:${FLOW_PROCESS_PORT}` in a thread, then runs `sbt -Dflow.conformance.target=127.0.0.1:<port> -Dflow.cluster.tests=off "sidecar/testOnly *ConformanceSuite"` from the repository root, forwarding `ANKKA_FLOW_CONFORMANCE_ONLY` as a munit filter) and `sdks/python/tests/test_conformance_reference.py` (the reference's behaviours through the `Harness`).
- [X] T107 [US5] Run `uv run conformance` and fix the Python SDK until every case passes; run with `ANKKA_FLOW_BREAK=ack-first` and confirm exactly `run.emits-precede-ack` fails (SC-005); record both in `sdks/python/README.md`.
- [X] T108 [P] [US5] Fill the `sdk-python` job in `.github/workflows/ci.yml`: `diff -r protocol/src/main/protobuf sdks/python/proto/src/main/protobuf`, `diff -r protocol/fixtures sdks/python/proto/fixtures`, `diff protocol/DESCRIPTOR.md sdks/python/proto/DESCRIPTOR.md` (FR-027); then `uv sync`, `uv run python scripts/proto.py`, `uv build` with an isolated wheel import smoke test, `uv run mypy`, `uv run pytest -q`, `uv run conformance`; and in `samples/cart-router`: `uv sync`, `uv run descriptor --check`, `uv run pytest -q`.
- [X] T109 [P] [US5] Write `docs/contributing/language-sdks.md`: what an SDK must implement (the two services, the descriptor writer, the fixtures), how to copy `protocol/`, how to run the suite against a port, how to read a failing case name, the `double-only` cases and why.

**Checkpoint**: the suite passes for Scala and Python; CI proves the protocol copy is identical.

---

## Phase 8: Polish & Cross-Cutting Concerns

- [X] T110 [P] Write `docs/concepts/pipelines.md` (streamlets, contracts, blueprints, topics, where ankka ends and ankka-flow begins), `docs/concepts/sidecar.md` (the pod, the conversation, commit after write, failure and redelivery, resets) and `docs/concepts/contracts.md` (JSON by schema name, fingerprints, why the sidecar never decodes).
- [X] T111 [P] Write `docs/reference/protocol.md` (from `contracts/protocol.md`), `docs/reference/cli.md`, `docs/reference/resource.md` (the `AnkkaFlow` fields, the cluster Secret, events, reset annotations), `docs/reference/sidecar.md` (env, files, probes, metrics), `docs/reference/limitations.md` (at-least-once only; JSON only; no built-in stages; no Scala or TypeScript SDK; one Kafka cluster type; no exactly-once; no UI; a JVM CLI), and `docs/sdk/python.md` (from `contracts/python-sdk.md`).
- [X] T112 [P] Rewrite `README.md` as a landing page: what it is, the laptop loop in five commands, links into `docs/` and `specs/001-version-one/`.
- [X] T113 Write `samples/cart-router/bench.py` (produce N records over P partitions at max rate; measure records/s per partition through the router to `valid-carts`, and per-record latency (produce timestamp → outlet record timestamp) compared with a plain consumer reading the same input; print a table; exit 0 iff ≥ 1,000/s per partition and < 10 ms added median), run it on this laptop from the compose file, and record the numbers with machine, record size and partition count in `research.md` *Measurements at the end* (SC-008).
- [X] T114 [P] Create `.github/workflows/release.yml`: on a `v*` tag, `sbt docker:publishLocal`, tag and push `ghcr.io/thinkmorestupidless/ankka-flow-{sidecar,operator}` and `sample-cart-router`, and run the `sdk-python` steps; no PyPI publish.
- [X] T115 Fill the `docs` job in `.github/workflows/ci.yml` (a link check over `docs/` and `README.md`) and the `Justfile` `docs` recipe.
- [X] T116 Answer every open item in `research.md` *Verify at implementation* (1–11) with what the tasks found, including item 6 (whether ankka's shopping-cart sample produces to Kafka; if not, name the stand-in used in quickstart tier 7), and update `NOTICE` with the final list of carried files.
- [X] T117 Run quickstart.md tiers 1–5 from a clean checkout and the reviewer's checklist (every carried file has its header and munit test; no pekko-http or pekko-grpc in the build; `ss -ltn` in the sidecar container shows only 2050; the process container has no ports, probes, mounts or secrets; nothing names the sidecar image; `flow verify` runs offline; fixtures match from both languages; the design doc is current); fix anything found.
- [X] T118 Run quickstart tiers 6 and 7 on the kind cluster beside ankka and record the result in `samples/cart-router/README.md` and `research.md` (SC-003's manual form). **Tier 6 passed; tier 7 not run**: ankka's operator configures no Kafka for services, so no deployed ankka service can publish (see research.md, *Quickstart tiers 6 and 7*).

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: no dependencies; T002–T008 run in parallel after T001.
- **Foundational (Phase 2)**: needs T001 and T012's project settings; T009–T011 before T012; T013–T017 in parallel after T012; T018–T019 after T015–T016. **Blocks every story.**
- **US1 (Phase 3)**: needs Phase 2. The sidecar (T020–T040) and the Python SDK (T041–T050) are independent of each other until the sample (T051–T054) joins them.
- **US2 (Phase 4)**: needs Phase 2 only. Can run in parallel with US1.
- **US3 (Phase 5)**: needs US2 (`crd`, `blueprint` for `TopicSettings`, `ResourceWriter` for the k3s suite) and US1's sidecar image and sample image (T040, T051) for `FlowClusterSuite`; rendering and its pure suites (T073–T088) need only US2.
- **US4 (Phase 6)**: needs US3 (reconciler, k3s suite) and US1 (the sidecar's `Stalls`, image).
- **US5 (Phase 7)**: needs US1 (`Conversation`, `ProcessDouble`, the Python SDK). Independent of US2–US4.
- **Polish (Phase 8)**: after the stories it documents; T113 needs US1; T118 needs US3 and US4.

### Within US1

T020 → T021–T022 [P] → T023 → T024 → T025 → T026 [P] → T027 → T028–T029 [P] → T030 → T031–T032 [P]
→ T033 → T034 → T035 → T036–T037 [P] → T038 → T039 → T040. Python: T041–T042 [P] → T043 → T044 →
T045 → T046 → T047 → T048 → T049 → T050. Sample: T051 → T052–T053 [P] → T054.

### Parallel Opportunities

- Phase 1: T002–T008 together.
- Phase 2: T010–T011 with T009; T013, T014, T017 together after T012.
- US1: the whole Python track (T041–T050) beside the whole sidecar track (T020–T040); within the sidecar, T021/T022, T028/T029, T031/T032, T036/T037 pairs.
- US2: T056–T058 together; T060, T062, T063 beside T061; T068 beside T067.
- US3: T074–T077 together; T079 beside T078; T085–T086 beside T084; T088 beside T087.
- US4: T093, T096–T098, T101 together with T092.
- US5: T106, T108, T109 beside T104–T105.
- US1 and US2 as two tracks after Phase 2; US5 beside US3/US4 once US1 is done.

---

## Parallel Example: after Phase 2

```bash
# Track A — the sidecar (US1)
Task: "T020 Configure the sidecar project and Settings.scala"
Task: "T026 ProcessDouble.scala"                     # in parallel with T021–T025

# Track B — the Python SDK (US1)
Task: "T041 pyproject.toml, scripts/proto.py, proto/ copy"
Task: "T042 records.py, ports.py, parameters.py"

# Track C — the blueprint module (US2)
Task: "T055 BlueprintProblem.scala"
Task: "T056 Topic.scala"  Task: "T057 StreamletRef.scala"  Task: "T058 VerifiedBlueprint.scala"
```

---

## Implementation Strategy

### MVP first (User Story 1)

1. Phase 1, Phase 2.
2. Phase 3 in two tracks; join at the sample.
3. **Stop and validate**: `sbt sidecar/test mutationCheck`, `uv run mypy && uv run pytest`,
   quickstart tier 5's `verify.py`. If one streamlet in Python can read, fan out, acknowledge and
   recover through the sidecar, the protocol, the sidecar and the SDK exist and agree.

### Incremental delivery

1. US1 → a streamlet runs on a laptop (MVP).
2. US2 → a pipeline is verified and a resource emitted with no cluster.
3. US3 → it deploys on kind like any workload.
4. US4 → it can be rebuilt and its lag watched.
5. US5 → a third SDK has a definition of correct.
6. Polish → documented, benchmarked, released.

### Parallel team strategy

Two people: one on the sidecar track, one on the Python track, both from Phase 2; the second
picks up US2 while the first finishes the Kafka suites; US3 and US4 follow on the sidecar side
while US5 follows on the SDK side.

---

## Notes

- Every carried file keeps its Lightbend header and comes with its test rewritten as munit; the
  source path in the fork is named in the task so the copy is traceable (SC-007).
- Nothing in any task adds pekko-http, pekko-grpc, Avro, spray-json or ScalaTest.
- `-Dflow.cluster.tests=off` skips the k3s suite and the sample image build; Docker is otherwise
  required, as in ankka.
- Commit after each task or logical group; stop at any checkpoint to validate the story alone.
