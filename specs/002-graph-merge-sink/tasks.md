# Tasks: a graph merge sink built into the sidecar

**Input**: Design documents from `/specs/002-graph-merge-sink/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/, quickstart.md

**Tests**: included. The spec requires them: the sink must be tested against a real graph database
including redelivery, reordering, restart with a batch in flight and a full replay (FR-031), the
built-in descriptor's canonical JSON is a committed fixture (R4), and each story's independent test
is a suite in quickstart.md. Tests are written before or with the code they prove, never after.

**Organization**: by user story. US1 (the merge is right however the deltas arrive) is the stage
inside the sidecar and is the MVP; US2 makes it declarable and deployable; US3 makes it watchable;
US4 is the sample beside ankka.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: can run in parallel (different files, no dependency on an unfinished task)
- **[Story]**: US1–US4 from spec.md
- Paths are repository-relative. `.../` abbreviates `src/main/scala/com/thinkmorestupidless/ankka/flow/`
  (or `src/test/scala/...`) under the module named.

## Path Conventions

The six sbt projects at the root, the Python SDK at `sdks/python/`, samples at `samples/`,
installation at `kustomization/`, documentation at `docs/` and the curated skills at
`tools/docs/skill/`. See plan.md *Project Structure*.

---

## Phase 1: Setup (dependencies and the rule that changes)

**Purpose**: the one new dependency, the test image switch, and the rule this feature narrows,
so that everything after it builds and is honest about what it does.

- [ ] T001 Add to `project/Dependencies.scala`: `V.neo4jDriver = "5.28.5"`, `V.neo4jImage = "neo4j:5.26-community"`, `val neo4jDriver = "org.neo4j.driver" % "neo4j-java-driver" % V.neo4jDriver`, `val testcontainersNeo4j = "org.testcontainers" % "neo4j" % V.testcontainers`; in `build.sbt` add `neo4jDriver` and `testcontainersNeo4j % Test` to `sidecar`, `testcontainersNeo4j % Test` to `operator`, and `Test / javaOptions += s"-Dflow.neo4j.image=${V.neo4jImage}"` in `commonSettings` beside the Kafka line. Run `sbt sidecar/evicted sidecar/docker:publishLocal` and record in research.md *Verify at implementation* item 1 whether the driver's Netty is shaded and that nothing clashes with `grpc-netty-shaded`.
- [ ] T002 [P] Narrow the rule in `CLAUDE.md`'s table: "The sidecar never decodes" → "The sidecar never decodes for a process. A record's value is bytes from Kafka to the process and back. A built-in stage decodes its own contract and nothing else." Add `flow.neo4j.image` to the note under *Commands* that lists test switches, and a line under *Rules* that a built-in stage's descriptor is a value in `protocol/.../Builtins.scala` and its canonical JSON a fixture under `protocol/fixtures/builtin/`.
- [ ] T003 [P] Add the `neo4j` module to the sidecar's test image imports: in `sidecar/src/test/.../sidecar/Neo4jSuite.scala` a trait over `munit.FunSuite` that starts one `org.testcontainers.containers.Neo4jContainer` from `sys.props.getOrElse("flow.neo4j.image", "neo4j:5.26-community")` with `withAdminPassword("flow-test")` in `beforeAll`, exposes `boltUri`, `driver` (a `org.neo4j.driver.Driver`), `pause()`/`unpause()` through `container.getDockerClient`, `clear()` (`MATCH (n) DETACH DELETE n` plus dropping the `element_id` constraint), and `query(cypher, params)` returning records; `munitTimeout` 3 minutes. Verify research item 6 here.

**Checkpoint**: `sbt sidecar/compile` green with the driver; `sbt 'sidecar/testOnly *Neo4jSuite'` (an empty suite using the trait) starts and stops a Neo4j container.

---

## Phase 2: Foundational (the built-in descriptor and the resource field)

**Purpose**: the descriptor every module reads, its lookup in a blueprint, and the resource field
the operator keys on. US1 needs the descriptor (the sidecar refuses any other); US2 needs all three.

**⚠️ CRITICAL**: no user story work begins until this phase compiles and its fixtures pass.

- [ ] T004 Create `protocol/.../protocol/Builtins.scala`: `object Builtins` with `val neo4jMergeSink: Spec` — `protocol_version` `ProtocolVersion.Current`, `sdk = SdkInfo("ankka-flow-sidecar", "0.0.0")`, streamlet `neo4j-merge-sink`, description `Merges graph deltas into Neo4j in one transaction per batch.`, inlet `in` with contract `json` / `ankka.graph-delta.v1` / `Fingerprint.fingerprint("ankka.graph-delta.v1")`, no outlets, parameters `secret` (STRING, no default, description `The Secret in the pipeline's namespace holding uri, username, password and optionally database.`) and `transaction-timeout` (DURATION, default `30s`) — `val all: Vector[Spec]`, `def byName(name): Option[Spec]`, and `val Prefix = "builtin/"`.
- [ ] T005 Create `protocol/src/test/.../protocol/BuiltinsSuite.scala` on the `DescriptorFixturesSuite` pattern: for each of `Builtins.all`, `DescriptorValidation.validate` is empty and `DescriptorJson.write(spec)` equals `protocol/fixtures/builtin/<name>.json` byte for byte, rewriting the file under `-Dflow.fixtures.regenerate=on`. Generate `protocol/fixtures/builtin/neo4j-merge-sink.json` with the switch and commit it. Run `cd sdks/python && uv run python scripts/proto.py` so `sdks/python/proto/fixtures/builtin/` exists, and confirm `uv run pytest -q` still passes (the Python fixture glob must not see the new directory).
- [ ] T006 [P] In `blueprint/.../blueprint/Descriptors.scala` add `builtin: Boolean = false` to `StreamletDescriptor` and a `ref: String` (`Builtins.Prefix + name` when built in, else `name`); in `StreamletRef.scala` resolve by `_.ref == descriptorName`; when `descriptorName` starts with `builtin/` and nothing matches, produce the new `BlueprintProblem.UnknownBuiltin(streamlet, name, known: Vector[String])` with message `Streamlet '<s>' names built-in descriptor 'builtin/<x>', which this version does not have; the built-ins are: <a, b>.` in `BlueprintProblem.scala`; also add `BuiltinHasImage(streamlet)`: `Streamlet '<s>' is built in and takes no image.` Add `blueprint/src/test/.../blueprint/BuiltinRefSuite.scala`: a built-in resolves only by its prefixed name, a file descriptor named `neo4j-merge-sink` does not shadow it and is not matched by `builtin/neo4j-merge-sink`, an unknown built-in gives `UnknownBuiltin` listing the known ones, and a blueprint whose only streamlet is built in verifies with an empty file list (no `EmptyStreamletDescriptors` when built-ins are present).
- [ ] T007 [P] In `crd/.../crd/AnkkaFlow.scala` append `builtin: Boolean = false` to `StreamletSpec` with a scaladoc (`true` for a streamlet whose descriptor the platform ships; `image` is then empty and the pod has only the sidecar). In `kustomization/components/crd/ankkaflow.yaml` add `builtin: { type: boolean }` under the streamlet item's properties and change `required: [name, image, descriptor]` to `[name, descriptor]`. Extend `crd/src/test/.../crd/CrdSchemaSuite.scala`'s round trip with a built-in streamlet (`builtin = true`, `image = ""`) and assert the schema no longer requires `image`.

**Checkpoint**: `sbt protocol/test blueprint/test crd/test` green; the fixture and its Python copy committed; `git diff --exit-code sdks/python/proto/` clean after `scripts/proto.py`.

---

## Phase 3: User Story 1 — A graph that is right however the deltas arrive (Priority: P1) 🎯 MVP

**Goal**: the stage inside the sidecar: deltas parsed and folded, one transaction per batch,
commit only after it, idempotent under redelivery, reordering, restart and replay; the sidecar runs
it with no process.

**Independent Test**: quickstart.md tiers 2 and 3: `sbt 'sidecar/testOnly *DeltasSuite
*Neo4jMergeSuite *Neo4jSinkKafkaSuite' mutationCheck` — the scripted sequence with every delta
twice, older after newer, a kill between transaction and commit, and a replay from the start,
yields the same graph each time (SC-001, SC-002).

### The contract, in code

- [ ] T008 [US1] Create `sidecar/.../sidecar/Deltas.scala`: `enum Delta { NodeMerge(id, version: Long, labels: Vector[String], properties: Map[String, Value]); EdgeMerge(id, version, tpe, from, to, properties); NodeTombstone(id, version); EdgeTombstone(id, version, tpe, from, to) }` with `Value` (string, long, double, boolean, or a homogeneous non-empty list of one); `def parse(offset: Long, value: ByteString): Either[String, Delta]` using `protocol.Json.parse`, applying every check in contracts/graph-delta.md's validation table with exactly its messages (`offset N: …`), an integral `Num` → `Long` (failing outside 64 bits), other numbers → `Double`, unknown top-level fields ignored; `def fold(deltas: Vector[Delta]): Folded(nodes, edges, nodeTombstones, edgeTombstones, stale: Int)` keeping, per id space (node ids vs edge ids), the highest version and the first on a tie; and `def toParameters(folded)` producing the four `java.util.List[java.util.Map[String, AnyRef]]` the statements take (a `Value` list becomes a Java list; the reserved keys are never present).
- [ ] T009 [P] [US1] Create `sidecar/src/test/.../sidecar/DeltasSuite.scala` (no container): every example in contracts/graph-delta.md parses to the expected `Delta`; every row of the validation table produces its message with the offset; `1790627790360` is a `Long` and `1.5` a `Double` and `1e3` fails as a version but is a `Double` property; arrays of mixed types, `null`, nested objects and reserved keys fail; folding keeps the highest version, the first on a tie, counts the rest stale, and keeps a node id and an edge id with the same string apart; unknown top-level fields are ignored.

### The stage

- [ ] T010 [US1] Create `sidecar/.../sidecar/Stage.scala`: `trait Stage { def processor: BatchProcessor; def open(stopped: () => Boolean): Either[Refusal, Unit]; def failed: Future[Throwable]; def fail(cause: Throwable): Unit; def stop(reason: String): Unit; def close(): Unit }` where `Refusal(problems: Vector[String], exitCode: Int)`; and `final class ProcessStage(channel, discovery, deployed, config)` that moves today's discovery-then-`Conversation.open` path out of `Supervisor` unchanged (`open` runs `discovery.run`, reports and returns `Refusal(problems, 1)` on `Left`, and creates the `Conversation`; `processor`/`failed`/`fail`/`stop` delegate to it).
- [ ] T011 [US1] Refactor `sidecar/.../sidecar/Supervisor.scala` to run over a `Stage`: `run()` creates the gRPC channel only for a `ProcessStage`; `loop` calls `stage.open` where it ran discovery and exits with the refusal's code; `Session` takes the `Stage` and uses `stage.processor` for every `InletGraph`; readiness is every inlet subscribed and `open` returned; teardown calls `stage.fail`, stop calls `stage.stop`, and `run` ends with `stage.close()`. Behaviour for a process is unchanged: `sbt 'sidecar/testOnly *ConversationSuite *SupervisorSuite *RestartKafkaSuite *ConformanceSuite'` stays green before moving on.
- [ ] T012 [P] [US1] In `sidecar/.../sidecar/StreamletConfig.scala` parse an optional `flow.stage { name = <string>, neo4j { credentials-dir = <string> } }` block into `StageConfig(name, neo4j: Option[Neo4jStageConfig])`; `check` refuses a `stage.name` that `Builtins.byName` does not know (`stage 'x' is not built into this sidecar; it has: neo4j-merge-sink`) and a `neo4j-merge-sink` stage with no `neo4j.credentials-dir`. In `Settings.scala` make `FLOW_PROCESS_ADDRESS` default as today but unused in stage mode (no validation of it there). Extend `StreamletConfigSuite` with the block, the unknown stage, and the missing directory.
- [ ] T013 [P] [US1] Create `sidecar/.../sidecar/Neo4jSecret.scala`: `Neo4jSecret(uri, username, password, database: String = "neo4j")` read from a directory of files `uri`, `username`, `password`, optional `database` (trimmed); a missing required file is `Left("credentials directory <dir> has no 'uri'")`; `def redact(message: String)`: replaces the password and any `user:pass@` userinfo in a string. Unit-test it in `Neo4jSecretSuite.scala`, including that `redact` leaves a message without the password unchanged.
- [ ] T014 [US1] Create `sidecar/.../sidecar/Neo4jMergeStage.scala` implementing `Stage` and `BatchProcessor`: `open` reads the secret (a missing file → `Refusal(_, 2)`), compares `deployed` with `Builtins.neo4jMergeSink` via `DescriptorValidation.compare` (differences → `Refusal(_, 1)`), builds the driver (`GraphDatabase.driver(uri, AuthTokens.basic(...))`), `verifyConnectivity` and checks `ServerInfo.agent` is `Neo4j/5.26` or later (else a failed open with the message of contracts/neo4j-merge-sink.md, retried by the loop), runs `CREATE CONSTRAINT element_id IF NOT EXISTS FOR (n:Element) REQUIRE n.id IS UNIQUE` (a `Neo.ClientError.Security.Forbidden` → `events.warning("ConstraintNotCreated", …)` and continue; any other failure → failed open); `process` parses every record (the first problem fails the Future with `StreamFailed("neo4j merge failed for inlet '<i>' partition <p>: <problem>")`), folds, and runs the four statements of contracts/neo4j-merge-sink.md verbatim inside one `session.executeWrite` with `TransactionConfig.timeout(transaction-timeout)` on the Secret's database, skipping a statement whose list is empty, then completes `Acked(Vector.empty)`; any exception → `StreamFailed` with the redacted message; `revoke` is a no-op; `failed` is a promise completed by `fail`; `close` closes the driver. Log lines pass through `Neo4jSecret.redact`. Verify research items 2–5 here and record them.
- [ ] T015 [US1] Wire stage mode in `sidecar/.../sidecar/Main.scala`: when `config.stage` is defined, construct `Neo4jMergeStage(deployed, config, events, …)` instead of the process pieces and skip the process-address log line; log `stage 'neo4j-merge-sink' for <pipeline>.<streamlet>`. Exit codes as contracts/neo4j-merge-sink.md.

### The suites

- [ ] T016 [US1] Create `sidecar/src/test/.../sidecar/Neo4jMergeSuite.scala` (`Neo4jSuite`, no Kafka): drive `Neo4jMergeStage.process` directly with `InputBatch`es built by `TestSpecs.input` and assert through `query`: every row of data-model.md's state table; labels replaced (`Element` kept); a property absent from the next version is gone; an edge creates placeholder endpoints (`_version = -1`, only `id`) that a later node delta replaces; a tombstone marks and keeps properties, and a lower-version merge after it is stale; equal version is stale; a batch is folded (two versions of one id in one batch write once); a batch with one malformed record writes nothing and fails naming the offset; numbers, booleans and arrays round-trip; the `written` counts equal the applied rows; a wrong password fails `open` with a message not containing it (grep the captured log); against a user without `CREATE CONSTRAINT` (`CREATE USER … ; GRANT ACCESS, MATCH, WRITE`) `open` succeeds and `ConstraintNotCreated` is recorded; the constraint exists after an admin open. Verify research item 5 here.
- [ ] T017 [US1] Extend `sidecar/src/test/.../sidecar/SidecarRun.scala` with a stage-mode constructor (`SidecarRun.stage(deployed, conf, events)`; no `processPort`) and `SidecarRun.stageConf(pipeline, streamlet, bootstrap, inlet, credentialsDir, maxRecords, config)` rendering a `streamlet.conf` with the `stage` block; `writeSecret(dir, secret)` writes the four files.
- [ ] T018 [US1] Create `sidecar/src/test/.../sidecar/Neo4jSinkKafkaSuite.scala` (`KafkaSuite with Neo4jSuite`): the sidecar in stage mode on the `builtin` fixture: (1) not ready while Neo4j's password is wrong, ready within 30 s of the files being corrected; (2) publish the scripted sequence of quickstart tier 3 keyed by element id over 3 partitions — `committed(group)` never exceeds the number of records whose deltas are in the graph, and at the end the graph equals the reference graph built by a plain driver from the same sequence delivered once in order; (3) publish every record again → committed advances, graph unchanged, and the stage's stale count equals the record count; (4) stop the sidecar with `stop()` immediately after a transaction is observed (use a `Neo4jSuite` hook that counts committed transactions via a `CALL dbms.listTransactions` poll, or a stage test hook `afterWrite`), start a new `SidecarRun` on the same group → the batch is redelivered, all stale, graph unchanged; (5) reset the group to the earliest offset with `Admin.alterConsumerGroupOffsets` while stopped, start again → identical graph; (6) a `descriptor.json` that is the `sink` fixture instead of the built-in → `exited` completes with 1 and the log names the differences; (7) `mutationCheck` still fails with the commit moved first (run `sbt mutationCheck`).

**Checkpoint**: `sbt 'sidecar/testOnly *DeltasSuite *Neo4jSecretSuite *StreamletConfigSuite *Neo4jMergeSuite *Neo4jSinkKafkaSuite'` and `sbt mutationCheck` green; the process suites unchanged. This is the MVP: a sidecar image that, given a `streamlet.conf` with a stage block and a credentials directory, builds a correct graph from a topic.

---

## Phase 4: User Story 2 — Declared, verified and deployed like any streamlet (Priority: P2)

**Goal**: `builtin/neo4j-merge-sink` in a blueprint verifies, generates a resource with no image,
and the operator renders a sidecar-only pod with the Secret mounted, refusing what it should.

**Independent Test**: quickstart.md tier 1 (`sbt 'cli/testOnly *VerifySuite *GenerateSuite'
'operator/testOnly *RenderingSuite *StreamletFilesSuite'`) and tier 5's k3s scenario (SC-003, SC-004).

### CLI

- [ ] T019 [US2] In `cli/.../cli/Verify.scala` append `Builtins.all.map(s => StreamletDescriptor(s.getStreamlet, builtin = true))` to the loaded descriptors; make `--descriptors` optional in `Main.scala` (`Opts.option[Path]("descriptors", …).orNone`) and let `Descriptors.load(None)` yield no files while a present but empty directory is still refused as today.
- [ ] T020 [US2] In `cli/.../cli/Main.scala` `generateResource`: exclude `descriptor.builtin` streamlets from the missing-image refusal and refuse an image supplied for one with `BuiltinHasImage`; in `ResourceWriter.scala` write `image = ""`, `builtin = true` for a built-in and `images(s.name)` otherwise.
- [ ] T021 [P] [US2] Add `cli/src/test/resources/blueprints/graph/{blueprint.conf,images.conf,descriptors/mapper.json,overrides.conf}`: a `mapper` descriptor (inlet `in` of `ankka.checkout-notice.v1`, outlet `deltas` of `ankka.graph-delta.v1`; generate it with the Python SDK or write it canonically by hand and check with `DescriptorJson`), a blueprint with `graph = builtin/neo4j-merge-sink`, an unmanaged input and a managed `graph-deltas` topic, `images.conf` with only `mapper`, and `overrides.conf` with `flow.streamlets.graph.config { secret = neo4j-shop }`. Extend `VerifySuite.scala`: verifies without a descriptor file for the sink; refused naming both ports when `mapper.deltas` is given another contract (`variant`); `builtin/nope` refused listing the built-ins; `flow verify` with no `--descriptors` on a blueprint of built-ins alone succeeds; a parameter the sink does not declare is refused; `secret` missing is refused as "no default and no value". Extend `GenerateSuite.scala`: `builtin: true`, `image: ""`, the embedded descriptor equals the fixture, `config.secret` present; `--image graph=x` refused with `BuiltinHasImage`'s message; the FR-020 test still finds no `sidecar` in the resource.

### Operator

- [ ] T022 [US2] In `operator/.../operator/Observed.scala` add `secrets: Map[String, SecretState]` keyed by Secret name (`SecretState(resourceVersion: String, keys: Set[String])`) and `secretProblems: Map[String, String]`; in `Fabric8Executor.observe` fetch, for every `builtin` streamlet, the Secret its `config.secret` names in the resource's namespace (`get` by name; absent → not in the map; a read error → `secretProblems`). Verify research item 7 here.
- [ ] T023 [US2] In `operator/.../operator/StreamletFiles.scala` render, for a `builtin` streamlet, `stage { name = <descriptor name>, neo4j { credentials-dir = "/etc/flow/neo4j" } }` inside `flow { }` and include the Secret's `resourceVersion` in `hash` (as `"\n---\nsecret:" + rv`); never the Secret's values. Extend `StreamletFilesSuite.scala`: the block is present for a built-in, absent otherwise; the hash changes with the `resourceVersion`; no password appears.
- [ ] T024 [US2] In `operator/.../operator/Rendering.scala`: refusals for a built-in streamlet whose `image` is not empty, whose descriptor name is not a built-in the operator knows (`Builtins.byName`), whose `config.secret` is missing, or whose Secret is absent or lacks `uri`/`username`/`password` (messages of contracts/built-in-streamlets.md and neo4j-merge-sink.md); `deploymentFor` with only the `sidecar` container, no `FLOW_PROCESS_ADDRESS`, a `neo4j` volume (`SecretVolumeSource`, `defaultMode 0400`) mounted read-only at `/etc/flow/neo4j`; the `Neo4jMergeStage` needs no other change. Confirm `LifecycleRules` and `StreamletRolled` compare `""` to `""` for a built-in.
- [ ] T025 [US2] Extend `operator/src/test/.../operator/Fixtures.scala` with a `graph` resource (the `mapper` from T021 plus the built-in from the fixture, `config.secret = "neo4j-shop"`) and an `Observed` carrying `neo4j-shop` with the four keys; extend `RenderingSuite.scala`: the sink's Deployment has one container named `sidecar` with mounts `config`, `api-token`, `neo4j` and no `FLOW_PROCESS_ADDRESS`; `streamlet.conf` in its Secret has the stage block and no password; the mapper's Deployment still has two containers; refusals with `actions.size == 2` for a missing Secret, a Secret without `password`, an image on the built-in, and an unknown built-in name; the config hash changes when the Secret's `resourceVersion` does.

### On a cluster

- [ ] T026 [US2] Extend `operator/src/test/.../operator/FlowClusterSuite.scala` with one scenario: a `neo4j` namespace with a Deployment on `flow.neo4j.image` (`NEO4J_AUTH=neo4j/flow-test`), a Service with Bolt on NodePort 30687 and `ClusterImages.importInto` extended to auto-pull `neo4j:*`; the Secret `neo4j-test` in the pipeline's namespace; a resource generated from the `graph` fixture with only an unmanaged `graph-deltas` input (no mapper image); assert the sink's pod has exactly one container, mounts `/etc/flow/neo4j`, the resource is `Ready`, and a delta published to the topic from the test JVM (through the existing external listener) appears in Neo4j (queried through the driver on `localhost:<mapped 30687>`); then delete the Secret, re-apply the resource and assert a `Refused` event and phase `Failed`. Verify research item 8 here.
- [ ] T027 [US2] Add `kustomization/overlays/neo4j/{kustomization.yaml,neo4j.yaml}`: namespace `neo4j`, a StatefulSet on `neo4j:5.26-community` with `NEO4J_AUTH` from a Secret, a Service `neo4j` (7687, 7474), and the Secret `neo4j-local` in namespace `shop` (`uri = bolt://neo4j.neo4j.svc:7687`, `username`, `password`, `database = neo4j`); a `Justfile` recipe `neo4j-up` (`kubectl apply -k kustomization/overlays/neo4j && kubectl -n neo4j rollout status statefulset/neo4j`) and `neo4j-down`; leave `overlays/local` and `deploy-local.sh` unchanged.

**Checkpoint**: tier 1 and tier 5 green; `flow verify cli/src/test/resources/blueprints/graph/blueprint.conf --descriptors …/descriptors` prints `verified: 2 streamlets, 2 topics`.

---

## Phase 5: User Story 3 — Watched while it writes (Priority: P3)

**Goal**: written and stale counts per partition beside lag; a database outage visible as not
ready, growing lag and a stall warning carrying the reason; nothing secret in a log.

**Independent Test**: quickstart.md tier 3's outage cases and `sbt 'sidecar/testOnly
*PrometheusRulesSuite'` (SC-005, FR-024, FR-025).

- [ ] T028 [P] [US3] Create `sidecar/.../sidecar/StageMetrics.scala`: per (inlet, partition) `LongAdder`s `deltasWritten`, `deltasStale`, `batchesFailed` behind a `StageMetricsMBean { getDeltasWritten; getDeltasStale; getBatchesFailed }`; register them from `Metrics.refresh` under `ankka.flow:type=stage,inlet=<i>,partition=<p>` for every partition `stalls.seen`; `Neo4jMergeStage` adds to them after each batch (written from the statements' `written` counts, stale from the fold and from `list size − written`, failed on any failure).
- [ ] T029 [P] [US3] Add three rules to `sidecar/src/universal/agent/prometheus.yaml` after the sidecar rules, mapping `ankka.flow<type=stage, inlet=(\S+), partition=([0-9]+)><>DeltasWritten:` → `ankka_flow_stage_deltas_written_total` (and `DeltasStale`, `BatchesFailed`) with labels `inlet`, `partition`, type COUNTER; extend `sidecar/src/test/.../sidecar/PrometheusRulesSuite.scala` with the three bean names.
- [ ] T030 [US3] Extend `Neo4jSinkKafkaSuite.scala`: after the scripted sequence, the stage beans report `written == applied` and `stale == folded + filtered`; `pause()` Neo4j, publish ten more records → within 30 s `!sidecar.ready`, `committed` unchanged, `events.warnings` (with `FLOW_STALL_WARNING_AFTER` set short through `Stalls`) contains `PartitionStalled` whose note contains the Neo4j error and not the password; `unpause()` → ready, drained, every delta once, `batchesFailed > 0`. Extend `Neo4jMergeSuite.scala`: every log line captured during a failed open and a failed batch passes `Neo4jSecret.redact` unchanged (the password never appears).
- [ ] T031 [US3] In `docs/deploy/observe.md` add a section on a built-in stage's metrics (the three counters beside lag) and on `ConstraintNotCreated`; in `docs/deploy/troubleshooting.md` add rows for: the sink never ready (wrong credentials, server below 5.26, unreachable), `PartitionStalled` with a Neo4j error, `ConstraintNotCreated`, `Refused` for a missing Secret, and exit 1 for a descriptor that is not the sidecar's built-in.

**Checkpoint**: tier 3 green including the outage cases; `curl :2050/metrics` on a running stage shows the three counters.

---

## Phase 6: User Story 4 — A sample beside ankka (Priority: P4)

**Goal**: `samples/checkout-graph`: a Python mapper from checkout notices to deltas in front of the
sink; the laptop loop with Neo4j in compose; the same pipeline on kind beside ankka; the
throughput floor measured.

**Independent Test**: quickstart.md tiers 4 and 6 (SC-006, SC-007).

- [ ] T032 [US4] Create `samples/checkout-graph/` on the `checkout-feed` pattern: `pyproject.toml` (`checkout-graph`, `[tool.ankka-flow] streamlet = "checkout_graph.mapper:CheckoutGraph"`, dev `pytest`, `neo4j` (the Python driver) and `kafka-python` for the scripts), `src/checkout_graph/{__init__,main,mapper}.py` — `CheckoutGraph` with `JsonInlet("in", schema_name="ankka.checkout-notice.v1")` and `JsonOutlet("deltas", schema_name="ankka.graph-delta.v1")`, emitting per notice `{"cartId","at"}` three deltas keyed by their ids: node `cart:<cartId>` (`Cart`, `{"cartId"}`), node `checkout:<cartId>:<at>` (`Checkout`, `{"cartId","checkedOutAt": ISO}`), edge `checked-out:<cartId>:<at>` (`CHECKED_OUT`, cart → checkout), each with `version = at`; a record that is not a notice is skipped with a log line (as `checkout-feed` does) — between `# docs:start mapper` / `# docs:end mapper`; `tests/test_mapper.py` with `Harness` (three deltas per notice, keys are the ids, version is `at`, a malformed record is skipped, order per cart) between `# docs:start mapper-test` markers; `uv run descriptor` → `flow/descriptor.json`, committed.
- [ ] T033 [P] [US4] Add `samples/checkout-graph/blueprint.conf` (streamlets `mapper = checkout-graph`, `graph = builtin/neo4j-merge-sink`; topics `cart-checkouts` unmanaged on `default` with `auto.offset.reset = earliest`, `graph-deltas` managed, 3 partitions, producers `[mapper.deltas]`, consumers `[graph.in]`), `k8s/in-cluster.conf` (`flow.streamlets.graph.config { secret = neo4j-local }`), `Dockerfile` and `Dockerfile.dockerignore` (from `checkout-feed`, names changed). Verify with `flow verify` and `flow generate --image mapper=sample-checkout-graph:latest --conf k8s/in-cluster.conf -n shop`.
- [ ] T034 [P] [US4] Add the laptop loop: `samples/checkout-graph/docker-compose.yml` (kafka as `cart-router`'s; `neo4j` on `neo4j:5.26-community` with `NEO4J_AUTH=neo4j/flow-local`, 7687 and 7474 published; `sidecar-mapper` as `cart-router`'s sidecar with `./flow-mapper:/etc/flow/config:ro`; `sidecar-graph` with `./flow-graph:/etc/flow/config:ro`, `./neo4j-secret:/etc/flow/neo4j:ro`, no `FLOW_PROCESS_ADDRESS`, `2051:2050`), `flow-mapper/streamlet.conf` and `flow-mapper/descriptor.json` (a copy of `flow/descriptor.json`, checked by the tests), `flow-graph/streamlet.conf` (the stage block; inlet `in` on `graph-deltas`) and `flow-graph/descriptor.json` (a copy of `protocol/fixtures/builtin/neo4j-merge-sink.json`, checked by the tests), `neo4j-secret/{uri,username,password,database}` (committed; a local-only password), `produce.py` (twenty notices over five carts with the CloudEvents headers ankka writes, creating the topics), `verify.py` (queries Neo4j with the Python driver: five `Cart`, twenty `Checkout`, twenty `CHECKED_OUT`, every `_version` equal to its `at`; exit 0), and `README.md` with the loop and the kind section.
- [ ] T035 [US4] Add `samples/checkout-graph/bench.py`: preload 60,000 deltas (from 20,000 notices) over 3 partitions, start the sink's sidecar, measure deltas per second per partition until the group's lag is zero, print it; run it and record the number in research.md *Measurements at the end* (SC-007 ≥ 1,000/s/partition).
- [ ] T036 [US4] Extend `.github/workflows/ci.yml`'s `sdk-python` job with a `Sample graph` step (`samples/checkout-graph`: `uv sync`, `uv run descriptor --check`, `uv run pytest -q`) and its `changes` filter already covers `samples/**`; extend `release.yml`'s images job to build and push `ghcr.io/thinkmorestupidless/sample-checkout-graph:${version}` from `samples/checkout-graph/Dockerfile`; add the sample to `sampleImage`'s comment in `build.sbt` only if the k3s suite needs it (it does not: T026 publishes deltas directly).
- [ ] T037 [US4] Run quickstart tier 6 on the kind cluster beside ankka (`just neo4j-up`, the image loaded, the resource applied, three checkouts, the Cypher query, a reset and scale-up leaving the graph identical) and record the run in `samples/checkout-graph/README.md` under *Last run on kind* and in research.md *Quickstart tier 6*.

**Checkpoint**: `cd samples/checkout-graph && uv run pytest -q && uv run descriptor --check`, then `docker compose up -d`, `produce.py`, `verify.py` (twice) and `bench.py` all green; the kind run recorded.

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: the documentation and the skills, the limitations page, and what the change to the
rules leaves to tidy.

- [ ] T038 [P] Write `docs/reference/graph-deltas.md` (kind `reference`): the contract from contracts/graph-delta.md in the docs' voice — the three shapes with examples, the eight rules a writer keeps, the validation table, and what the sink does with a delta; and `docs/reference/neo4j-merge-sink.md` (kind `reference`): the descriptor and its two parameters, the Secret's keys and where it is mounted, the blueprint and `--conf` fragments, what it writes (`Element`, `_version`, `_deleted`, placeholders, the constraint), the four statements, metrics, events, exit codes and readiness, and the laptop configuration.
- [ ] T039 [P] Write `docs/build/graph-sink.md` (kind `guide`, `languages: [python]`): build a graph from a pipeline — decide the ids and versions, map events to deltas (include `samples/checkout-graph/src/checkout_graph/mapper.py#mapper` and `tests/test_mapper.py#mapper-test`), wire the sink (include `samples/checkout-graph/blueprint.conf`), give it a Secret, run it on a laptop with the compose file, deploy it on kind beside ankka, and read the graph; when a design needs it (an eventually consistent graph built from several services' topics) and when it does not.
- [ ] T040 Update the existing pages: `docs/reference/blueprint.md` (`builtin/<name>` in `streamlets`, the built-ins that exist), `docs/reference/cli.md` (`--descriptors` optional, no image for a built-in, the two new refusals), `docs/reference/resource.md` (`builtin`, `image` empty), `docs/reference/operator.md` (the Secret it reads, the mount, the refusals), `docs/reference/sidecar.md` (stage mode, the `stage` block, the mount, the three metrics, exit codes), `docs/concepts/sidecar.md` (a pod with only the sidecar; the decoding exception stated), `docs/reference/limitations.md` (drop "no stages built into the sidecar"; add "one built-in stage, Neo4j 5.26 or later, no way to add a stage from outside the sidecar image, no reads from the graph"), `docs/reference/glossary.md` (graph delta, element, tombstone, built-in streamlet, stage, placeholder), `docs/index.md` (one line under *What it promises*), `docs/contributing/documentation.md` (markers now live in three samples; fix the stale "cart router sample" sentence) and `CLAUDE.md`'s *Documentation* section likewise.
- [ ] T041 Add the pages to `mkdocs.yml`'s nav (Build: *Build a graph from a pipeline* after *Read an ankka service's topic*; Reference: *Graph deltas* and *Neo4j merge sink* after *Descriptor*) and to the skills: `ankka-flow` (graph-sink, graph-deltas; rule text: the sidecar decodes only a built-in stage's contract; a new rule on state-shaped versioned deltas), `ankka-flow-deploy` (neo4j-merge-sink, graph-sink; the Secret and the refusals in *Mistakes to check for*), `ankka-flow-python` (graph-deltas; emitting deltas keyed by id), `ankka-flow-protocol` (graph-deltas). Run `just docs-sync && just docs` and commit the rendered skills; `docs check` reports no problem.
- [ ] T042 [P] Update `specs/001-version-one/contracts/sidecar.md` and `protocol/README.md` (and the SDK's copy via `scripts/proto.py`) with one sentence each: the sidecar decodes nothing for a process; a built-in stage decodes its own contract. Confirm CI's protocol diff passes.
- [ ] T043 Run the whole build on a laptop kept awake: `caffeinate -i sbt test` (with the k3s suite), `sbt mutationCheck`, `sbt scalafmtCheckAll scalafmtSbtCheck`, `cd sdks/python && uv run mypy && uv run pytest -q`, every sample's tests, `just docs`; fix what fails; tick the reviewer's checklist in quickstart.md.

---

## Dependencies & Execution Order

- **Phase 1 → Phase 2 → US1**: T001 before any sidecar test; T004–T005 before T014 (the descriptor the stage compares against) and T018 (the fixture the suite deploys); T006–T007 before US2.
- **US1 (T008–T018)** is the MVP and depends only on Phases 1–2. Inside it: T008 → T009; T010 → T011; T012, T013 in parallel with T010–T011; T014 after T008, T010, T012, T013; T015 after T014; T016 after T014; T017 after T012; T018 after T015, T017.
- **US2 (T019–T027)** depends on Phase 2, not on US1, for T019–T025; T026 (k3s) needs the sidecar image with the stage (US1 through T015). T027 is independent.
- **US3 (T028–T031)** depends on US1 (T014, T018); T028 and T029 in parallel; T030 after T028; T031 after T030.
- **US4 (T032–T037)** depends on US1 for the laptop loop and US2 for kind; T032–T034 in parallel once Phase 2 is done (the mapper needs nothing from the sidecar); T035 after T034; T037 after T027, T033, T036.
- **Polish (T038–T043)** after everything; T038, T039, T042 in parallel; T040 → T041; T043 last.

## Parallel Execution Examples

- After Phase 2: `T008`, `T010`, `T012`, `T013` (sidecar, four files) with `T019`+`T020`+`T021` (cli), `T022`+`T023` (operator), `T027` (kustomization) and `T032`–`T034` (the sample's Python, which needs only the SDK).
- Inside US1 once T014 lands: `T016` (merge suite) with `T017` (SidecarRun), then `T018`.
- Inside US2: `T024`–`T025` (rendering) with `T026`'s manifests, then the k3s run.
- Polish: `T038`, `T039`, `T042` at once; then `T040`, `T041`, `T043`.

## Implementation Strategy

1. **MVP = Phases 1–3** (T001–T018): a sidecar image that, given a hand-written `streamlet.conf`
   with a stage block and a directory of credentials, merges a topic of deltas into Neo4j
   correctly under redelivery, restart and replay, proven by two container suites and the
   mutation check. Nothing in the CLI or operator changes yet; the pod is assembled by hand.
2. **Then US2** (T019–T027): the same sink declared in a blueprint, generated into a resource and
   run by the operator on k3s; this is when a user can deploy it.
3. **Then US3** (T028–T031): counts and the outage path, cheap once the stage exists.
4. **Then US4** (T032–T037): the sample, the laptop loop with Neo4j, the kind run beside ankka,
   and the throughput number.
5. **Then Polish**: three pages, ten edits, the skills, the full build.

Each phase ends at a checkpoint that is a green command, so a stop after any of them leaves a
coherent tree.
