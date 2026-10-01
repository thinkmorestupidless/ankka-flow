# Tasks: compacted delta topics

**Input**: Design documents from `/specs/003-compacted-delta-topics/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/, quickstart.md

**Tests**: included. The spec requires them: the feature must be tested against a real broker with
compaction actually run (FR-019), the key fixture is shared by two languages (research R5), and
each story's independent test is a suite in quickstart.md. A test is written with or before the
code it proves, and a refusal is seen to fail first.

**Organization**: by user story. US1 (a delta cannot be keyed wrongly) is the MVP: without it
nothing else is safe. US2 makes delta topics compacted and markers harmless; US3 proves and
documents the rebuild; US4 covers deleted elements over time.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: can run in parallel (different files, no dependency on an unfinished task)
- **[Story]**: US1–US4 from spec.md
- Paths are repository-relative. `.../` abbreviates `src/main/scala/com/thinkmorestupidless/ankka/flow/`
  (or `src/test/scala/...`) under the module named.

## Path Conventions

The six sbt projects at the root, the Python SDK at `sdks/python/`, the sample at
`samples/checkout-graph/`, documentation at `docs/`, curated skills at `tools/docs/skill/`. See
plan.md *Project Structure*.

---

## Phase 1: Setup (the fixture two languages share)

**Purpose**: one statement of what a delta's key is, readable by the sink's suite and the SDK's.

- [X] T001 Create `protocol/fixtures/graph-deltas/keys.json`: a JSON array of `{"delta": {…}, "key": "…"}` with, at least: a node merge (`cart:cart-1` → `node:cart:cart-1`), an edge merge (→ `edge:<id>`), a node tombstone and an edge tombstone (each the key of the element it marks), a node and an edge with the *same* id (two different keys), an id containing several colons, and an id with non-ASCII characters (`café:žluťoučký`). Two-space indent, one trailing newline. Run `cd sdks/python && uv run python scripts/proto.py` so `sdks/python/proto/fixtures/graph-deltas/keys.json` exists; confirm `diff -r protocol/fixtures sdks/python/proto/fixtures` is empty and `uv run pytest -q` still passes.

---

## Phase 2: Foundational (the key, computed in one place)

**Purpose**: the function both the refusal (US1) and the tests of every later story rely on.

**⚠️ CRITICAL**: no user story work begins until this passes.

- [X] T002 In `sidecar/.../sidecar/Deltas.scala` add `def key(delta: Delta): String` (`"node:" + id` for `NodeMerge` and `NodeTombstone`, `"edge:" + id` for `EdgeMerge` and `EdgeTombstone`) and `def keyBytes(delta: Delta): ByteString` (UTF-8). In `sidecar/src/test/.../sidecar/DeltasSuite.scala` add a test that reads `protocol/fixtures/graph-deltas/keys.json` (through `TestSpecs.repoRoot` and `protocol.Json`), parses each `delta` with `Deltas.parse`, and asserts `Deltas.key` equals the row's `key`, with the row's index in the failure message.

**Checkpoint**: `sbt 'sidecar/testOnly *DeltasSuite'` green; the fixture and its Python copy committed.

---

## Phase 3: User Story 1 — A delta that cannot be keyed wrongly (Priority: P1) 🎯 MVP

**Goal**: the sink refuses a delta whose key is not its element's, on every delta topic; the SDK
builds deltas whose keys cannot be chosen.

**Independent Test**: quickstart.md tiers 1 and 2: `sbt 'sidecar/testOnly *DeltasSuite
*Neo4jMergeSuite *Neo4jSinkKafkaSuite'` and `cd sdks/python && uv run mypy && uv run pytest -q` —
a wrongly keyed and a keyless delta each fail their batch naming offset, found and expected key,
and write nothing; a node and an edge with one id are both written; the helper takes no key
(SC-003, SC-004).

### The sink

- [X] T003 [US1] In `sidecar/.../sidecar/Deltas.scala` add `def read(offset: Long, key: Option[ByteString], value: ByteString): Either[String, Delta]`: parse the value as today, then require `key` to equal `keyBytes(delta)`; otherwise `Left(s"offset $offset: key '<found, UTF-8>' is not this delta's element key '<expected>'")`, or for no key `Left(s"offset $offset: no key; this delta's element key is '<expected>'")`. Extend `DeltasSuite`: the right key is accepted for each kind; a bare id, another element's key, and the right id under the wrong kind (`edge:` on a node delta) are refused with exactly these messages; a tombstone under its element's key is accepted; a key that is not valid UTF-8 is refused without throwing.
- [X] T004 [US1] In `sidecar/.../sidecar/Neo4jMergeStage.scala` `process` call `Deltas.read(r.offset, r.getRecord.key, r.getRecord.value)` in place of `Deltas.parse`. Update `sidecar/src/test/.../sidecar/Neo4jMergeSuite.scala`: its `Opened.apply`/`attempt` build each record's key from the delta it carries (`Deltas.keyBytes(Deltas.parse(…))`, falling back to an arbitrary key for a deliberately malformed value), so the existing fifteen cases pass unchanged; add: a well-formed delta under key `cart-1` fails the batch with the offset, found and expected key and leaves the graph empty and `batchesFailed == 1`; a keyless delta likewise; a node and an edge that share an id are both written. Run the wrong-key test before changing `process` and see it fail.
- [X] T005 [US1] Bring the other suites' deltas to the keyed form so they pass against the new sink: in `sidecar/src/test/.../sidecar/Neo4jSinkKafkaSuite.scala` key the scripted sequence `node:<id>` / `edge:<id>` (its tombstone under `node:n0`); in `operator/src/test/.../operator/FlowClusterSuite.scala` publish the built-in scenario's delta under `node:cart:k3s`. Run `sbt 'sidecar/testOnly *Neo4jSinkKafkaSuite'`.

### The SDK

- [ ] T006 [P] [US1] Create `sdks/python/src/ankka_flow/graph.py` per contracts/python-graph.md: `SCHEMA_NAME = "ankka.graph-delta.v1"`; `node_key(id) -> bytes`, `edge_key(id) -> bytes`; `class GraphDeltaOutlet(JsonOutlet)` whose `__init__(self, name)` fixes the schema name, with `node(record, *, id, version, labels=(), properties=None)`, `edge(record, *, id, version, type, from_id, to_id, properties=None)`, `tombstone_node(record, *, id, version)` and `tombstone_edge(record, *, id, version, type, from_id, to_id)`, each validating its arguments (the `ValueError`s the contract lists, naming the argument; `bool` is not accepted as a version), building the value with `ankka_flow.json.dumps` and returning `self.emit(record, value=…, key=…)`; a frozen dataclass `Delta(kind, element, id, version, labels, type, from_id, to_id, properties, key)`; and `read(record) -> Delta`, raising `ValueError` for a record that is not a delta or whose key is not its element's. Export `GraphDeltaOutlet` from `sdks/python/src/ankka_flow/__init__.py` (and `__all__`). `uv run mypy` clean.
- [ ] T007 [US1] Create `sdks/python/tests/test_graph.py`: for every row of `proto/fixtures/graph-deltas/keys.json`, building the row's delta with the outlet gives a record whose key is the row's `key` and whose value parses to the row's `delta`; `inspect.signature` of the four methods has no `key` parameter; every `ValueError` case of the contract; `read` round-trips each kind and refuses a wrongly keyed record; a streamlet declaring `GraphDeltaOutlet("deltas")` writes the same descriptor as one declaring `JsonOutlet("deltas", schema_name="ankka.graph-delta.v1")`; through `Harness`, a delta built from an input record leaves `h.skipped` empty. `uv run mypy && uv run pytest -q`.

**Checkpoint**: tier 1's Scala and Python commands and tier 2 green. This is the MVP: no wrongly
keyed delta reaches a graph, and a Python mapper cannot write one.

---

## Phase 4: User Story 2 — A delta topic that stays the size of the graph (Priority: P2)

**Goal**: managed delta topics compacted by default, said so at verification; delete markers passed
over and counted; an existing uncompacted topic reported.

**Independent Test**: quickstart.md tier 1's blueprint, CLI and operator commands and tier 2's
marker cases (SC-005, SC-006).

### Delete markers

- [X] T008 [US2] In `sidecar/.../sidecar/Deltas.scala` make `read` return `Either[String, Read]` with `enum Read { case Applied(delta: Delta); case Marker }`: an empty `value` is `Right(Read.Marker)` whatever the key (present, absent or malformed). Extend `DeltasSuite`: empty value with an element key, with no key, and with an arbitrary key are all markers; a one-byte value is not.
- [X] T009 [US2] In `sidecar/.../sidecar/Neo4jMergeStage.scala` drop markers before the fold and count them; a batch with no deltas runs no transaction and completes `Acked(Vector.empty)`. Change `Neo4jMergeStage.Record.applied` to `(inlet, partition, written, stale, markers)` with `stale = records − markers − written`; in `sidecar/.../sidecar/StageMetrics.scala` add a `markers` adder and `getDeleteMarkers` to `StageMetricsMBean`; add to `sidecar/src/universal/agent/prometheus.yaml` the rule for `ankka.flow<type=stage, …><>DeleteMarkers:` → `ankka_flow_stage_delete_markers_total` (COUNTER, labels `inlet`, `partition`) and its case in `PrometheusRulesSuite.scala`. Extend `Neo4jMergeSuite`: a batch of a delta, a marker and another delta writes two and counts one marker and no stale; a batch of markers alone is acknowledged, writes nothing and fails nothing; a marker for an element in the graph leaves it exactly as it was.

### Compaction by default

- [X] T010 [P] [US2] Create `blueprint/.../blueprint/DeltaTopics.scala` per research R3 and data-model.md: `enum Decision { case Compacted; case Kept(policy: String); case NotOurs }`, `def decide(topic: VerifiedTopic, topicConfig: Map[String, String]): Option[Decision]` (`None` unless some connection's schema name is `Builtins.GraphDeltaSchema`), `def note(topic: VerifiedTopic, decision: Decision): String` with the four texts of contracts/delta-topics.md, and `val CleanupPolicy = "cleanup.policy"`. Create `blueprint/src/test/.../blueprint/DeltaTopicsSuite.scala` (with `Builders`): every row of data-model.md's decision table, including a topic with only a producing delta port, `compact,delete`, a policy without `compact`, an unmanaged topic, and a topic with no delta port (`None`).
- [X] T011 [US2] In `cli/.../cli/Verify.scala` append `DeltaTopics.note` for each topic `decide` recognises (using `overrides.topicConfig(t)` through `TopicSettings`) to `notes`; in `cli/.../cli/ResourceWriter.scala` `topic(...)` add `cleanup.policy -> compact` to `topicConfig` when the decision is `Compacted`. Extend `cli/src/test/.../cli/VerifySuite.scala` and `GenerateSuite.scala` on the `blueprints/graph` fixture: the note for `graph-deltas` on stderr and `cleanup.policy: compact` in the resource; with `topic { cleanup.policy = delete }` in a `variant`, the resource keeps `delete` and the note warns; a `--conf` override of the policy wins; the `cart` fixture's resource is byte-identical to before (no delta port, no change). Verify research item 4 here.

### The operator

- [X] T012 [P] [US2] In `operator/.../operator/Rendering.scala`, for an existing managed topic whose resource `topicConfig("cleanup.policy")` contains `compact` and whose observed value does not, record `TopicNotCompacted` (Warning) with the note of contracts/delta-topics.md and leave `cleanup.policy` out of the `TopicSettingsIgnored` list. Extend `operator/src/test/.../operator/RenderingSuite.scala`: that event with the topic's name and observed policy; no `TopicSettingsIgnored` naming `cleanup.policy`; another differing setting on the same topic is still reported there; an existing topic already compacted records neither.
- [ ] T013 [US2] Extend the built-in scenario of `operator/src/test/.../operator/FlowClusterSuite.scala` with two managed topics in the `graphs` resource, each with `topicConfig = Map("cleanup.policy" -> "compact")`: one absent (assert through `Admin.describeConfigs` that the operator created it with `cleanup.policy=compact`) and one pre-created without it (assert the `TopicNotCompacted` event and that its policy is still `delete`). Verify research item 3 here.

**Checkpoint**: tier 1 green in every module; `flow verify samples/checkout-graph/blueprint.conf …`
prints the compaction note.

---

## Phase 5: User Story 3 — A graph rebuilt from its topic alone (Priority: P3)

**Goal**: proof, against a broker that has compacted, that the sink alone rebuilds the live graph;
the procedure documented.

**Independent Test**: quickstart.md tier 3: `sbt 'sidecar/testOnly *CompactionKafkaSuite'`
(SC-001, SC-002).

- [X] T014 [US3] In `sidecar/src/test/.../sidecar/KafkaSuite.scala` add `protected def kafkaEnv: Map[String, String] = Map.empty`, applied to the container with `withEnv` before it starts, and let `publish` take `Option[String]` values (a `None` value is a record with no value) without changing existing callers' meaning; add `createTopic(name, partitions, config: Map[String, String])`.
- [X] T015 [US3] Create `sidecar/src/test/.../sidecar/CompactionKafkaSuite.scala` (`KafkaSuite with Neo4jSuite`, `kafkaEnv = Map("KAFKA_LOG_CLEANER_BACKOFF_MS" -> "500")`, `munitTimeout` 6 minutes): a one-partition topic with `cleanup.policy=compact`, `segment.ms=200`, `min.cleanable.dirty.ratio=0.01`, `min.compaction.lag.ms=0`, `max.compaction.lag.ms=500`, `delete.retention.ms=200`; publish ten versions of each of a thousand nodes plus edges between neighbours, keyed by element key; run the sidecar in stage mode to the end and snapshot the graph (every node's and edge's properties); stop it. Publish a few records to another key to roll the segment and `eventually` (2 minutes) assert a fresh consumer from the start reads fewer than two thousand records for the thousand node keys. `clear()` the database, run the sidecar under a new group from the start, and assert the graph equals the snapshot and the stage read fewer than a fifth of the records first written. Also rebuild from a second, never-compacted topic holding the same records and assert the same graph (FR-014). Verify research item 1 here; if compaction cannot be had within two minutes, reduce the element count, never the assertions, and record it.
- [X] T016 [P] [US3] Add to `cli/src/test/.../cli/CliResetSuite.scala`: with one streamlet at `replicas: 0` and no pods and another running, `flow reset <pipeline> --streamlet <the stopped one>` is accepted and its request names only that streamlet; the same request for the running one is refused. Verify research item 5.
- [ ] T017 [P] [US3] Write `docs/deploy/rebuild-a-graph.md` (kind `guide`): the four steps of contracts/rebuild.md with the `--conf` fragment and commands, what the rebuilt graph contains and does not, what the topic must be (compacted, keyed, markers only after tombstones), and how to watch it (lag to zero; `deltas_written_total`). Add it to `mkdocs.yml`'s nav under *Run and operate* after *Rebuild from the start*, and to the `pages:` of `tools/docs/skill/ankka-flow-deploy/SKILL.md` and `tools/docs/skill/ankka-flow/SKILL.md`.

**Checkpoint**: tier 3 green; the guide renders.

---

## Phase 6: User Story 4 — Deleted elements leave the topic eventually (Priority: P4)

**Goal**: tombstones stay as their elements' last records; a writer's delete marker removes one from
future rebuilds and never stalls the sink.

**Independent Test**: quickstart.md tier 3's marker cases (SC-005, FR-015, FR-016).

- [X] T018 [US4] Extend `CompactionKafkaSuite.scala`: tombstone one node and rebuild before any marker — it is present and marked deleted; publish a delete marker (a `None` value under the node's element key), let the broker compact, rebuild — the node is absent, every other element equals the snapshot, and the stage counted the marker if it was still in the log; rebuild from the never-compacted topic holding merge, tombstone and marker — the node is present and marked deleted, so the live graph is the same in both. Assert nowhere that the sink wrote to the topic: its end offsets change only by what the test published (FR-016).
- [X] T019 [US4] Extend `Neo4jSinkKafkaSuite.scala`: a delete marker produced by the plain client with a null value, between two deltas, is read past: committed offsets reach the end, the sidecar stays ready, and `delete_markers_total` is one; a zero-length value behaves the same. Verify research item 2.

**Checkpoint**: tier 3 fully green.

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: the sample, the documentation and skills, the measurement, the whole build, and the
migration on the cluster that still runs 0.2.0.

- [ ] T020 [P] Move `samples/checkout-graph` to the helper: `src/checkout_graph/mapper.py` declares `GraphDeltaOutlet("deltas")` and yields `self.deltas.node(...)`, `.node(...)`, `.edge(...)` (the `# docs:start mapper` region kept); `tests/test_mapper.py` reads the emits with `graph.read` and asserts the three keys (`node:cart:cart-1`, `node:checkout:…`, `edge:checked-out:…`) inside the `mapper-test` region; `bench.py` keys its preloaded deltas with `graph.node_key` / `graph.edge_key`; `README.md` shows a console consumer printing keys and notes that the delta topic is compacted. `uv run descriptor --check` must report the descriptor unchanged; `uv run pytest -q`.
- [ ] T021 Update the documentation per research R9: `docs/reference/graph-deltas.md` (the key rule and table, the refusal messages, delete markers, *For a writer built before this rule* with the migration steps of contracts/element-keys.md), `docs/reference/neo4j-merge-sink.md` (wrong key, markers, `ankka_flow_stage_delete_markers_total`), `docs/reference/blueprint.md` and `docs/reference/cli.md` (delta topics compacted by default; the notes), `docs/reference/operator.md` and `docs/reference/resource.md` (`TopicNotCompacted`), `docs/reference/python-sdk.md` (`ankka_flow.graph`), `docs/build/graph-sink.md` (the helper in place of hand-built deltas; the keys), `docs/concepts/topics.md` (a paragraph on delta topics), `docs/deploy/troubleshooting.md` (rows: a wrong-key stall; `TopicNotCompacted`), `docs/deploy/observe.md` (the fourth counter), `docs/reference/limitations.md` (the platform writes no delete markers and never alters a topic's policy) and `docs/reference/glossary.md` (element key, delta topic, delete marker, rebuild).
- [ ] T022 Update the skills: `tools/docs/skill/ankka-flow/SKILL.md` (the delta rule gains the key and "compacted by default"), `tools/docs/skill/ankka-flow-python/SKILL.md` (build deltas with `GraphDeltaOutlet`, never by hand; a mistake to check for: `emit(..., key=…)` on a delta), `tools/docs/skill/ankka-flow-deploy/SKILL.md` (`TopicNotCompacted`, the rebuild guide, markers); keep each description within 1024 characters. Run `just docs-sync && just docs`; `docs check` reports no problem; commit the rendered skills.
- [ ] T023 Run `samples/checkout-graph/bench.py` on the compose file and record the result in research.md *Measurements at the end*; it must not be worse than 1,782 deltas/s per partition by more than run-to-run noise.
- [ ] T024 Run the whole build kept awake: `caffeinate -i sbt scalafmtCheckAll scalafmtSbtCheck test mutationCheck`, `cd sdks/python && uv run mypy && uv run pytest -q`, the three samples' `descriptor --check` and `pytest`, `just docs`; fix what fails; tick the reviewer's checklist in quickstart.md (including `git diff --stat origin/main -- protocol/fixtures/builtin` being empty).
- [ ] T025 Run quickstart tier 6 on the kind cluster that still runs the 0.2.0 `checkouts-graph` pipeline: `just deploy`; observe the sink refuse the first old-keyed record with the expected key and the resource carry `TopicNotCompacted`; carry out the migration (scale to zero, delete the delta topic, load the new mapper image, `flow reset`, scale up); confirm the topic is compacted and the graph as it was; then rebuild into an emptied Neo4j with the mapper stopped. Record the run in `samples/checkout-graph/README.md` and research.md *Quickstart tier 6*.
- [ ] T026 State the breaking change where a release carries it: the pull request's description and the `v0.3.0` tag's annotation say that a writer of `ankka.graph-delta.v1` built for 0.2.0 is refused until its deltas are keyed `node:<id>` / `edge:<id>`, with a link to the migration steps on `docs/reference/graph-deltas.md`.

---

## Dependencies & Execution Order

- **T001 → T002 → US1.** The fixture, then the key function, then everything.
- **US1 (T003–T007)**: T003 → T004 → T005 in the sidecar; T006 → T007 in the SDK, in parallel with the sidecar.
- **US2 (T008–T013)** depends on US1 for T008–T009 (they extend `read`); T010 and T012 depend on nothing but Phase 2 and can start with US1; T011 after T010; T013 after T009 and T012 (it needs this build's sidecar and operator images).
- **US3 (T014–T017)**: T014 → T015, after US1 and T009 (the rebuild reads keyed deltas and may meet markers); T016 and T017 are independent.
- **US4 (T018–T019)** after T015 and T009.
- **Polish**: T020 after T007; T021 after everything it describes; T022 after T017 and T021; T023 after T020 and T009; T024 after all code; T025 after T024; T026 with the pull request.

## Parallel Execution Examples

- After T002: the sidecar's T003–T005, the SDK's T006–T007, `blueprint`'s T010 and the operator's T012 touch four different modules and can proceed together.
- T016 (CLI test) and T017 (the guide) alongside T015 (the compaction suite).
- T020 (the sample) alongside T021 (the docs) once the SDK helper exists.

## Implementation Strategy

1. **MVP = Phases 1–3** (T001–T007): keys enforced by the sink and built by the SDK. On its own
   this already makes it safe for an operator to set `cleanup.policy = compact` by hand.
2. **Then US2** (T008–T013): compaction becomes the default and markers become harmless; a delta
   topic is now bounded without anyone asking.
3. **Then US3** (T014–T017): the rebuild is proven against real compaction and written down.
4. **Then US4** (T018–T019): the long-run behaviour of deleted elements is pinned by tests.
5. **Then Polish**: the sample, the docs and skills, the measurement, the whole build, and the
   migration of the pipeline on the kind cluster.

Each phase ends at a checkpoint that is a green command.
