# Implementation Plan: compacted delta topics

**Branch**: `003-compacted-delta-topics` | **Date**: 2026-10-01 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/003-compacted-delta-topics/spec.md` and its
clarifications: ankka-flow only; a wrongly keyed delta fails its batch on every delta topic; the
platform writes no delete markers; the contract keeps the name `ankka.graph-delta.v1`.

## Summary

A topic of graph deltas becomes a durable, bounded record of the graph. A delta's record key is
defined — `node:<id>` or `edge:<id>` — and the merge sink refuses a delta whose key is not its
element's, so a compacted topic can never lose one element to another's record. A record with no
value is a delete marker the sink passes over and counts. A managed topic with a port of the delta
contract is compacted by default: the CLI decides it, says so in a note, and writes it into the
resource; the operator creates it and warns about an existing topic that is not compacted. The
Python SDK gains `GraphDeltaOutlet`, which builds node, edge and tombstone records with the right
key and takes none from the author. A graph is rebuilt from its topic alone with commands that
already exist — scale the sink to zero, empty the database, reset the sink, scale up — documented
as a guide and proven against a broker that has actually compacted.

## Technical Context

**Language/Version**: Scala 3.9.0, JDK 21, sbt 1.12.15; Python 3.12 for the SDK and the sample.

**Primary Dependencies**: none new. The sink already has the Neo4j driver; the SDK helper is plain
Python over the existing `JsonOutlet`.

**Storage**: Kafka topics with `cleanup.policy = compact` for delta topics; Neo4j as before. The
graph's stored shape (`Element`, `_version`, `_deleted`) is unchanged.

**Testing**: munit with the existing Kafka and Neo4j containers. New: `CompactionKafkaSuite`
(a broker tuned to compact within the test, a thousand elements at ten versions, a rebuild into an
emptied database compared with the original, a delete marker); additions to `DeltasSuite`,
`Neo4jMergeSuite`, `Neo4jSinkKafkaSuite`, `RenderingSuite`, `VerifySuite`, `GenerateSuite`,
`CliResetSuite`, `PrometheusRulesSuite`; a new `DeltaTopicsSuite` in `blueprint`; the SDK's
`test_graph.py` under `mypy --strict`; the sample's harness tests. A shared fixture,
`protocol/fixtures/graph-deltas/keys.json`, read by both Scala and Python (research R5).

**Target Platform**: unchanged. The kind cluster that still runs a 0.2.0 `checkouts-graph`
pipeline is the migration's proving ground.

**Project Type**: the existing build. Changes in `blueprint` (delta topic decision), `cli` (notes,
the default), `sidecar` (key check, markers, a counter), `operator` (one event), `sdks/python`
(the helper), `samples/checkout-graph`, `protocol/fixtures` (a key fixture), `docs/` and the skills.
No CRD, protocol or descriptor change.

**Performance Goals**: no regression of the sink's measured throughput (1,782 deltas/s per
partition): the key check is a byte comparison per record. SC-002: a compacted topic of a thousand
elements written ten times holds under two thousand records and a rebuild reads under a fifth of
what was written.

**Constraints**: a wrongly keyed or keyless delta fails its batch and is never applied (FR-003); a
record with no value is acknowledged, never a failure (FR-006); the platform never writes a delete
marker (FR-016) and never alters an existing topic (FR-010); the default is the CLI's, written into
the resource, so the resource says what runs; the contract's name does not change (FR-016a), so the
built-in descriptor and its fixture are untouched; correctness never depends on compaction having
run (FR-014).

**Scale/Scope**: about 4 new Scala files (`DeltaTopics`, `DeltaTopicsSuite`,
`CompactionKafkaSuite`, the key fixture's reader in `DeltasSuite`) and edits to about 10
(`Deltas`, `Neo4jMergeStage`, `StageMetrics`, `Metrics`, `KafkaSuite`, `Verify`,
`ResourceWriter`, `Rendering`, their suites); 1 new Python module and its tests; the sample's
mapper, tests and bench; 1 new Prometheus rule; 1 new docs page and about 12 edited; 3 skills.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

`.specify/memory/constitution.md` is the unfilled template, so the gate is the rules table in
`CLAUDE.md`:

| Rule | This feature |
|---|---|
| Module direction | Kept. `DeltaTopics` is in `blueprint`, which already depends on `protocol` for `Builtins.GraphDeltaSchema`; the CLI calls it. |
| The sidecar never decodes for a process | Kept. Only the built-in stage reads keys and values, of its own contract. |
| Commit after the write | Kept. A batch of markers alone runs no transaction and is acknowledged; nothing commits ahead of a write. |
| Never skip | Kept, and extended: a wrongly keyed delta fails its batch. A delete marker is not a skipped record: it carries nothing to apply, by the contract. |
| Explicit registration | Kept. The built-in descriptor is unchanged. |
| Built-in stages are declared values | Kept. No change to `Builtins` or its fixture. |
| Pure rendering, one place for I/O | Kept. `TopicNotCompacted` is a `RecordEvent` from `Rendering`, decided from the `TopicState` the executor observed. |
| The resource says what runs | Kept, and the reason the default is the CLI's: `cleanup.policy = compact` is in the resource, never added by the operator. |
| Kafka credentials reach only the sidecar | Untouched. |
| No literal image tags in tests | Kept. |
| Warning-free compile | Kept. |
| Not in this build | Nothing added. |

**Post-design re-check**: unchanged. No rule is bent; Complexity Tracking is empty.

## Project Structure

### Documentation (this feature)

```text
specs/003-compacted-delta-topics/
├── plan.md
├── research.md          # R1–R9 and six items to verify at implementation
├── data-model.md        # the key, the marker, the delta topic decision, the counter, the event
├── quickstart.md        # six tiers
├── contracts/
│   ├── element-keys.md        # the key rule, the refusal, the marker — the contract's addition
│   ├── delta-topics.md        # what the CLI decides and says, what the operator does
│   ├── python-graph.md        # GraphDeltaOutlet and the test helpers
│   └── rebuild.md             # the procedure and what a rebuilt graph contains
├── checklists/requirements.md
└── tasks.md             # /speckit-tasks
```

### Source Code (repository root)

```text
protocol/fixtures/graph-deltas/keys.json          # deltas and their keys; read by Scala and Python

blueprint/
├── src/main/scala/.../blueprint/DeltaTopics.scala        # decide(topic, topicConfig): Compacted | Kept | NotOurs; notes
└── src/test/scala/.../blueprint/DeltaTopicsSuite.scala

cli/src/main/scala/.../cli/
├── Verify.scala             # the delta topic notes
└── ResourceWriter.scala     # cleanup.policy = compact by default for a managed delta topic

sidecar/
├── src/main/scala/.../sidecar/
│   ├── Deltas.scala             # key(d); read(offset, key, value): Delta | Marker; the two refusals
│   ├── Neo4jMergeStage.scala    # uses read; drops and counts markers
│   ├── StageMetrics.scala       # DeleteMarkers
│   └── Metrics.scala            # (the bean interface gains one getter)
├── src/universal/agent/prometheus.yaml           # ankka_flow_stage_delete_markers_total
└── src/test/scala/.../sidecar/
    ├── KafkaSuite.scala             # kafkaEnv hook
    ├── CompactionKafkaSuite.scala   # compaction actually run; rebuild; marker after tombstone
    └── DeltasSuite, Neo4jMergeSuite, Neo4jSinkKafkaSuite, PrometheusRulesSuite   # additions

operator/src/main/scala/.../operator/Rendering.scala    # TopicNotCompacted

sdks/python/
├── src/ankka_flow/graph.py      # GraphDeltaOutlet, read, node_key, edge_key, Delta
├── src/ankka_flow/__init__.py   # exports GraphDeltaOutlet
└── tests/test_graph.py          # the helper, the validations, keys.json

samples/checkout-graph/          # mapper, tests and bench on the helper; README
docs/                            # deploy/rebuild-a-graph.md; edits (research R9)
tools/docs/skill/{ankka-flow,ankka-flow-python,ankka-flow-deploy}/SKILL.md
```

**Structure Decision**: no new module and no new dependency. The decision about a topic is
`blueprint`'s because it is a pure function of a verified topic; the check on a record is the
stage's because only the stage reads deltas; the helper is the SDK's because the contract is the
platform's.

## Complexity Tracking

No violations.

## Phase summary

- **Phase 0** (research.md): nine decisions verified against the code, six items to settle at
  implementation, the first being whether a test can make a broker compact within a minute.
- **Phase 1** (data-model.md, contracts/, quickstart.md): the key and marker rules as an addition
  to the delta contract; the CLI's decision and notes and the operator's event; the SDK's helper;
  the rebuild procedure with what a rebuilt graph does and does not contain; six proving tiers
  ending with the migration of the 0.2.0 pipeline still running on the kind cluster.
- **Phase 2** (`/speckit-tasks`): the key fixture and the sink's check first (US1), with the SDK
  helper beside it; then the CLI's default and the operator's event (US2); then the compaction
  suite and the rebuild guide (US3, US4); then the sample, the docs and the skills.
