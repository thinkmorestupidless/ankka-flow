# Implementation Plan: a graph merge sink built into the sidecar

**Branch**: `002-graph-merge-sink` | **Date**: 2026-09-29 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/002-graph-merge-sink/spec.md`, the three decisions
of its clarification session (a fixed graph-delta schema; a streamlet with a built-in descriptor;
Neo4j 5 only), and the two settled at review (an unreadable delta fails its batch; the sink creates
its constraints when it can and warns when it cannot).

## Summary

The sidecar gains its first built-in stage: a **graph merge sink** that reads graph deltas — node
merges, edge merges and tombstones in a versioned JSON contract, `ankka.graph-delta.v1` — and
merges each batch into Neo4j 5 in one transaction, committing the inlet's offsets only after the
transaction. Because every delta is a statement of state with a version from its source entity, the
merge is idempotent under redelivery, reordering across sources, restart and a full replay. A
blueprint declares the sink as a streamlet whose descriptor is built in
(`graph = builtin/neo4j-merge-sink`); `flow verify` knows the built-in descriptors and checks the
sink's inlet like any port; `flow generate` needs no image for it and writes `builtin: true`; the
operator renders a pod with only the sidecar and mounts the Neo4j connection Secret the sink's
`secret` parameter names. Inside the sidecar the stage is the second implementation of the
`BatchProcessor` seam version one left for it, behind a small `Stage` abstraction that lets the
supervisor's loop run without a process. A Python sample maps ankka's shopping-cart checkout
notices to deltas in front of the sink; two Neo4j suites, rendering tests and one k3s scenario
prove it; three new documentation pages and the skills carry it.

## Technical Context

**Language/Version**: Scala 3.9.0, JDK 21, sbt 1.12.15, as the build is. Python 3.12 for the
sample. Cypher 5 against Neo4j 5.26 LTS (research R7).

**Primary Dependencies**: new on `sidecar`: `org.neo4j.driver:neo4j-java-driver` 5.28.5 (R7; a
"not in this build" addition recorded there). Everything else as version one: pekko 1.7.0,
pekko-connectors-kafka 1.2.0, kafka-clients 3.9.2, grpc via ScalaPB, fabric8 7.9.0, decline 2.6.2,
Typesafe Config, the protocol's own `Json`. No new JSON library: the stage parses deltas with
`protocol.Json` (R9). Still no pekko-http, pekko-grpc, Avro, spray-json or ScalaTest.

**Storage**: Neo4j, written only, one database per sink named in the connection Secret; Kafka
consumer groups for offsets, as before. The graph holds, per element, `id`, `_version` and
`_deleted` beside the delta's own properties (data-model.md).

**Testing**: munit, forked and serialised, `-Dflow.neo4j.image` forwarded like `-Dflow.kafka.image`
(R7). testcontainers `neo4j` 1.21.4 for `Neo4jMergeSuite` (Neo4j only) and `Neo4jSinkKafkaSuite`
(Kafka and Neo4j, the sidecar in stage mode through `SidecarRun`); `RenderingSuite`,
`StreamletFilesSuite`, `CrdSchemaSuite`, `VerifySuite`/`GenerateSuite`, a new `BuiltinsSuite`; one
new `FlowClusterSuite` scenario on k3s with Neo4j in the cluster; the sample's `Harness` tests and
descriptor check in CI (R12; quickstart.md).

**Target Platform**: as version one — Kubernetes for the operator and pods (kind locally, with a
`kustomization/overlays/neo4j` for a development Neo4j installed by `just neo4j-up`), Docker
Compose for the laptop loop (the sample's compose file adds Neo4j and a sidecar in stage mode).
Images: the sidecar image gains the driver and the stage; the sample's mapper image is published as
`ghcr.io/thinkmorestupidless/sample-checkout-graph`.

**Project Type**: the existing sbt build; every module but `crd` changes (`protocol`: built-in
descriptors; `blueprint`: the `builtin/` lookup; `crd`: the `builtin` field; `sidecar`: the stage;
`operator`: sidecar-only rendering and the Secret; `cli`: no image for built-ins), plus one Python
sample, one kustomization overlay, three docs pages.

**Performance Goals**: SC-007: ≥ 1,000 deltas/s per inlet partition against a local Neo4j with
one transaction per batch, measured by `samples/checkout-graph/bench.py` on the laptop compose file
and recorded in research.md. The inlet's batch bounds (100 records, 1 MiB by default) bound the
transaction.

**Constraints**: a batch is one transaction, applied entirely or not at all, and committed to Kafka
only after Neo4j confirms (FR-011, FR-012); a delta the stage cannot read fails its batch, never
skips (FR-015); the version guard is strict (`_version < version`) and ties are stale (FR-007–010);
the connection Secret is mounted into the sidecar and appears nowhere in the resource (FR-022); a
built-in streamlet's pod has one container (FR-021); the sidecar refuses a deployed descriptor that
is not its own built-in (R3); `protocol/` stays byte-identical to the SDK's copy, so the built-in
descriptor's canonical JSON is a committed fixture under a new directory (R4); the "sidecar never
decodes" rule is narrowed to the process path (R14).

**Scale/Scope**: roughly 12 new Scala files (`Builtins`, `Stage`/`ProcessStage`,
`Neo4jMergeStage`, `Deltas` (parse and fold), `Neo4jSecret`, `StageMetrics`, the two suites, the
CLI and operator test additions) and edits to ~15 (`StreamletRef`, `BlueprintProblem`, `Verify`,
`Main`, `ResourceWriter`, `AnkkaFlow`, the CRD YAML, `Supervisor`, `StreamletConfig`, `Settings`,
`Metrics`, `Rendering`, `Observed`, `Fabric8Executor`, `StreamletFiles`, `LifecycleRules`); 3 new
Prometheus rules; one Python sample of ~5 files; one kustomization overlay; 3 new and ~10 changed
docs pages; 4 skills re-rendered.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

`.specify/memory/constitution.md` is the unfilled template, so the gate is the rules table in
`CLAUDE.md`, checked rule by rule:

| Rule | This feature |
|---|---|
| Module direction | Kept. `Builtins` lives in `protocol` so `blueprint`, `cli`, `operator` and `sidecar` reach it without a new edge; nothing depends on `sidecar` or `operator`. |
| The sidecar never decodes | **Narrowed, deliberately.** The stage decodes its own contract and nothing else; the process path is unchanged. The rule's text changes with this feature (R14) and the spec's Context says why. Listed under Complexity Tracking. |
| Commit after the write | Kept and extended: the Neo4j transaction is "the write"; `CommitAfterWrite` is untouched and `mutationCheck` still guards it (R1). |
| Never skip | Kept: an unreadable delta fails its batch and stalls the partition (FR-015); nothing is dead-lettered. |
| Explicit registration | Kept in a new form: the built-in descriptor is a declared value, and the sidecar refuses a deployed descriptor that differs from its own (R3). No scanning. |
| Pure rendering, one place for I/O | Kept: `Rendering.render` gets the observed Secret through `Observed` and returns the mount and the refusal as actions; `Fabric8Executor` reads the Secret (R6). |
| The resource says what runs | Kept: `builtin: true` and the `secret` parameter are in the resource; the operator adds only the sidecar image, Kafka clusters and the Secret mount (R5, R6). |
| Kafka credentials reach only the sidecar | Extended to Neo4j's: mounted into the sidecar container, never in the resource; there is no process container to keep them from (R6). |
| No literal image tags in tests | Kept: `-Dflow.neo4j.image` from `V.neo4jImage` (R7). |
| Warning-free compile | Kept; the driver adds no generated sources. |
| Not in this build | pekko-http, pekko-grpc, Avro, spray-json, ScalaTest: none added. The Neo4j driver is a new dependency recorded in research.md first, as the rule requires (R7). |

**Post-design re-check**: unchanged. The one deliberate narrowing (decoding) is justified below;
every other rule holds as written or is extended in its own spirit.

## Project Structure

### Documentation (this feature)

```text
specs/002-graph-merge-sink/
├── plan.md              # This file
├── research.md          # Phase 0: R1–R14 and the verify-at-implementation list
├── data-model.md        # Phase 1: the delta, the element, the stage's files, the resource field
├── quickstart.md        # Phase 1: six tiers, cheapest first
├── contracts/
│   ├── graph-delta.md          # the wire contract every mapper writes
│   ├── neo4j-merge-sink.md     # the sink: descriptor, parameters, Secret, statements, metrics, events
│   └── built-in-streamlets.md  # blueprint, CLI, resource and operator behaviour for a built-in
├── checklists/requirements.md
└── tasks.md             # Phase 2 (/speckit-tasks)
```

### Source Code (repository root)

```text
protocol/
├── src/main/scala/.../protocol/Builtins.scala          # neo4jMergeSink: Spec; all
├── src/test/scala/.../protocol/BuiltinsSuite.scala     # canonical JSON byte-equal, validates
└── fixtures/builtin/neo4j-merge-sink.json              # committed; copied into sdks/python/proto by scripts/proto.py

blueprint/src/main/scala/.../blueprint/
├── Descriptors.scala        # StreamletDescriptor(proto, builtin = false)
├── StreamletRef.scala       # "builtin/" + name lookup; UnknownBuiltin
└── BlueprintProblem.scala   # UnknownBuiltin, BuiltinHasImage messages

crd/
├── src/main/scala/.../crd/AnkkaFlow.scala             # StreamletSpec.builtin: Boolean = false
└── (kustomization/components/crd/ankkaflow.yaml)      # builtin property; image no longer required

cli/src/main/scala/.../cli/
├── Verify.scala             # Builtins.all appended; --descriptors optional
├── Main.scala               # no image needed for a built-in; an image given for one is refused
└── ResourceWriter.scala     # builtin = true, image = ""

sidecar/
├── src/main/scala/.../sidecar/
│   ├── Stage.scala              # trait Stage; ProcessStage (today's discovery + Conversation)
│   ├── Neo4jMergeStage.scala    # open (connectivity, version, descriptor, constraint), process, close
│   ├── Deltas.scala             # parse one record → Delta; fold a batch; the problems
│   ├── Neo4jSecret.scala        # uri/username/password/database from the credentials directory
│   ├── StageMetrics.scala       # per-partition written/stale/failed beans
│   ├── Supervisor.scala         # loop over a Stage; ready = inlets subscribed ∧ stage ready
│   ├── StreamletConfig.scala    # flow.stage { name, neo4j { credentials-dir } }
│   ├── Settings.scala           # FLOW_PROCESS_ADDRESS optional in stage mode
│   └── Metrics.scala            # registers the stage beans
├── src/universal/agent/prometheus.yaml   # three new rules
└── src/test/scala/.../sidecar/
    ├── Neo4jSuite.scala             # trait: the Neo4j container from -Dflow.neo4j.image, driver, pause/unpause
    ├── Neo4jMergeSuite.scala        # the stage alone
    ├── Neo4jSinkKafkaSuite.scala    # the sidecar in stage mode, end to end
    ├── SidecarRun.scala             # stage-mode variant
    └── PrometheusRulesSuite.scala   # the new rules

operator/
├── src/main/scala/.../operator/
│   ├── Observed.scala           # secrets: Map[String, SecretState]; secretProblems
│   ├── Fabric8Executor.scala    # observe fetches each built-in streamlet's Secret
│   ├── Rendering.scala          # sidecar-only pod; neo4j volume + mount; refusals; hash includes resourceVersion
│   ├── StreamletFiles.scala     # the stage block in streamlet.conf
│   └── LifecycleRules.scala     # unchanged in behaviour; built-in image is ""
└── src/test/scala/.../operator/
    ├── RenderingSuite.scala, StreamletFilesSuite.scala, Fixtures.scala   # built-in fixture and Secret
    └── FlowClusterSuite.scala   # one scenario: Neo4j in k3s, the sink pod, a delta in the graph

samples/checkout-graph/          # Python mapper + the sink; compose with Neo4j; bench.py; README
kustomization/overlays/neo4j/    # a development Neo4j and the sample Secret; `just neo4j-up`
docs/                            # reference/graph-deltas.md, reference/neo4j-merge-sink.md, build/graph-sink.md; edits
tools/docs/skill/*/SKILL.md      # rules and pages
build.sbt, project/Dependencies.scala, Justfile, .github/workflows/{ci,release}.yml, CLAUDE.md
```

**Structure Decision**: no new module. The stage is sidecar code because it runs in the sidecar's
image and stream; the built-in descriptor is protocol code because four modules read it and
`protocol` depends on nothing of ours. The sample follows `samples/checkout-feed`; the development
Neo4j is an overlay of its own so `just up` stays what it is.

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| The sidecar decodes record values, in the stage only | A merge sink cannot write a graph from bytes; the whole point of a built-in stage is to do generic work the platform owns, and this work is on the record's content. | A process-hosted sink written against the SDK would keep the rule intact, but then every graph pipeline ships a container with a database driver in it, in every language, and idempotence is each author's problem again — exactly what the feature removes. The rule is narrowed to the process path, where it still holds without exception. |

## Phase summary

- **Phase 0** (research.md): fourteen decisions, all verified against this repository or a public
  source; eight assumptions to settle at implementation, each named with the task that settles it.
- **Phase 1** (data-model.md, contracts/, quickstart.md): the delta and element models; three
  contracts — the wire format, the sink, and what a built-in streamlet means to the blueprint, the
  CLI, the resource and the operator; six proving tiers.
- **Phase 2** (`/speckit-tasks`): tasks in dependency order — protocol and blueprint first (the
  descriptor and the lookup), then the CLI, the CRD and the operator (so a resource can exist), then
  the sidecar stage against Neo4j, then the sample, the overlay, the docs and the skills.
