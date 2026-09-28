# Implementation Plan: ankka-flow version one

**Branch**: `001-version-one` | **Date**: 2026-09-28 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/001-version-one/spec.md`, the design treatment it
was written from, `docs/design/version-one.md`, and the two clarification sessions of 2026-09-28
recorded in the spec (a new repository; a Pekko sidecar per pod; indefinite redelivery on failure;
JSON only; full reconcile on change; `replicas` in the resource; one performance floor).

## Summary

Run streaming pipelines whose stages are written in any language. A **streamlet** is a container
holding only the developer's code, speaking a small protobuf protocol over loopback to a
**sidecar** the platform injects, which owns everything Kafka: subscribing, batching, producing,
committing after the write, consumer groups, lag and resets. A streamlet's **descriptor** is
written by its SDK at build time; a **blueprint** wires descriptors through topics and is
**verified** before any pod exists; the CLI emits an **`AnkkaFlow`** resource the **operator**
runs as one two-container Deployment per streamlet. What Lightbend's Cloudflow and its Pekko fork
proved is carried over as six self-contained pieces with their tests and copyright notices:
blueprint verification, JSON contracts by schema name, commit-after-write ordering, consumer-group
reset, topic creation, and client-id naming with the Prometheus rules. Everything is Scala 3 on
ankka's build conventions, plus one Python SDK with a conformance suite that any later SDK must
pass.

## Technical Context

**Language/Version**: Scala 3.9.0, JDK 21, sbt 1.12.15 (ankka's; research R1). Python 3.12 for
the SDK (R15). Protobuf 3 for the protocol.

**Primary Dependencies**: `protocol`: ScalaPB 0.11.11 via `sbt-protoc` 1.0.6, `grpc-netty-shaded`
and `grpc-stub` at ScalaPB's pinned grpc-java, jsoniter-scala 2.40.1 for the descriptor codec (R1,
R5). `blueprint`: Typesafe Config (the version pekko 1.7.0 pins), `protocol`. `crd`: fabric8
`kubernetes-client` 7.9.0, Jackson 2.21.4 + `jackson-module-scala` (matching fabric8). `sidecar`:
pekko 1.7.0, pekko-connectors-kafka 1.2.0 (kafka-clients 3.9.x transitively, pinned explicitly),
`protocol`, logback 1.6.3, `jmx_prometheus_javaagent` in the image. `operator`: `crd`, `blueprint`,
`protocol`, fabric8, kafka-clients (Admin). `cli`: decline 2.6.2, `blueprint`, `crd`, `protocol`,
fabric8 (for `reset` only). Python: `grpcio>=1.84,<2`, `protobuf>=6,<8`, dev `grpcio-tools`,
`pytest`, `mypy --strict`, managed with `uv` and hatchling. **No pekko-http, no pekko-grpc, no
Avro, no spray-json, no ScalaTest** anywhere.

**Storage**: Kafka topics only. Offsets live in Kafka's consumer groups; the reset request and its
done marker are annotations on the `AnkkaFlow`; a streamlet's configuration is a Secret the
operator renders. Nothing else is persisted.

**Testing**: munit 1.3.6 for every Scala module, forked and serialised, `-D` switches forwarded to
the test JVM (ankka's `commonSettings`). testcontainers 1.21.4: Kafka (`apache/kafka:3.9.1`) for
the carried Kafka tests and the sidecar's graph tests; k3s (`rancher/k3s:v1.35.1-k3s1`) for one
operator suite, skipped by `-Dflow.cluster.tests=off`. A scriptable `ProcessDouble` in Scala for
the conversation, so the protocol is proven before any Python exists. The conformance suite is a
munit suite parameterised by target (in-process reference, or a process at
`-Dflow.conformance.target`). Descriptor fixtures are checked byte for byte in both Scala and
Python. A mutation alias proves SC-006. (quickstart.md, tiers 1–5)

**Target Platform**: Kubernetes (kind for development, any cluster with a Kafka reachable from it)
for the operator and pods; Docker Compose on macOS and Linux for the laptop loop; the SDK targets
CPython 3.12 on Linux and macOS. Images are `eclipse-temurin:21-jre` based, published to
`ghcr.io/thinkmorestupidless/ankka-flow-{sidecar,operator}`.

**Project Type**: one sbt build with six projects (`protocol`, `blueprint`, `crd`, `sidecar`,
`operator`, `cli`) of which two are applications with images; one Python project outside sbt
(`sdks/python`); one sample (`samples/cart-router`, Python, with its own image); `kustomization/`
for installing; `docs/`. A new repository, so also `build.sbt`, `project/`, `.scalafmt.conf`,
`.github/workflows/ci.yml`, `Justfile`, `CLAUDE.md`.

**Performance Goals**: SC-008: the Python cart router sustains ≥ 1,000 records/s per inlet
partition on a laptop from the compose file, with the sidecar path adding < 10 ms median latency
per record over a plain consumer; measured by `samples/cart-router/bench.py` and recorded in
research.md. Nothing beyond that floor (spec Out of Scope).

**Constraints**: the sidecar never decodes a value (FR-009); both gRPC ends bind loopback only
(FR-007); at most one batch in flight per (inlet, partition), offsets committed only after every
emit is confirmed (FR-010–012); a stream failure is redelivered indefinitely, never skipped or
dead-lettered (FR-012a); the resource never names the sidecar image (FR-020); Kafka credentials
reach only the sidecar (FR-021); the same declaration yields identical descriptor bytes in every
language (FR-002); every carried piece arrives with its real-Kafka test and its Lightbend header
(SC-007); the `protocol/` directory is copied verbatim into the SDK and CI diffs it (FR-027).

**Scale/Scope**: roughly 120 Scala files (the sidecar's graph, conversation and supervisor, the
operator's rendering and executors, and the carried blueprint module are the large ones), 3 proto
files, ~25 Python modules plus the template and sample, one CRD YAML, one kustomization tree, one
compose file, ~40 munit suites of which 7 need Docker and 1 needs k3s. The riskiest piece is the
sidecar's failure path: revocation while in flight, process restart, sidecar restart, and the
commit boundary under each; the most expensive is the operator's k3s suite.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

`.specify/memory/constitution.md` is the unfilled template and this repository has no `CLAUDE.md`
yet. The principles below are ankka's, from its `CLAUDE.md` and its feature 009 plan, adopted here
because the spec says ankka-flow follows ankka's conventions "so people move between the two
without relearning". The first tasks write `CLAUDE.md` with them; `/speckit-constitution` should
run before the next feature.

| Principle (from ankka) | How this plan keeps it |
|---|---|
| Module dependency direction; the protocol depends on nothing of the platform's | `protocol` depends only on ScalaPB, grpc and jsoniter; `blueprint` on `protocol` and Config; `crd` on fabric8 alone; `sidecar` on `protocol` and Pekko; `operator` on `crd`, `blueprint`, `protocol`; `cli` on `blueprint`, `crd`, `protocol`. No module depends on `sidecar` or `operator`; applications are `publish / skip` |
| Registration is explicit; no classpath scanning | a streamlet is declared and its descriptor written by the SDK; the sidecar checks discovery against a file (FR-001, FR-008). Cloudflow's class scanning is the thing this project exists to remove |
| A pure rendering function; one executor does I/O | `Rendering.render(resource, settings, observed): Either[problems, actions]`; `Fabric8Executor` and `KafkaExecutor` are the only I/O (R13) |
| The operator holds only what the resource says, plus its own configuration | the sidecar image is operator configuration (the same tension ankka named in 009 and resolved the same way); everything else the operator renders is in the resource, including deploy-time overrides merged by the CLI (R12) |
| Wire names are declared separately from code names | port and parameter names are strings in the declaration (`JsonInlet("in", …)`), never Python attribute names |
| A test must never name an image by literal tag | the k3s suite uses `BuildInfo.version` with `+` → `-` and imports images built in the same sbt session |
| Tests are serialised deliberately; Docker suites start their own containers | `Tags.limit(Tags.Test, 1)`, `Test / parallelExecution := false`; testcontainers per suite |
| Effects are inert data | an `Outcome` (emits + ack/fail) is data the sidecar interprets; the process performs no Kafka I/O; the operator's `Action`s are data an executor applies |
| Never touch an actor's context from a callback; every reply re-enters as a message | the `Conversation` completes `Future`s that the stream's `mapAsync` awaits; the `Supervisor` is a plain state machine driven by `Future` completions on the system dispatcher, not an actor with callbacks |
| Warning-free compile with `-Wunused` | generated sources sit in `protocol` with its own `scalacOptions` (`-source:3.3`), as ankka's |
| `RollingUpdate` with surge, readiness by the platform's opinion | one Deployment per streamlet, `maxSurge 1, maxUnavailable 0`, exec readiness on the sidecar only; the process container has no probe |
| Public `docs/` describe what exists; limitations are listed honestly | `docs/` gets concepts, reference and a `limitations.md` naming at-least-once, JSON only, no built-in stages, no Scala SDK |

**One deliberate departure**: ankka's operator writes no Kubernetes Events and reports through
`status.detail` only. The spec requires Events (FR-022) because a topic difference, a refused reset
or a stalled partition is a history, not a state. Both the operator and the sidecar write
`events.k8s.io/v1` Events; the sidecar does so with a raw API call rather than a client library so
its image stays small (R9).

No violations.

## Project Structure

### Documentation (this feature)

```text
specs/001-version-one/
├── plan.md
├── research.md                  # R1–R16, verify-at-implementation, measurements
├── data-model.md                # protocol, descriptor, blueprint, resource, operator, sidecar, CLI, SDK values; lifecycles
├── quickstart.md                # tiers 1–7 and the reviewer's checklist
└── contracts/
    ├── protocol.md              # the three .proto files, conversation by conversation, the README's rules
    ├── descriptor.md            # DESCRIPTOR.md: canonical JSON, fixtures, validation
    ├── sidecar.md               # image, env, files, probes, metrics, failure, local run
    ├── resource-and-operator.md # blueprint.conf, AnkkaFlow, cluster secret, settings, rendering, reconcile, events, RBAC
    ├── cli.md                   # verify, generate, reset, version; every refusal message
    ├── python-sdk.md            # the package's API, descriptor script, harness, template, sample
    └── conformance.md           # the reference streamlet and every case by name
```

### Source Code (repository root)

```text
build.sbt, project/{build.properties, plugins.sbt, Dependencies.scala}, .scalafmt.conf, .githooks/
CLAUDE.md, README.md, LICENSE, Justfile, docker-compose.yml (dev Kafka), .github/workflows/{ci,release}.yml

protocol/                                    # sbt project ankka-flow-protocol; the artefact SDKs copy
├── README.md, DESCRIPTOR.md
├── fixtures/{descriptors,declarations,conversations}/
├── src/main/protobuf/ankka/flow/v1/{payload,discovery,streamlet}.proto
├── src/main/scala/com/thinkmorestupidless/ankka/flow/protocol/
│   ├── DescriptorJson.scala                 # canonical JSON read/write for Spec (R5)
│   ├── DescriptorValidation.scala           # the rules in contracts/descriptor.md → Vector[Problem]
│   ├── Fingerprint.scala                    # Base64(SHA-256(name)) — carried from cloudflow-json (R4)
│   └── ProtocolVersion.scala                # "1.0"; compatible(sidecar, sdk)
└── src/test/scala/.../protocol/{DescriptorJsonSuite, FingerprintSuite, ProtocolVersionSuite}.scala

blueprint/                                   # ankka-flow-blueprint; carried from cloudflow-blueprint (R6)
├── src/main/scala/com/thinkmorestupidless/ankka/flow/blueprint/
│   ├── Blueprint.scala, Topic.scala, StreamletRef.scala, VerifiedBlueprint.scala, BlueprintProblem.scala   # carried, Lightbend headers
│   ├── TopicSettings.scala                  # partitions, replicas, configs, BatchSettings
│   └── Overrides.scala                      # --conf parsing and merge
└── src/test/scala/.../blueprint/
    ├── BlueprintSuite.scala, BlueprintParserSuite.scala, UnmanagedTopicSuite.scala   # carried assertions, munit
    ├── JsonContractSuite.scala              # carried JsonSchemaVerificationSpec
    ├── BlueprintBuilder.scala, DescriptorBuilder.scala   # carried test helpers
    └── OverridesSuite.scala

crd/                                         # ankka-flow-crd
├── src/main/scala/com/thinkmorestupidless/ankka/flow/crd/{AnkkaFlow, AnkkaFlowDefinition, ResetRequest}.scala   # ResetRequest carried from cloudflow-crd
├── src/main/resources/ankka-flow/crd/ankkaflow.yaml   → symlink to kustomization/components/crd/ankkaflow.yaml
└── src/test/scala/.../crd/{CrdSchemaSuite, ResetRequestSuite}.scala

sidecar/                                     # ankka-flow-sidecar: an application and an image
├── src/main/scala/com/thinkmorestupidless/ankka/flow/sidecar/
│   ├── Main.scala, Settings.scala, StreamletConfig.scala, Descriptor.scala
│   ├── Discovery.scala                      # dial, compare, validate, ReportError
│   ├── Conversation.scala                   # one Run stream; correlation, buffering, violations
│   ├── BatchProcessor.scala                 # trait + RemoteProcessor (the FR-018 seam)
│   ├── InletGraph.scala                     # committablePartitionedSource → batches → process → produce → commit
│   ├── CommitAfterWrite.scala               # carried sinkCommittingAfter, OffsetFirstObserved (R8)
│   ├── Producers.scala, Supervisor.scala, Probes.scala, Stalls.scala, EventSink.scala
│   └── Metrics.scala                        # the sidecar MBeans
├── src/main/resources/{application.conf, logback.xml}
├── image/{prometheus.yaml}                  # carried rules + two sidecar rules; the agent jar is fetched by the build
└── src/test/scala/.../sidecar/
    ├── ProcessDouble.scala                  # scriptable process speaking the protocol
    ├── ConversationSuite.scala, SupervisorSuite.scala, StreamletConfigSuite.scala
    ├── InletGraphKafkaSuite.scala, RestartKafkaSuite.scala
    ├── RecordKafkaSuite.scala, SinkCommittingAfterKafkaSuite.scala, ConsumerLagKafkaSuite.scala, PrometheusRulesSuite.scala   # carried
    └── conformance/{ConformanceSuite, ConformanceTarget, ConformanceReference}.scala

operator/                                    # ankka-flow-operator: an application and an image
├── src/main/scala/com/thinkmorestupidless/ankka/flow/operator/
│   ├── Main.scala, Settings.scala, Operator.scala, WorkQueue.scala, PipelineReconciler.scala
│   ├── Rendering.scala, Action.scala, Executor.scala, Fabric8Executor.scala, KafkaExecutor.scala
│   ├── TopicResolution.scala                # cluster secret precedence; carried TopicActions logic (R12, R13)
│   ├── ConsumerGroupReset.scala             # carried, kafka-clients only
│   ├── StreamletFiles.scala                 # descriptor.json + streamlet.conf rendering and hash
│   ├── Labels.scala, Names.scala, Events.scala, LifecycleRules.scala
├── src/main/resources/ankka-flow/install/operator.yaml   → symlink into kustomization/components/operator/
└── src/test/scala/.../operator/
    ├── RenderingSuite, TopicResolutionSuite, ResetRequestSuite, LifecycleRulesSuite, StreamletFilesSuite
    ├── ConsumerGroupResetSuite.scala        # carried; Kafka
    ├── ClusterImages.scala, FlowClusterSuite.scala   # k3s

cli/                                         # ankka-flow-cli; `flow`
├── src/main/scala/com/thinkmorestupidless/ankka/flow/cli/{Main, Verify, Generate, Reset, Version, Descriptors, Images, ResourceWriter, ResetGuards}.scala
└── src/test/scala/.../cli/{VerifySuite, GenerateSuite, CliResetSuite}.scala + src/test/resources/blueprints/

sdks/python/                                 # contracts/python-sdk.md
├── pyproject.toml, uv.lock, README.md, scripts/proto.py, proto/ (copy), template/
├── src/ankka_flow/{__init__, streamlet, ports, parameters, records, descriptor, server, json, _descriptor, _conformance}.py, testkit/, py.typed
└── tests/{test_descriptor_fixtures, test_server, test_harness, test_conformance_reference}.py

samples/cart-router/                         # the spec's router: blueprint, compose, flow/, src/, produce.py, verify.py, bench.py, Dockerfile
kustomization/                               # components/{crd,operator,kafka}, overlays/{local}, kind.yaml, deploy-local.sh
docs/                                        # concepts/{pipelines,sidecar,contracts}.md, reference/{protocol,cli,resource,sidecar,limitations}.md, sdk/python.md, design/version-one.md (updated)
```

**Structure Decision**: six sbt projects mirror ankka's split so a reader of one repository knows
the other: `protocol` is the artefact SDKs consume and depends on nothing of the platform's;
`blueprint` is pure verification so the CLI can run with no Kafka and no cluster (FR-006);
`crd` is the resource model alone so the operator and the CLI share it without sharing anything
else; `sidecar` and `operator` are the two applications that become images; `cli` is the developer's
tool. The Python SDK lives in-tree so the protocol, the fixtures, the conformance suite and the
SDK move in one tag (FR-027). The sample is a directory, not an sbt project, because it is a Python
image; the build's `docker:publishLocal` builds it through a task so the k3s suite can import it.
The kustomization tree is canonical and the two symlinks into it follow ankka's rule that
Kustomize's load restrictor forbids the reverse.

## Complexity Tracking

No constitution violations to justify. Three additions that could look like scope and are not:

| Addition | Why it is in this feature |
|---|---|
| The `ProcessDouble` in Scala | the only way to prove revocation, restart and every protocol violation before an SDK exists, and afterwards the only way to test misbehaviour a correct SDK cannot produce (the `violation.*` conformance cases) |
| Kubernetes Events from the sidecar | FR-012a requires a stalled partition to be a warning event; the sidecar is the only thing that knows, and a raw `POST` with the pod's token is ~60 lines and no dependency |
| Deploy-time overrides merged by the CLI | FR-021's three-layer precedence must leave the resource saying what will run; merging two layers in the CLI and the third (secrets) in the operator is the smallest split that keeps it so |

## Phase summary

- **Phase 0** (research.md): sixteen decisions, all verified against `../ankka` and `../cloudflow`
  or named as assumptions in *Verify at implementation*. No `NEEDS CLARIFICATION` remains.
- **Phase 1** (this plan, data-model.md, contracts/, quickstart.md): done. The spec's assumption on
  the CRD group is amended to the decided value.
- **Phase 2** (`/speckit-tasks`): the natural order is the build skeleton and `protocol` with its
  fixtures; the carried `blueprint` module; the `crd`; the sidecar's conversation and supervisor
  against the double, then its Kafka graph; the operator's rendering, then its executors and the
  k3s suite; the CLI; the Python SDK, harness and sample; the conformance suite against both;
  kustomization, compose, CI, docs and the benchmark. Each carried piece is one task: copy, keep
  the header, convert the test to munit, make it pass here.
