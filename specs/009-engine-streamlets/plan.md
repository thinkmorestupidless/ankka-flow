# Implementation Plan: Engine Streamlets

**Branch**: `009-engine-streamlets` | **Date**: 2026-11-22 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/009-engine-streamlets/spec.md`

## Summary

A streamlet becomes logic written against one engine's API and run by that engine. The descriptor
gains an `engine` and loses the protocol (R3); a contract becomes a name and an optional
fingerprint computed as ankka computes it (R4). Two SDKs give the logic its ports as the engine's
own sources and sinks from one settings file (R5): Pekko Streams in Scala, owning the read, the
write and the commit so that commit after write holds (R7); Flink in Java, where everything
between `read` and `write` is Flink's (R8). The operator renders a Deployment for the first (R9)
and a `FlinkDeployment` for the second (R10), refuses a Flink streamlet where the Flink operator
or the state store is missing (R11), resets state by advancing an epoch (R12), and reports lag,
grants and conditions on the resource's status for every engine alike (R14, R16, R17). A
blueprint names an ankka topic by project and name; `flow verify` checks its contract against a
file exported from ankka, and the pipeline reaches ankka's broker as a registered machine through
a Kafka cluster Secret (R16). `flow local` writes the settings for a laptop (R15). A sample
pipeline, a Pekko Streams cleaner feeding a Flink counter, is the source of the documentation
and runs in the cluster suite (R19). Version one is then removed (R18).

## Technical Context

**Language/Version**: Scala 3.9.0 (blueprint, crd, operator, cli); Scala 3.3 LTS (descriptor, the
Pekko Streams SDK and sample); Java 17 by `--release` on JDK 21 (the Flink SDK, testkit and
sample)

**Primary Dependencies**: Pekko 1.7.1, pekko-connectors-kafka 1.2.0, kafka-clients 3.9.2 (as
now); added: Flink 2.2.1 (provided), flink-connector-kafka 5.0.0-2.2, jackson-databind for the
Flink SDK's JSON codec, sbt-jupiter-interface; on a cluster the Flink Kubernetes Operator 1.16.1.
Removed: ScalaPB, grpc-java, the JMX exporter, the Neo4j driver

**Storage**: Kafka topics; a Flink streamlet's state in the cluster's state store (any URI Flink
writes; MinIO in the suite); settings in Secrets

**Testing**: munit for the Scala modules; JUnit 5 for the Java ones; testcontainers Kafka for the
Pekko Streams delivery suites and `mutationCheck`; Flink's local environment for the harness;
`FlowClusterSuite` on k3s with the Flink operator and MinIO; the features checker; `just docs`

**Target Platform**: Kubernetes 1.21 or later with the Flink Kubernetes Operator for Flink
streamlets; a laptop with Docker for the walkthrough

**Project Type**: two libraries, an operator, a CLI, a sample, documentation

**Performance Goals**: none new. Conditions appear within one minute of their cause (SC-004), set
by the 30 s observe pass

**Constraints**: no ankka artefact as a dependency; no BSL licence on the path (V9); no
classpath scanning; pure rendering with I/O in the two executors; warning-free; the suites stay
green between tasks, which is why removal is the last phase; nothing already published is
republished

**Scale/Scope**: about 9 new source files in `descriptor`, 12 in the Pekko Streams SDK, 14 Java
files in the Flink SDK and testkit, 6 in the sample; `Rendering`, `LifecycleRules`, `Observed`,
`Fabric8Executor`, `KafkaExecutor`, `Reset` and `StreamletFiles` in the operator reworked;
`Verify`, `Generate`, `ResourceWriter`, `mcp/Cluster`, `mcp/FlowTools` in the CLI reworked and
`Local` added; 3 modules, 4 samples, 2 CI/release jobs and about 15 pages removed; about 10
pages written or rewritten; 5 skills become 4

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

`.specify/memory/constitution.md` is the unfilled template; the gate is `CLAUDE.md`'s rules
table and the family's principles.

| Rule | This feature |
|---|---|
| Module direction | Kept, renamed: `descriptor` depends on nothing of ours; `pekkoSdk`, `blueprint` → `descriptor`; `flinkSdk` depends on nothing of ours; `flinkTestkit` → `flinkSdk`; `operator` → `crd`, `blueprint`, `descriptor`; `cli` → `blueprint`, `crd`, `descriptor`. The operator's test edge to `sidecar` goes. |
| The sidecar never decodes for a process | Retired with the sidecar. Its successor: the platform never decodes a record; a port's codec is the streamlet's. |
| Commit after the write | Kept, moved: the Pekko Streams SDK owns it; `CommitAfterWrite` and its Kafka suite move with their headers; `mutationCheck` points at the moved suite. |
| Never skip | Kept for both engines (R7, data-model's codec rule). |
| Explicit registration | Kept: a main names its streamlet; the SDK writes the descriptor. |
| Pure rendering, one place for I/O | Kept: a `FlinkDeployment`, lag, grants and the Flink CRD are observed by the executors and rendered from `Observed`. |
| The resource says what runs | Kept: engine, parallelism and Flink's resources are in the resource; the operator adds only the state store and cluster Secrets. |
| Kafka credentials reach only the sidecar | **Changed** (R6): a streamlet holds the credentials of its own topics. Stated in `CLAUDE.md`, the limitations page and Complexity Tracking. |
| No literal image tags in tests | Kept: `-Dflow.flink.image`, `-Dflow.flink.operator.image`, `-Dflow.minio.image` join `-Dflow.kafka.image`, all forwarded. |
| Two Scala versions | Kept: `descriptor`, `pekkoSdk`, `cartCleaner` on the LTS line. |
| Warning-free compile | Kept; `-Xlint:all -Werror` for the Java modules. |
| Built-in stages are declared values | Retired: no built-in stage. |
| One `flow`, two drivers | Kept: `flow local` is in the one suite; the reachability metadata is regenerated after `init` goes and `local` arrives. |
| `flow init`'s templates are plain files | Retired with `flow init`. |
| `flow mcp` touches one cluster | Kept: lag now comes from the resource's status, so no port-forward. |
| Not in this build | Kept: no pekko-http, pekko-grpc, Avro, spray-json, ScalaTest. Flink and JUnit 5 are added and recorded in research (R1, R8). |
| Principle 1, no BSL | V9 before the Flink modules are written. |
| Principle 7, only a tag publishes; nothing republished | The removed artefacts get no release step; the new ones take the Scala SDK job's steps under new coordinates. |
| Principle 8, the honest list | The limitations page states delivery per engine, the credentials change, state left after a reset, no metrics endpoint, ankka's token and connection limits. |

**Post-design re-check**: no new violation. The one changed rule is justified below.

## Project Structure

### Documentation (this feature)

```text
specs/009-engine-streamlets/
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   ├── descriptor.md
│   ├── streamlet-settings.md
│   ├── sdks.md
│   ├── cli.md
│   └── resource-and-operator.md
└── tasks.md             # /speckit-tasks
```

### Source Code (repository root)

```text
descriptor/                               # was protocol/
├── DESCRIPTOR.md
├── fixtures/{descriptors,declarations,contracts}/
└── src/{main,test}/scala/…/flow/descriptor/
    Descriptor.scala  DescriptorJson.scala  DescriptorValidation.scala
    Contract.scala  Canonical.scala  Json.scala  Settings.scala (the properties format, written and read)

sdks/pekko-streams/src/{main,test}/scala/…/flow/pekko/
    PekkoStreamlet.scala  Ports.scala  Codec.scala  Record.scala  Parameters.scala  Context.scala
    Runner.scala  PekkoStreamlets.scala  CommitAfterWrite.scala  Probes.scala  PekkoHarness.scala
sdks/flink/src/{main,test}/java/…/flow/flink/
    FlinkStreamlet.java  Inlet.java  Outlet.java  Contract.java  Codec.java  Record.java  Header.java
    Parameter.java  FlinkContext.java  FlinkStreamlets.java  Settings.java  DescriptorWriter.java  Canonical.java
sdks/flink-testkit/src/{main,test}/java/…/flow/flink/testkit/FlinkHarness.java

blueprint/   Blueprint.scala  Topic.scala  BlueprintProblem.scala  Descriptors.scala  Overrides.scala
             AnkkaTopics.scala (new)  — DeltaTopics.scala, TopicSettings' batch removed
crd/         AnkkaFlow.scala  ResetRequest.scala (state epochs)  kustomization/components/crd/ankkaflow.yaml
operator/    Rendering.scala  FlinkRendering.scala (new)  StreamletFiles.scala  Observed.scala
             LifecycleRules.scala  Conditions.scala (new)  Reset.scala  KafkaExecutor.scala (lag, grants)
             Fabric8Executor.scala (FlinkDeployment, CRD, flink-state)  TopicResolution.scala (machine)
             StateStore.scala (new)  Settings.scala  — BuiltinStages.scala removed
cli/         Main.scala  Verify.scala  Generate.scala  ResourceWriter.scala  Local.scala (new)
             AnkkaDeclarations.scala (new)  mcp/Cluster.scala  mcp/FlowTools.scala
             — Init.scala, Scaffold.scala, src/main/templates/ removed

samples/cart-pipeline/
├── blueprint.conf   flow/{cleaner,counter}.json   images.conf   produce.sh   verify.sh
├── schemas/cart-events.v1.json
├── k8s/{in-cluster.conf, kafka-cluster-ankka.yaml, flink-state.yaml}
├── ankka/blueprint.conf                   # the same pipeline on an ankka project's topics
├── cleaner/   src/{main,test}/scala/…/Cleaner.scala, Main.scala, CleanerSuite.scala
└── counter/   src/{main,test}/java/…/CartCounter.java, CartCounterTest.java;  Dockerfile

kustomization/components/{crd,operator,kafka,minio (new)}   overlays/local   deploy-local.sh
operator/src/test/resources/flink-operator.yaml            # rendered from the chart, committed
docs/        concepts/engines.md (new)  build/pekko-streams-streamlet.md  build/flink-streamlet.md
             reference/pekko-streams-sdk.md  reference/flink-sdk.md  reference/streamlet-settings.md (new)
             and every surviving page rewritten where it names the sidecar
tools/docs/skill/{ankka-flow, ankka-flow-deploy, ankka-flow-pekko-streams, ankka-flow-flink}/

removed:  sidecar/  sdks/scala/  sdks/python/  samples/{cart-router,cart-router-scala,checkout-feed,checkout-graph}/
          protocol/src/main/protobuf  kustomization/overlays/neo4j
```

**Structure Decision**: the repository keeps its shape, one sbt build of small modules with the
direction above. Three modules are replaced by four, of which two are Java so that a Flink
streamlet's classpath holds no Scala. The sample is one directory with one blueprint and one
module per streamlet, because one blueprint over two engines is the thing being shown.

### Order of work

1. Setup and verify-first (V1 to V11), in research before code.
2. Foundation beside version one: `descriptor` (new model, contract, settings), then blueprint,
   crd and the CLI's reading of it. Version one's descriptors stop verifying here; its modules
   keep compiling against the old `protocol` until phase 8, so `protocol` and `descriptor` exist
   side by side until then.
3. Story 1: the Pekko Streams SDK, the cleaner, the operator's Deployment, `flow local`.
4. Story 2: the Flink SDK and testkit, the counter, `FlinkRendering`, the prerequisites, state
   epochs.
5. Story 3: the mixed blueprint end to end, lag and conditions on the status, `flow mcp`.
6. Story 4: ankka topics in the blueprint and the CLI, the machine's cluster, grants.
7. Story 5: the engines page and the limitations page, with every page of the new SDKs.
8. Story 6: removal, the release workflow, `CLAUDE.md`, the README.
9. Polish: quickstart tiers 2 and 3, the soak, the native image's metadata.

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| A streamlet's container holds broker credentials (rule "Kafka credentials reach only the sidecar") | An engine reads and writes Kafka itself; that is what "run by the engine" means | Keeping a proxy between the engine and Kafka is the sidecar again, and Flink's exactly-once needs the producer in the job |
| Two languages for two SDKs | Flink's first-class API and its state serializers are Java (clarified) | One Scala SDK over both would put Scala's library and a generic serializer into every Flink job's state |
| `protocol` and `descriptor` side by side for phases 2 to 7 | Keeps every suite green while the replacement is built | Removing first leaves the operator and CLI uncompilable for most of the build |
| An operator under ours (Flink's) | Running Flink jobs on Kubernetes with upgrades from state is that operator's job | Rendering Flink's pods ourselves re-implements it, and is where Cloudflow cost most |
