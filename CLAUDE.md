# CLAUDE.md

Guidance for Claude Code (claude.ai/code) working in this repository.

## What this is

`ankka-flow` runs streaming pipelines beside [ankka](https://github.com/thinkmorestupidless/ankka).
A pipeline is a set of **streamlets** with typed inlets and outlets, wired by a **blueprint** over
Kafka topics. A streamlet's logic is written in any language and shipped as an image containing only
that code. A **sidecar** the platform injects into every pod owns everything Kafka and talks to the
process over a protobuf protocol on loopback.

It descends from Lightbend's Cloudflow by way of the thinkmorestupidless fork. Neither is a
dependency. Six pieces were carried over with their tests (see `NOTICE`). It follows ankka's
conventions so people move between the two without relearning: the operator's render/execute split,
the protocol directory SDKs copy, the conformance suite, the Python SDK's tooling.

Start with `specs/001-version-one/spec.md` (behaviour), `specs/001-version-one/plan.md` and
`research.md` (decisions and why), and `specs/001-version-one/contracts/` (every interface).

## Commands

Docker is required: the Kafka suites start their own broker, and one suite starts k3s.

```bash
sbt test                                    # everything, including the k3s suite and the sample image it needs
sbt -Dflow.cluster.tests=off test           # everything but k3s, in a few minutes
sbt protocol/test blueprint/test crd/test   # the pure modules, seconds
sbt sidecar/test                            # the conversation, the Kafka graph, the carried Kafka suites, conformance
sbt operator/test                           # rendering, topic resolution, reset, and FlowClusterSuite (k3s)
sbt cli/test
sbt 'sidecar/testOnly *ConformanceSuite'                                             # the Scala reference
sbt 'sidecar/testOnly *ConformanceSuite' -Dflow.conformance.target=127.0.0.1:9010    # a process on a port
sbt mutationCheck                           # SC-006: the commit-after-write suite must FAIL with the commit moved first
sbt docker:publishLocal sampleImage         # ankka-flow-sidecar, ankka-flow-operator, sample-cart-router
sbt cli/stage                               # cli/target/universal/stage/bin/flow
sbt -Dflow.fixtures.regenerate=on 'protocol/testOnly *DescriptorFixturesSuite'   # rewrite protocol/fixtures/descriptors
cd sdks/python && uv sync && uv run python scripts/proto.py && uv run mypy && uv run pytest -q && uv run conformance
sbt scalafmtAll scalafmtSbt                 # format; `just hooks` installs the pre-commit check
```

`just` wraps these (`just --list`). Recipes stay one command each; logic lives in
`kustomization/deploy-local.sh`.

**Tests are serialised deliberately** (`Tags.limit(Tags.Test, 1)`, `Test / parallelExecution :=
false`). Suites that start containers contend when they overlap. Do not undo it.

**Test switches must be forwarded** to the forked test JVM. `forwardedTestSwitches` in `build.sbt`
lists them; a new `-Dflow.*` switch that is not listed there silently does nothing.

A long run on a laptop: `caffeinate -i sbt test`. A sleeping Mac pauses Docker while deadlines keep
counting, and the failure looks like the platform's.

## Rules

| Rule | What it means here |
|---|---|
| Module direction | `protocol` depends on nothing of ours. `blueprint` → `protocol`. `crd` → fabric8 only. `sidecar` → `protocol`. `operator` → `crd`, `blueprint`, `protocol`. `cli` → `blueprint`, `crd`, `protocol`. Nothing depends on `sidecar` or `operator` except the operator's tests reading `StreamletConfig`. |
| The sidecar never decodes | A record's value is bytes from Kafka to the process and back. A contract is a format and a fingerprint, never a type the sidecar understands. |
| Commit after the write | Offsets are committed only once every emit for the batch is confirmed by the broker. `CommitAfterWrite` and its Kafka suite are the guard; `mutationCheck` proves the suite would catch a regression. |
| Never skip | A failed batch is redelivered from the last commit, indefinitely. Skipping a record is the process's decision: acknowledge without emitting. |
| Explicit registration | A streamlet is declared; its SDK writes the descriptor. The sidecar compares discovery with the deployed file and refuses a difference. No classpath scanning, ever. |
| Pure rendering, one place for I/O | `Rendering.render` returns actions; `Fabric8Executor` and `KafkaExecutor` perform them. |
| The resource says what runs | Deploy-time overrides are merged by the CLI into the resource. The operator adds only what only it can know: the sidecar image (its own setting) and Kafka cluster secrets. |
| Kafka credentials reach only the sidecar | Mounted from a Secret into the sidecar container. The process container has no ports, probes, mounts or secrets. |
| No literal image tags in tests | The Kafka image comes from `-Dflow.kafka.image`; built images use `BuildInfo.version` with `+` → `-`. |
| Warning-free compile | `-Wunused:all` is on. Generated ScalaPB sources are silenced by `-Wconf` on `src_managed` only. |
| Not in this build | pekko-http, pekko-grpc, Avro, spray-json, ScalaTest. If a change needs one, it is a design change: update `research.md` first. |

## Carried code

A file derived from Cloudflow keeps Lightbend's header verbatim and arrives with its test, rewritten
as munit with every assertion kept. Add the file to `NOTICE`. The source path in the fork is named in
the task that carried it (`specs/001-version-one/tasks.md`).

## Kubernetes manifests

The canonical manifests live in `kustomization/components/`. The copies the JVM reads from its
classpath (`crd/src/main/resources/ankka-flow/crd/ankkaflow.yaml`,
`operator/src/main/resources/ankka-flow/install/operator.yaml`) are symlinks **into** them. Kustomize's
load restrictor forbids the reverse direction.

Domain for labels, annotations and the CRD group: `flow.ankka.thinkmorestupidless.com`.

## Environment variables

Everything the platform sets is `FLOW_*`. The process gets `FLOW_PROCESS_PORT` (9010) and nothing
else. The sidecar's variables are in `specs/001-version-one/contracts/sidecar.md`; the operator's in
`contracts/resource-and-operator.md`.
