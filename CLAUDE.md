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
sbt sdk/test                                # the Scala SDK: the descriptor fixtures, the server, the harness
sbt sdkConformance                          # the conformance suite against the Scala SDK, in process
sbt 'sidecar/testOnly *ConformanceSuite' -Dflow.conformance.target=127.0.0.1:9010    # a process on a port
sbt 'sdk/runMain com.thinkmorestupidless.ankka.flow.sdk.conformance.ConformanceMain 9010'   # the Scala SDK's reference, on a port
sbt mutationCheck                           # SC-006: the commit-after-write suite must FAIL with the commit moved first
sbt docker:publishLocal sampleImage         # ankka-flow-sidecar, ankka-flow-operator, sample-cart-router
sbt cli/stage                               # cli/target/universal/stage/bin/flow (the JVM build)
just cli-native                             # the native flow: GraalVM 25 (GRAALVM_HOME), cli/target/graalvm-native-image/flow
sbt cli/test -Dflow.cli.binary=$PWD/cli/target/graalvm-native-image/flow   # the same suite, driven through the binary
cli/native-smoke.sh cli/target/graalvm-native-image/flow "" cli/target/universal/stage/bin/flow   # what an image silently loses, byte-compared
sbt -java-home $GRAALVM_HOME cli/test -Dflow.cli.agent=on   # regenerate the image's reachability metadata (then prune; see its README)
sbt -Dflow.fixtures.regenerate=on 'protocol/testOnly *DescriptorFixturesSuite'   # rewrite protocol/fixtures/descriptors
cd sdks/python && uv sync && uv run python scripts/proto.py && uv run mypy && uv run pytest -q && uv run conformance
sbt scalafmtAll scalafmtSbt                 # format; `just hooks` installs the pre-commit check
just docs                                   # check every page and build the docs site (uv)
just docs-sync                              # refresh included samples, the protocol table, the skills
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
| Module direction | `protocol` depends on nothing of ours. `sdk` → `protocol`. `blueprint` → `protocol`. `crd` → fabric8 only. `sidecar` → `protocol`, and `sdk` in tests only (the conformance suite serves the SDK's reference streamlet). `cartRouterScala` → `sdk`. `operator` → `crd`, `blueprint`, `protocol`. `cli` → `blueprint`, `crd`, `protocol`. Nothing depends on `sidecar` or `operator` except the operator's tests reading `StreamletConfig`. |
| The sidecar never decodes for a process | A record's value is bytes from Kafka to the process and back. A contract is a format and a fingerprint, never a type the sidecar understands. A built-in stage decodes its own contract and nothing else. |
| Commit after the write | Offsets are committed only once every emit for the batch is confirmed by the broker. `CommitAfterWrite` and its Kafka suite are the guard; `mutationCheck` proves the suite would catch a regression. |
| Never skip | A failed batch is redelivered from the last commit, indefinitely. Skipping a record is the process's decision: acknowledge without emitting. |
| Explicit registration | A streamlet is declared; its SDK writes the descriptor. The sidecar compares discovery with the deployed file and refuses a difference. No classpath scanning, ever. |
| Pure rendering, one place for I/O | `Rendering.render` returns actions; `Fabric8Executor` and `KafkaExecutor` perform them. |
| The resource says what runs | Deploy-time overrides are merged by the CLI into the resource. The operator adds only what only it can know: the sidecar image (its own setting) and Kafka cluster secrets. |
| Kafka credentials reach only the sidecar | Mounted from a Secret into the sidecar container. The process container has no ports, probes, mounts or secrets. |
| No literal image tags in tests | The Kafka image comes from `-Dflow.kafka.image` and Neo4j's from `-Dflow.neo4j.image`; built images use `BuildInfo.version` with `+` → `-`. |
| Two Scala versions | `protocol`, `sdk` and the Scala sample compile with `V.scalaLts` (the Scala 3 LTS line, `ltsSettings`), so any Scala 3.3+ project can depend on what is published; every other module stays on `V.scala`. A published module never depends on a 3.9-only one. |
| Warning-free compile | `-Wunused:all` is on. Generated ScalaPB sources are silenced by `-Wconf` on `src_managed` only. |
| Built-in stages are declared values | A stage the sidecar runs with no process has its descriptor in `protocol/.../Builtins.scala`, its canonical JSON in `protocol/fixtures/builtin/`, and a blueprint names it `builtin/<name>`. The sidecar refuses a deployed descriptor that is not its own built-in. |
| One `flow`, two drivers | `cli/test` is one suite; `-Dflow.cli.binary=<path>` runs every case through that executable instead of in process. No case is skipped or conditional on the driver. The release runs it against each platform's binary and byte-compares the smoke outputs with the JVM build. |
| The native image's metadata is generated, then pruned | `cli/src/main/resources/META-INF/native-image/…/reachability-metadata.json` comes from the tracing agent (`-Dflow.cli.agent=on`, GraalVM as the JVM) and is cut down to what `flow` uses; its README says how. Regenerate on a fabric8 or Jackson upgrade, a new command, or a binary failing on a class or resource it cannot find. |
| The tap is shared | `thinkmorestupidless/homebrew-tap` holds ankka's formula and ours. The `homebrew` job clones it, writes `Formula/ankka-flow.rb` and pushes an ordinary commit; never a subtree split or a force push, which would erase the other. A pre-release tag (with a hyphen) publishes the binaries and the formula only. |
| No logback in the CLI | `flow` logs nothing: `slf4j-nop`, so no classpath logging configuration has to survive the native image. fabric8 runs over `kubernetes-httpclient-jdk` there, with the Vert.x client excluded (Netty cannot be built as it comes). |
| Not in this build | pekko-http, pekko-grpc, Avro, spray-json, ScalaTest. If a change needs one, it is a design change: update `research.md` first. |

## Documentation

One tree, `docs/`, of plain Markdown with YAML frontmatter, built by ankka's docs tool (a `uv`
dependency named in `tools/docs/pyproject.toml`, pinned by its `uv.lock`; the settings are
`extra.docs` in `mkdocs.yml`): the MkDocs Material site at flow.ankka.cloud (GitHub Pages, from
`main`, by `.github/workflows/docs.yml`), `llms.txt`, `llms-full.txt`, Markdown per page,
`docs-index.json`, and four Agent Skills curated in `tools/docs/skill/<name>/SKILL.md` and rendered
into `marketplace/plugins/ankka-flow/skills/` (committed; `docs check` fails when stale; a tag
publishes the plugin to `thinkmorestupidless/ankka-marketplace`). The rules are ankka's, on
`docs/contributing/documentation.md` here and in full at docs.ankka.cloud/contributing/documentation/.
The ones that bite:

- **A page stands alone and tells no history.** No positional phrases, no FR/SC/T numbers, no `specs/`
  paths; `docs check` refuses both. `specs/` and `notes/` are records, not pages.
- **Samples are included from tested code** between `# docs:start name` and `# docs:end name`
  markers, named by `<!-- include: path#name -->` before the block (no `#name` includes the whole
  file); `just docs-sync` copies them and `docs check` fails on drift. Markers live in the three
  samples (`cart-router`, `checkout-feed`, `checkout-graph`), never in `protocol/` (the SDKs copy it
  byte for byte). `README.md` includes the same way, refreshed by `just readme-sync` and checked by
  the docs workflow, since the docs tool reads only the pages.
- **The RPC table on `reference/protocol.md` is generated** from `protocol/src/main/protobuf`.
- **A new page goes in `mkdocs.yml`'s `nav` and in a skill's `pages:` list**, or `docs check` fails.
- **A behaviour change is a docs change.** The pages restate CLI flags, events, env vars and protocol
  rules; change them in the same commit.

## Living features

**Specs from feature 004 on keep their acceptance scenarios in living features**, not in the spec.
The [speckit-bdd](https://github.com/thinkmorestupidless/speckit-bdd) extension and preset are
installed under `.specify/`: `/speckit-specify` writes a spec whose acceptance scenarios *name*
scenarios, the `after_specify` hook runs `/speckit-bdd-features` to write them as Gherkin under
`features/<area>/` with every word they use in the root `GLOSSARY.md`, and the `before_clarify` hook
runs `/speckit-bdd-check`, which turns undefined words, refused synonyms, contradictions and untraced
requirements into clarification questions. `specs-from: "004"` in
`.specify/extensions/bdd/bdd-config.yml` leaves specs 001–003 as they were written. `just features`
(and CI's `features` job) runs the same checker from the same config, and fails when it read nothing.
The checker runs through `uvx`, so `uv` must be on `PATH`. Glossary terms follow
`docs/reference/glossary.md` where the docs already define a word.

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
