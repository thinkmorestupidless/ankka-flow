# CLAUDE.md

Guidance for Claude Code (claude.ai/code) working in this repository.

## What this is

`ankka-flow` runs streaming pipelines beside [ankka](https://github.com/thinkmorestupidless/ankka).
A pipeline is a set of **streamlets** with typed inlets and outlets, wired by a **blueprint** over Kafka
topics. A streamlet is written against one **engine**'s own API and run by that engine: **Pekko
Streams** (Scala, `sdks/pekko-streams`) for a stage that needs neither windows nor state, **Apache
Flink** (Java, `sdks/flink`, tested with `sdks/flink-testkit`) for one that does. Its SDK writes a
**descriptor** naming the engine; `flow` checks blueprints against descriptors; the operator runs a
Pekko Streams streamlet as a Deployment and a Flink streamlet as a FlinkDeployment that the Apache
Flink Kubernetes Operator runs, with its checkpoints in a state store. A blueprint reads and writes an
ankka project's topics by `ankka = "<project>/<name>"`, as a registered machine.

It descends from Lightbend's Cloudflow by way of the thinkmorestupidless fork. Neither is a
dependency; the carried pieces are listed in `NOTICE`. It follows ankka's conventions so people move
between the two without relearning: the operator's render/execute split, the conformance of SDKs to
shared fixtures, the docs tool and the marketplace plugin.

Start with `specs/009-engine-streamlets/spec.md` (behaviour), `plan.md` and `research.md` (decisions
and why), `contracts/` (every interface), and the living features under `features/`.

## Commands

Docker is required: the Kafka suites start their own broker, and one suite starts k3s.

```bash
sbt test                                    # everything, including the k3s suite and the images it needs
sbt -Dflow.cluster.tests=off test           # everything but k3s, in a few minutes
sbt descriptor/test blueprint/test crd/test # the pure modules, seconds
sbt pekkoSdk/test flinkSdk/test flinkTestkit/test   # the SDKs (`just sdks` adds mutationCheck)
sbt mutationCheck                           # the commit-after-write suite must FAIL with the commit moved first
sbt cartCleaner/test cartCounter/test       # the sample's own tests, which the docs include
sbt cartCleaner/descriptorCheck cartCounter/descriptorCheck   # the committed descriptors are what the code declares
sbt operator/test                           # rendering, topics, lag, grants, reset, and FlowClusterSuite (k3s)
sbt operator/test -Dflow.flink.tests=off    # the cluster suite without its Flink cases
sbt operator/test -Dflow.cluster.soak=on    # ten minutes of pod kills under load
sbt cli/test                                # verify, generate, local, reset, the ankka topics, mcp
sbt cli/stage                               # cli/target/universal/stage/bin/flow (the JVM build)
just cli-native                             # the native flow: GraalVM 25 (GRAALVM_HOME), cli/target/graalvm-native-image/flow
sbt cli/test -Dflow.cli.binary=$PWD/cli/target/graalvm-native-image/flow   # the same suite, driven through the binary
cli/native-smoke.sh cli/target/graalvm-native-image/flow "" cli/target/universal/stage/bin/flow   # what an image silently loses, byte-compared
sbt -java-home $GRAALVM_HOME cli/test -Dflow.cli.agent=on   # regenerate the image's reachability metadata (then prune; see its README)
sbt -Dflow.fixtures.regenerate=on 'descriptor/testOnly *DescriptorFixturesSuite'   # rewrite descriptor/fixtures/descriptors
just images                                 # ankka-flow-operator, sample-cart-cleaner, sample-cart-counter
just up                                     # kind, the CRD, both operators, Kafka, MinIO and flink-state
just flink-operator-manifest                # render the Flink operator's chart into operator/src/test/resources
sbt scalafmtAll scalafmtSbt                 # format; `just hooks` installs the pre-commit check
just features                               # the living features against the glossary and the specs
just docs                                   # check every page and build the docs site (uv)
just docs-sync                              # refresh included samples and the skills
```

`just` wraps these (`just --list`). Recipes stay one command each; logic lives in
`kustomization/deploy-local.sh`.

**Tests are serialised deliberately** (`Tags.limit(Tags.Test, 1)`, `Test / parallelExecution :=
false`). Suites that start containers contend when they overlap. Do not undo it, and never run two
sbt builds at once.

**Test switches must be forwarded** to the forked test JVM. `forwardedTestSwitches` in `build.sbt`
lists them; a new `-Dflow.*` switch that is not listed there silently does nothing.

A long run on a laptop: `caffeinate -i sbt test`. A sleeping Mac pauses Docker while deadlines keep
counting, and the failure looks like the platform's.

## Rules

| Rule | What it means here |
|---|---|
| Module direction | `descriptor` depends on nothing of ours. `pekkoSdk` → `descriptor`. `flinkSdk` depends on nothing of ours (Java; it writes the same descriptor bytes, proven by the fixtures). `flinkTestkit` → `flinkSdk`. `blueprint` → `descriptor`. `crd` → fabric8 only. `operator` → `crd`, `blueprint`, `descriptor`. `cli` → `blueprint`, `crd`, `descriptor`. `cartCleaner` → `pekkoSdk`; `cartCounter` → `flinkSdk`, and `flinkTestkit` in tests. |
| A streamlet holds the credentials of its own topics only | The operator resolves each port's Kafka settings from the cluster Secrets and writes them into that streamlet's Secret, `streamlet.properties`; nothing else reaches the pod. A blueprint and a resource never hold credentials. |
| Commit after the write | In the Pekko Streams SDK, an offset is committed only once every record produced from it is confirmed by the broker. `CommitAfterWrite` and its Kafka suite are the guard; `mutationCheck` proves the suite would catch a regression. |
| Never skip | A record that fails is retried from the last commit or checkpoint, indefinitely. Dropping a record is the streamlet's decision. |
| Explicit registration | A streamlet declares its ports and parameters; its SDK writes the descriptor. No classpath scanning, ever. |
| One descriptor, byte for byte | `descriptor/fixtures/descriptors` is what each declaration in `descriptor/fixtures/declarations` must write. Both SDKs reproduce it; a change to the format changes the fixtures and both SDKs together. |
| The Java modules | `flinkSdk`, `flinkTestkit` and `cartCounter` are Java, compiled `--release 17` for Flink's images; Flink itself is `Provided`. Scala on them is not supported. |
| Pure rendering, one place for I/O | `Rendering.render` returns actions; `Fabric8Executor` and `KafkaExecutor` perform them. |
| The resource says what runs | Deploy-time overrides are merged by the CLI into the resource. The operator adds only what only it can know: the Kafka cluster settings and the state store. |
| The Flink operator manifest is rendered and committed | `operator/src/test/resources/flink-operator.yaml` comes from the Flink Kubernetes Operator's chart by `just flink-operator-manifest`; the cluster suite and `just up` apply it. Re-render it on a Flink operator upgrade, never edit it by hand. |
| No literal image tags in tests | The Kafka image comes from `-Dflow.kafka.image`; built images use `BuildInfo.version` with `+` → `-`. |
| Two Scala versions | `descriptor`, `pekkoSdk` and `cartCleaner` compile with `V.scalaLts` (`ltsSettings`), so any Scala 3.3+ project can depend on what is published; every other Scala module stays on `V.scala`. A published module never depends on a 3.9-only one. |
| Warning-free compile | `-Wunused:all` is on. |
| No built-ins, no templates | Every streamlet is the author's code in the author's image. A streamlet starts from `samples/cart-pipeline`, not from a generated project. |
| One `flow`, two drivers | `cli/test` is one suite; `-Dflow.cli.binary=<path>` runs every case through that executable instead of in process. No case is skipped or conditional on the driver. The release runs it against each platform's binary and byte-compares the smoke outputs with the JVM build. |
| The native image's metadata is generated, then pruned | `cli/src/main/resources/META-INF/native-image/…/reachability-metadata.json` comes from the tracing agent (`-Dflow.cli.agent=on`, GraalVM as the JVM) and is cut down to what `flow` uses; its README says how. Regenerate on a fabric8 or Jackson upgrade, a new command, or a binary failing on a class or resource it cannot find. |
| The tap is shared | `thinkmorestupidless/homebrew-tap` holds ankka's formula and ours. The `homebrew` job clones it, writes `Formula/ankka-flow.rb` and pushes an ordinary commit; never a subtree split or a force push, which would erase the other. A pre-release tag (with a hyphen) publishes the release page and the binaries only, and touches no formula, so the tap always installs a release. |
| No logback in the CLI | `flow` logs nothing: `slf4j-nop`, so no classpath logging configuration has to survive the native image. fabric8 runs over `kubernetes-httpclient-jdk` there, with the Vert.x client excluded (Netty cannot be built as it comes). |
| `flow mcp` touches one cluster | The MCP server's cluster tools act only on the context and namespace `flow.toml` names in the directory it starts in, never the kubeconfig's current context; without the file every cluster tool refuses. Stdout carries only the protocol; anything for a person goes to stderr. The docs it serves are the pages built into that `flow` (`ankka-flow/docs/`), and apply and reset carry the destructive hint. |
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
- **Samples are included from tested code** between `// docs:start name` and `// docs:end name`
  markers (`#` in conf, yaml and shell), named by `<!-- include: path#name -->` before the block (no
  `#name` includes the whole file); `just docs-sync` copies them and `docs check` fails on drift.
  Markers live in the two sample modules, `samples/cart-pipeline/cleaner` and
  `samples/cart-pipeline/counter`, and the sample's blueprints and manifests. `README.md` includes
  the same way, refreshed by `just readme-sync` and checked by the docs workflow, since the docs tool
  reads only the pages.
- **A new page goes in `mkdocs.yml`'s `nav` and in a skill's `pages:` list**, or `docs check` fails.
- **A behaviour change is a docs change.** The pages restate CLI flags, events, env vars and descriptor
  rules; change them in the same commit.

## Living features

**Specs from feature 004 on keep their acceptance scenarios in living features**, not in the spec.
The [speckit-bdd](https://github.com/thinkmorestupidless/speckit-bdd) extension and preset are
installed under `.specify/`: `/speckit-specify` writes a spec whose acceptance scenarios *name*
scenarios, the `after_specify` hook runs `/speckit-bdd-features` to write them as Gherkin under
`features/<area>/` with every word they use in the root `GLOSSARY.md`, and the `before_clarify` hook
runs `/speckit-bdd-check`, which turns undefined words, refused synonyms, contradictions and untraced
requirements into clarification questions. `specs-from: "009"` in
`.specify/extensions/bdd/bdd-config.yml` leaves specs 001–008 as they were written: 004–007 name
scenarios of version one, which feature 009 removed. `just features`
(and CI's `features` job) runs the same checker from the same config, and fails when it read nothing.
The checker runs through `uvx`, so `uv` must be on `PATH`. Glossary terms follow
`docs/reference/glossary.md` where the docs already define a word.

## Carried code

A file derived from Cloudflow keeps Lightbend's header verbatim and arrives with its test, rewritten
as munit with every assertion kept. Add the file to `NOTICE`.

## Kubernetes manifests

The canonical manifests live in `kustomization/components/`. The copies the JVM reads from its
classpath (`crd/src/main/resources/ankka-flow/crd/ankkaflow.yaml`,
`operator/src/main/resources/ankka-flow/install/operator.yaml`) are symlinks **into** them. Kustomize's
load restrictor forbids the reverse direction.

Domain for labels, annotations and the CRD group: `flow.ankka.thinkmorestupidless.com`.

## Environment variables

Everything the platform sets is `FLOW_*`. A streamlet's container gets `FLOW_CONFIG_DIR`, the
directory holding its `streamlet.properties` (`contracts/streamlet-settings.md`), and nothing else.
The operator's settings are in `contracts/resource-and-operator.md` and `docs/reference/operator.md`.
