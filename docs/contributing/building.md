---
title: Build ankka-flow from source
description: The modules of the build, the JVM build of the flow CLI, the images, the native binary and its checks, and the test suites, from a clone of the repository.
kind: contributing
related: [get-started/install.md, reference/cli.md, contributing/documentation.md, contributing/engines.md]
---

# Build ankka-flow from source

[Install the tools](../get-started/install.md) installs a released `flow` and pulls released images.
This page is for changing ankka-flow itself, or for a platform the release does not build for: the
modules, the JVM build of the CLI, the images, the native binary and the checks that prove a native
build carries everything it needs, and the test suites. Commands run from the root of a clone of the
repository.

## What you need

| Tool | Used for |
|---|---|
| JDK 21 or later, and sbt | every module; the JVM build of the CLI. The Java modules compile with `--release 17` |
| Docker | the images, and the Kafka and Kubernetes containers the test suites start |
| GraalVM 25, community edition | the native binary, and the tracing agent that regenerates its metadata |
| uv | the documentation build and the living features' checker |
| just (optional) | short names for the commands on this page; `just --list` prints them |

## The modules

One sbt build holds every module. The sbt project id is the name to use in a command, such as
`sbt pekkoSdk/test`.

| Project | Directory | What it is | Depends on |
|---|---|---|---|
| `descriptor` | `descriptor/` | the descriptor model, its canonical JSON, its validation and fixtures, contract fingerprints, and the streamlet settings file; published as `ankka-flow-descriptor` | nothing of ours |
| `pekkoSdk` | `sdks/pekko-streams/` | the Pekko Streams SDK and its harness, in Scala; published as `ankka-flow-pekko-streams` | `descriptor` |
| `flinkSdk` | `sdks/flink/` | the Flink SDK, in Java with no Scala on its classpath; published as `ankka-flow-flink` | nothing of ours |
| `flinkTestkit` | `sdks/flink-testkit/` | the Flink SDK's harness; published as `ankka-flow-flink-testkit` | `flinkSdk` |
| `blueprint` | `blueprint/` | reading and verifying a blueprint against descriptors, and deploy-time configuration | `descriptor` |
| `crd` | `crd/` | the `AnkkaFlow` resource model | fabric8 and Jackson only |
| `operator` | `operator/` | the Kubernetes operator: rendering for both engines, its executors, and the cluster suite | `crd`, `blueprint`, `descriptor` |
| `cli` | `cli/` | the `flow` CLI, including `flow mcp` | `blueprint`, `crd`, `descriptor` |
| `cartCleaner` | `samples/cart-pipeline/cleaner/` | the sample's Pekko Streams streamlet; image `sample-cart-cleaner` | `pekkoSdk` |
| `cartCounter` | `samples/cart-pipeline/counter/` | the sample's Flink streamlet, in Java; image `sample-cart-counter` | `flinkSdk`, and `flinkTestkit` in tests |

`descriptor` and `pekkoSdk` compile with the Scala 3 LTS line, so any Scala 3.3 or later project can
depend on what they publish; the other Scala modules use the build's own Scala version. `flinkSdk`,
`flinkTestkit` and `cartCounter` are Java, compiled for Java 17, and take Flink's own modules as
`provided`.

## The JVM build of the CLI

```bash
sbt cli/stage
export PATH="$PWD/cli/target/universal/stage/bin:$PATH"
flow version
```

`just cli` runs the same build and prints the `export` line. This `flow` is a launcher script over the
CLI's jars and needs a JDK on the machine. It behaves exactly as the native binary does: the same
commands, the same output, the same exit codes; the CLI's suite runs against both. It is the build a
platform outside the four released ones uses.

## The images

```bash
sbt operator/docker:publishLocal cartCleaner/docker:publishLocal cartCounter/image
```

This builds `ankka-flow-operator`, `sample-cart-cleaner` and `sample-cart-counter` into your local
Docker, each tagged with the build's version. The version comes from the nearest git tag; a snapshot
version contains a `+`, which a Docker tag may not, so the tag has a `-` in its place. The operator's
and the cleaner's images are tagged `latest` as well.

`cartCounter/image` stages the counter's jar as `usrlib/streamlet.jar`, with every dependency Flink
does not provide beside it, and builds the sample's `Dockerfile` from Flink's own image.
[Build an image](../build/images.md) describes both kinds of image.

`just up` builds the images, loads them into a kind cluster and installs the platform there, which is
how [Deploy to a local cluster](../get-started/deploy-locally.md) runs code you have changed.

## The native binary

The released `flow` is a GraalVM native image: the CLI's jars compiled ahead of time into one
executable with no JVM to install. Building one needs a GraalVM's `native-image`, found through
`GRAALVM_HOME` or on your `PATH`:

```bash
export GRAALVM_HOME=/path/to/graalvm        # or put its bin directory on PATH
sbt cli/stage cli/GraalVMNativeImage/packageBin
cli/target/graalvm-native-image/flow version
```

What the image must carry is declared in the CLI's jar under `META-INF/native-image`: the build's
flags in `native-image.properties`, and in `reachability-metadata.json` every class fabric8, Jackson
and the YAML writer find by reflection and every resource the CLI reads from its classpath. A native
image leaves out anything not declared there, and the build still succeeds; the command that needed
the missing piece then fails at run time, or answers with nothing. Two checks catch that.

### The suite against the binary

The CLI's suite normally calls the CLI in its own JVM. Given a binary, it runs every case through that
executable instead:

```bash
sbt cli/test -Dflow.cli.binary="$PWD/cli/target/graalvm-native-image/flow"
```

### The smoke script

`cli/native-smoke.sh` runs a binary and asks it for each thing the image has to carry: its version, its
usage, a verification of each sample read from disk, a resource written as YAML, and a reset that
reaches the Kubernetes client. Given a JVM-built `flow` as well, it diffs every `verify` and `generate`
output against it byte for byte. It never touches your own cluster: it points `KUBECONFIG` at a file
that does not exist.

```bash
cli/native-smoke.sh cli/target/graalvm-native-image/flow "" cli/target/universal/stage/bin/flow
```

The second argument is the version the binary is expected to print, or empty to accept any. A
release's build passes the tag's version, so a binary that reports the wrong version fails the release.

`just cli-native` does all of it: the JVM build, the native build, the suite against the binary, and
the smoke script against the JVM build.

### Regenerating the reachability metadata

`reachability-metadata.json` is generated, not written by hand. GraalVM's tracing agent watches the
CLI's suite run and records every class, resource and serialized type the CLI reached by name:

```bash
sbt -java-home "$GRAALVM_HOME" cli/test -Dflow.cli.agent=on
# writes cli/target/native-image-agent/reachability-metadata.json
```

The test JVM has to be the GraalVM, which is what `-java-home` arranges. The agent's output includes
what the suite itself reached: the test framework, sbt's test runner, logback and fabric8's mock
server, the test resources, and the `.tasty` files jackson-module-scala looks for beside fabric8's
Java classes and never finds. Prune those before copying the file into
`cli/src/main/resources/META-INF/native-image/com.thinkmorestupidless/ankka-flow-cli/`; the `README.md`
beside it says what to drop, and lists the entries that were added by hand, each with the case that
needed it.

Regenerate the metadata when:

- fabric8 or Jackson is upgraded, since the classes they reach by reflection change between versions;
- a command is added, since it may reach classes or resources no existing command did;
- the suite run against the binary, or the smoke script, fails on a class or resource the binary
  cannot find. The binary's error names it.

Then rebuild the binary and run both checks again. Continuous integration builds the native binary on
one platform for every change under `cli/`, runs the suite against it and runs the smoke script, so a
missing entry is found at the pull request rather than at the tag.

## The test suites

```bash
sbt test                                      # everything, including the cluster suite
sbt -Dflow.cluster.tests=off test             # everything but the cluster suite
sbt descriptor/test blueprint/test crd/test   # the pure modules, in seconds
sbt pekkoSdk/test                             # the harness, and commit after write on a real Kafka
sbt mutationCheck                             # the commit-after-write suite must fail with the commit moved first
sbt flinkSdk/test flinkTestkit/test           # the descriptor bytes, and the harness on a local Flink
sbt cartCleaner/test cartCounter/test         # the sample's tests, which the documentation includes
sbt cartCleaner/descriptorCheck cartCounter/descriptorCheck
sbt cli/test                                  # verify, generate, local, reset and mcp
sbt -Dflow.cluster.tests=off operator/test    # rendering for both engines, reset and conditions
sbt operator/test                             # the same, and the cluster suite
sbt scalafmtAll scalafmtSbt                   # format; `just hooks` installs the pre-commit check
```

The cluster suite, `FlowClusterSuite`, needs Docker. It starts k3s in a container, builds and imports
the operator's and the sample's images, installs the Flink Kubernetes Operator and MinIO, and deploys
the sample pipeline, taking about fifteen minutes. `-Dflow.cluster.tests=off` skips it and the images
it needs; `-Dflow.flink.tests=off` keeps it but skips its Flink cases. A long run on a laptop is best
kept awake, as `just test-all` does: a sleeping machine pauses Docker while the suite's deadlines keep
counting.

`mutationCheck` runs the Pekko Streams SDK's commit-after-write suite with the commit moved ahead of
the write, and fails the build if that suite still passes.

The test suites run one at a time on purpose: the ones that start containers contend when they
overlap. A `-Dflow.*` switch reaches the forked test JVM only when `forwardedTestSwitches` in
`build.sbt` lists it.

[Writing documentation](documentation.md) covers the documentation build, and [Adding an
engine](engines.md) what a third engine would take.
