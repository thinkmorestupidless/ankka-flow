---
title: Build ankka-flow from source
description: Build the flow CLI, the images and the native binary from a clone of the repository, run the suite against the binary, and regenerate the native image's reachability metadata.
kind: contributing
related: [get-started/install.md, reference/cli.md, contributing/documentation.md, contributing/language-sdks.md]
---

# Build ankka-flow from source

[Install the tools](../get-started/install.md) installs a released `flow` and pulls released images.
This page is for changing ankka-flow itself, or for a platform the release does not build for: the
JVM build of the CLI, the images, the native binary, and the checks that prove a native build carries
everything it needs. Commands run from the root of a clone of the repository.

## What you need

| Tool | Used for |
|---|---|
| JDK 21 or later, and sbt | every module; the JVM build of the CLI |
| Docker | the images, and the Kafka and Kubernetes containers the test suites start |
| GraalVM 25, community edition | the native binary, and the tracing agent that regenerates its metadata |
| uv, with Python 3.12 or later | the Python SDK and the samples |
| just (optional) | short names for the commands below; `just --list` prints them |

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
sbt docker:publishLocal sampleImage
```

This builds `ankka-flow-sidecar`, `ankka-flow-operator` and `sample-cart-router` into your local Docker,
each tagged with the build's version and with `latest`. The version comes from the nearest git tag; a
snapshot version contains a `+`, which a Docker tag may not, so the tag has a `-` in its place.
`just images` is the same command, and `sbt sidecar/docker:publishLocal` builds the sidecar alone.

`just up` builds the three images, loads them into a kind cluster and installs the platform there,
which is how [Deploy to a local cluster](../get-started/deploy-locally.md) runs code you have changed.

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
usage, a verification read from disk, a resource written as YAML, and a reset that reaches the
Kubernetes client. Given a JVM-built `flow` as well, it diffs every `verify` and `generate` output
against it byte for byte. It never touches your own cluster: it points `KUBECONFIG` at a file that
does not exist.

```bash
cli/native-smoke.sh cli/target/graalvm-native-image/flow "" cli/target/universal/stage/bin/flow
```

The second argument is the version the binary is expected to print, or empty to accept any. A
release's build passes the tag's version, so a binary that reports the wrong version fails the release.

`just cli-native` does all of it: the JVM build, the native build, the suite against the binary, and
the smoke script against the JVM build.

## Regenerating the reachability metadata

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
needed it. A charset, for example, is not reflection, so the agent cannot record it; the image is
told to carry every charset in `native-image.properties` instead.

Regenerate the metadata when:

- fabric8 or Jackson is upgraded, since the classes they reach by reflection change between versions;
- a command is added, since it may reach classes or resources no existing command did;
- the suite run against the binary, or the smoke script, fails on a class or resource the binary
  cannot find. The binary's error names it.

Then rebuild the binary and run both checks again. Continuous integration builds the native binary on
one platform for every change under `cli/`, runs the suite against it and runs the smoke script, so a
missing entry is found at the pull request rather than at the tag.

## The templates of `flow init`

`flow init` writes projects from templates carried inside the CLI, the native binary included:

- `cli/src/main/templates/common/` holds what both languages share: the blueprint, the sidecar's
  configuration, the compose file, the cluster configuration and `.gitignore`;
- `cli/src/main/templates/scala/` and `cli/src/main/templates/python/` hold each language's own files.

The build copies `common/` and then a language's directory into the CLI's jar under
`ankka-flow/templates/<language>/`, with the ankka-flow plugin's rendered skills as `.claude/skills/`,
and writes an `index.txt` listing every file, because a directory inside a jar or a native image cannot
be listed. Hidden files are copied too, and a path written twice fails the build.

Rendering is exact token replacement in every path and file: `{{name}}`, `{{class}}`, `{{package}}`,
`{{package_path}}`, `{{module}}`, `{{fingerprint}}`, `{{flow_version}}`, `{{sdk_version}}`,
`{{image_version}}`, `{{protocol_version}}`, `{{scala_version}}`, `{{sbt_version}}` and
`{{native_packager_version}}`; the last three come from this build, so a generated Scala project uses
the sbt, sbt-native-packager and Scala 3 LTS this repository is proven with. Nothing else is expanded:
GitHub's `${{ … }}` passes through. The skills are copied as they are, never rendered, because their
pages show tokens of their own. Each template's `flow/descriptor.json` is a template too.

The template suite renders each language's project through the CLI and builds it against this
repository's SDK — the Scala SDK published locally, the Python SDK by path — running the project's
tests, its descriptor check and its image build, and `flow verify` on its blueprint:

```bash
sbt 'cli/testOnly *TemplateSuite'                              # both languages
sbt 'cli/testOnly *TemplateSuite' -Dflow.template.tests=python  # one; `off` for none
```

A change to a template is checked by that suite, and is a documentation change: the tutorial and the
CLI reference describe what the projects hold. After changing a template's streamlet, render a project,
run its descriptor command, and copy the result back with its tokens.

## Everything else

```bash
sbt test                                    # everything, including the Kubernetes suite and the sample image it needs
sbt -Dflow.cluster.tests=off test           # everything but the Kubernetes suite
sbt scalafmtAll scalafmtSbt                 # format; `just hooks` installs the pre-commit check
cd sdks/python && uv sync && uv run python scripts/proto.py && uv run mypy && uv run pytest -q && uv run conformance
```

The test suites run one at a time on purpose: the ones that start containers contend when they
overlap. [Writing documentation](documentation.md) covers the docs build, and [Adding a language
SDK](language-sdks.md) what an SDK in another language must pass.
