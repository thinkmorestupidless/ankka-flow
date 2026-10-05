# Research: A Native `flow` Binary, Released and Brewable

Decisions for [plan.md](plan.md), each with what it rests on. "Verify first" marks a claim read
from code or documentation and not yet run; the task that touches it starts with the check.

## R1. The binary is built the way ankka's is

**Decision**: `cli` gains `GraalVMNativeImagePlugin` (sbt-native-packager 1.11.7 is already in the
build), `GraalVMNativeImage / name := "flow"`, `graalVMNativeImageCommand` from `GRAALVM_HOME` or
`native-image` on `PATH`, and a `META-INF/native-image/com.thinkmorestupidless/ankka-flow-cli/
native-image.properties` in the jar carrying `--no-fallback`, `-march=compatibility` and
`-Dsun.misc.unsafe.memory.access=allow`, as ankka's `cli` does (`ankka/build.sbt` 686–720,
`ankka/cli/.../native-image.properties`). `sbt cli/GraalVMNativeImage/packageBin` yields
`cli/target/graalvm-native-image/flow`.

**Rationale**: the same plugin, the same GraalVM (25, community, through `graalvm/setup-graalvm`),
the same four runners. What differs is below the surface: what this CLI carries.

**Alternatives considered**: a separate native-image build script outside sbt — a second source of
truth for the classpath. A JVM launcher bundled with a runtime (jlink) — still a JVM, and not what
`brew install` should put on a machine.

## R2. What this CLI leans on that ankka's does not

`flow` depends on `decline`, fabric8 `kubernetes-client` 7.9.0, `jackson-module-scala`,
`jackson-dataformat-yaml` and `logback` (`build.sbt`, `cli`). ankka's native CLI depends on
`decline` and its own modules, with jsoniter for JSON. Three of `flow`'s dependencies reach things
a native image cannot see at build time:

| Dependency | Reaches at run time | Where in `flow` |
|---|---|---|
| fabric8 `kubernetes-client` | model classes and their builders by reflection; `KubernetesSerialization`; the config loader reading `KUBECONFIG` | `Reset.scala` (`KubernetesClientBuilder`, `FlowSerialization`), `ResourceWriter.scala` (the `AnkkaFlow` model) |
| jackson (scala module, YAML) | serializers found by annotation and by name; the YAML writer | `ResourceWriter.scala` writes the resource as YAML; `crd`'s `AnkkaFlow` model |
| logback | appenders and encoders named in `logback.xml`, instantiated by name | `cli/src/main/resources/logback.xml` |

**Decision**: the image's reachability configuration is generated once with GraalVM's tracing
agent, by running the JVM CLI's suite under it, and committed under `META-INF/native-image/…/`
(`reflect-config.json`, `resource-config.json`, `serialization-config.json` as needed); then
pruned to what `flow` uses. fabric8 7.x ships its own reachability metadata for the model and the
client (**verify first**: whether 7.9.0's jars carry `META-INF/native-image` entries, and whether
they cover `KubernetesSerialization` with a custom mapper); jackson-module-scala and logback do not,
and are covered by the generated configuration or, for logback, replaced.

**Logback**: a CLI that prints its own messages needs no logging framework; fabric8 logs through
slf4j, and what it logs at `warn` on a mock-server refusal is noise. **Decision**: drop logback
from the CLI for `slf4j-nop` (**verify first**: that nothing a user needs is logged only through
slf4j; `CliResetSuite`'s refusal messages are `flow`'s own). One fewer thing to configure for the
image and a smaller binary.

**Alternatives considered**: hand-written reflection configuration — brittle against a fabric8
upgrade; the generated set, pruned, is what the agent saw the CLI do. Replacing fabric8 in the CLI
with plain HTTP to the API server — a rewrite of `reset` for the sake of the image, and the CRD
model is shared with the operator through `crd`.

## R3. One suite, two drivers

**What is there**: every CLI case goes through `CliFixtures.flow(args*)`, which calls
`Main.run(args, out, err)` in process and returns `(code, out, err)`; `CliResetSuite` calls
`Main.run` with a `KubernetesReset` built on the mock server's client, injected as a parameter
(`CliResetSuite.scala:106–118`).

**Decision**: `CliFixtures.flow` becomes a `Driver` with two implementations chosen by the switch
`-Dflow.cli.binary=<path>` (forwarded to the test JVM through `forwardedTestSwitches`, as every
`-Dflow.*` switch must be): in process, as today; or a subprocess running the binary with the same
arguments, capturing stdout, stderr and the exit code. The reset suite stops injecting a client:
it writes a kubeconfig for the mock server (its address; `useHttps = false`, so no certificate)
to a temporary file and runs `flow` with `KUBECONFIG` set to it, in both drivers, so the binary's
own config loading — fabric8's, by reflection — is what is tested. `Main.run`'s `resetter`
parameter goes; `Reset.kubernetes` reads the environment as it does for a user.

**Rationale**: FR-005 says every case runs against the binary with no case skipped, and the only
way the reset cases can is through the environment, which is also what a user's `flow reset`
does. The in-process driver stays the default: fast, and what a contributor runs.

**Verify first**: that the mock server's `createClient().getConfiguration` gives an address a
kubeconfig can name, and that fabric8 reads `KUBECONFIG` in a native image (its config loading is
one of the reflection-heavy paths).

## R4. Byte-identical output, checked in the release

**Decision**: `cli/native-smoke.sh <binary> [version] [jvm-cli]` runs the binary through
`version`, `--help`, a wrong flag, `verify` and `generate` on each sample in `samples/` with its
`k8s/in-cluster.conf` and an `--image`, and `reset` against a kubeconfig that names an unreachable
address (a refusal that proves the client loaded and tried); each answer is checked for the thing
a missing resource would blank. When a JVM build is given, every `verify` and `generate`
output is diffed against it and any difference fails. The release's `cli-native` leg runs `sbt
cli/stage` first and passes the JVM build, so FR-007 is checked on every platform.

**Rationale**: ankka's smoke script is the model (`ankka/cli/native-smoke.sh`): it asks for each
thing the image must carry rather than trusting the build. `flow` has fewer things and one more
check, the diff.

## R5. The release: a release page, four legs, then the formula

**What is there**: ankka-flow's `release.yml` has `images`, `sdk-python` and `marketplace`, and
creates no GitHub release; ankka's `cli` job creates one (`gh release create --verify-tag
--generate-notes`) and `cli-native` uploads to it (`ankka/.github/workflows/release.yml`).

**Decision**: a `release-page` job creates the tag's GitHub release if absent (as ankka's `cli`
does), `cli-native` (a four-leg matrix, `needs: release-page`) builds on each platform from a
clean tree, runs `sbt cli/stage`, the suite against the binary (`sbt 'cli/testOnly *' -Dflow.cli.
binary=…`), the smoke script with the diff, and uploads `ankka-flow-cli-<version>-<platform>.tar.gz`
and its `.sha256` with `--clobber`. `homebrew` (`needs: cli-native`) updates the formula. The
existing three jobs are untouched and do not depend on the new ones (FR-014).

**Platforms and runners**: `linux-x64` on `ubuntu-22.04`, `linux-arm64` on `ubuntu-22.04-arm`,
`macos-arm64` on `macos-14`, `macos-x64` on `macos-15-intel` — ankka's four.

## R6. The tap is shared, so neither release may replace it whole

**What is there**: ankka's `homebrew` job does `git subtree split --prefix homebrew` and
**force-pushes** that split to the tap's `main`. The tap's `main` is therefore exactly ankka's
`homebrew/` directory. A second repository doing the same would replace `ankka.rb` with
`ankka-flow.rb`, and ankka's next release would replace it back.

**Decision**: ankka-flow's `homebrew` job clones the tap, writes `Formula/ankka-flow.rb` from this
repository's `homebrew/Formula/ankka-flow.rb` with the version and checksums filled in, commits and
pushes without force, retrying once on a non-fast-forward. **And ankka's job has to change the same
way** — clone, write `Formula/ankka.rb`, commit, push — or its next release wipes `ankka-flow.rb`.
That change is a pull request in ankka, small, and this feature's release must not be cut before
it is merged. The spec's FR-012 ("MUST NOT touch any other formula") is only true of both.

**Rationale**: FR-012 and the scenario "ankka's release and ankka-flow's release each update only
their own formula" cannot hold with a force-pushed subtree on either side.

**Alternatives considered**: a second tap for ankka-flow — two taps to add for one platform, and
the spec says the same tap. Keeping both formulas in ankka's repository — ankka-flow's release
could not update its own.

## R7. The token

ankka's `homebrew` job pushes with `TEMPLATE_REPO_TOKEN`; ankka-flow's `marketplace` job pushes
with `MARKETPLACE_REPO_TOKEN`, which has write to `ankka-marketplace` only. **Decision**: a
repository secret `HOMEBREW_TAP_TOKEN` on ankka-flow, a fine-grained token with contents write on
`thinkmorestupidless/homebrew-tap`. Outside the repository, the maintainer's to set; the job fails
naming the secret when it is missing. (Reusing `MARKETPLACE_REPO_TOKEN` by widening its scope is
the maintainer's call; the workflow names one secret either way.)

## R8. CI builds one native image before a release does

**Decision**: `ci.yml` gains a `cli-native` job on `linux-x64` only, run when `cli/**`, `crd/**`,
`blueprint/**`, `protocol/**`, `build.sbt` or `project/**` change: build the image, run the suite
against it, run the smoke script with the diff. The other three platforms are built only at a
release.

**Rationale**: a reflection path added to `flow` on a branch would otherwise be found at the next
tag, with the release half-published. One platform in CI catches the class of problem; the release
catches the platform-specific remainder. Cost: a GraalVM build of a small CLI, a few minutes.

## R9. Version, clean tree, and what `flow version` prints

`BuildInfo.version` comes from sbt-dynver, from the nearest tag, so a build at the tag `v0.4.0`
prints `flow 0.4.0, protocol 1.0`; a build from a dirty tree prints a snapshot version. The leg
refuses a dirty tree before building, as ankka's does, and the smoke script asserts the version
is the tag's (FR-003, FR-010).

## R10. Documentation

- `docs/get-started/install.md` is rewritten: `brew install thinkmorestupidless/tap/ankka-flow`
  first; the archive per platform with `shasum -a 256 -c`; macOS's prompt for a downloaded binary;
  then the images, which are pulled from the registry as today. Building from source moves to
  `docs/contributing/` (a new page, *Build ankka-flow from source*, or a section of the existing
  contributing page — **verify** which exists), where `just cli` and `sbt cli/stage` are named.
- `docs/reference/cli.md`, `README.md`, `samples/cart-router/README.md` and every page that says
  `sbt cli/stage` or `just cli` (`git grep` finds four files) say install.
- The four skills are re-rendered by `docs sync`; `ankka-flow` and `ankka-flow-deploy` get the
  install path in their rules.
- The install page lists the four platforms and says that a platform outside them builds from
  source.

## Verify first

1. **R2**: whether fabric8 7.9.0 ships native-image metadata, and what the tracing agent reports
   for `flow`'s suite — the size of the generated configuration says how much is hand-pruned.
2. **R2**: that `slf4j-nop` loses nothing a user sees.
3. **R3**: that a kubeconfig naming the mock server's address works for the in-process driver, and
   that the native binary reads `KUBECONFIG`.
4. **R5**: that `gh release create --verify-tag` works with the workflow's own token on this
   repository, as it does on ankka's.
5. **R6**: that a non-force push to the tap from two repositories interleaves cleanly, with the
   retry; and that ankka's job is changed before this feature's first release.
6. **R1**: that `-march=compatibility` is accepted on the arm64 runners (ankka's four legs pass with
   it, so it should be).
