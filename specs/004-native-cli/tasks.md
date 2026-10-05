# Tasks: A Native `flow` Binary, Released and Brewable

**Input**: Design documents from `/specs/004-native-cli/`

**Prerequisites**: [plan.md](./plan.md), [spec.md](./spec.md), [research.md](./research.md),
[data-model.md](./data-model.md), [contracts/](./contracts/), [quickstart.md](./quickstart.md);
the scenarios are in `features/cli/*.feature`.

**Tests**: included, and first where a test can be written before the thing it holds. The suite
that proves the binary is the one that exists; the work is to run it a second way without losing
a case.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: can run in parallel — different files, no dependency on an incomplete task
- **[Story]**: US1 (install `flow` with one command), US2 (the binary is the CLI), US3 (the
  release does it every time), US4 (the docs say install)

Paths are repository-relative. `CLI` = `cli/src/main/scala/com/thinkmorestupidless/ankka/flow/cli`,
`CLIT` = `cli/src/test/scala/com/thinkmorestupidless/ankka/flow/cli`, `NI` =
`cli/src/main/resources/META-INF/native-image/com.thinkmorestupidless/ankka-flow-cli`. "R*n*" is a
section of `research.md`; "V*n*" an item of its *Verify first* list.

---

## Phase 1: Setup

- [ ] T001 Confirm a GraalVM with `native-image` is available for local work (`sdk install java 25-graalce` or `GRAALVM_HOME`), and record in `specs/004-native-cli/research.md` the version used and, for V1, whether the fabric8 7.9.0 jars on the CLI's classpath carry `META-INF/native-image` entries (`unzip -l` each `kubernetes-client*`/`kubernetes-model*` jar).
- [ ] T002 Add `flow.cli.binary` and `flow.cli.agent` to `forwardedTestSwitches` in `build.sbt`, with a comment naming each; `sbt cli/test` still green.

**Checkpoint**: nothing has changed for a user; the switches reach the test JVM.

---

## Phase 2: Foundational — one suite, two drivers (R3)

**Purpose**: the only proof that the binary is the CLI is the suite, so the suite must be able to
drive a binary before there is one.

**⚠️ CRITICAL**: US2 and everything after it depend on this phase.

- [ ] T003 In `CLIT/CliFixtures.scala`, replace `flow(args*)` with a `Driver`: `InProcess` (today's `Main.run` with captured streams) and `Binary(path)` (a subprocess running the binary with the arguments, `KUBECONFIG` and the working directory passed through, stdout, stderr and the exit code captured, a 60 s timeout); `flow` picks `Binary` when `-Dflow.cli.binary` is set, else `InProcess`. Add a case to `CLIT/VerifySuite.scala` that fails when the switch names a path that does not exist, so a misspelt switch cannot silently test in process.
- [ ] T004 In `CLIT/CliResetSuite.scala`, stop injecting `KubernetesReset(() => newClient())`: write a kubeconfig for the mock server (its host and port from `server.createClient().getConfiguration.getMasterUrl`, `insecure-skip-tls-verify`, a context named `flow-test`) to a temporary file in `beforeAll`, run every reset case through `CliFixtures.flow` with `KUBECONFIG` set to it, and read the annotation back through the suite's own client as today. Confirm V3 for the in-process driver.
- [ ] T005 In `CLI/Main.scala` and `CLI/Reset.scala`, remove `Main.run`'s `resetter` parameter; `Reset.kubernetes` builds its client from the environment (`KUBECONFIG`, then the defaults) on every call, as a user's `flow reset` does. `sbt cli/test` green with the same 43 cases, in process.
- [ ] T006 Commit Phase 2 on its own: the suite is unchanged in content and green before any native work.

**Checkpoint**: `sbt cli/test` passes in process through the new seam; `-Dflow.cli.binary=/nowhere` fails loudly.

---

## Phase 3: User Story 2 — The binary is the CLI, not an approximation of it (Priority: P1)

**Goal**: a native `flow` that passes the whole suite and whose `verify` and `generate` output is
byte-identical to the JVM build's on every sample.

**Independent Test**: `sbt cli/test -Dflow.cli.binary=…` with every case green; `cli/native-smoke.sh`
with the diff against the staged JVM CLI exits 0.

- [ ] T007 [US2] In `build.sbt`, enable `GraalVMNativeImagePlugin` on `cli`: `GraalVMNativeImage / name := "flow"`, `graalVMNativeImageCommand` from `GRAALVM_HOME` else `native-image`, `Docker / publishLocal := {}` and `Docker / publish := {}` as ankka's `cli` has them (R1).
- [ ] T008 [P] [US2] Write `NI/native-image.properties`: `--no-fallback`, `-march=compatibility`, `-Dsun.misc.unsafe.memory.access=allow`, each with a one-line reason as ankka's has.
- [ ] T009 [US2] Replace `logback` with `slf4j-nop` in `cli`'s `libraryDependencies` in `build.sbt` and delete `cli/src/main/resources/logback.xml`; run `sbt cli/test` and read every message the suite asserts on to confirm V2 — nothing a user sees came through slf4j. Record the answer in `research.md`.
- [ ] T010 [US2] Add a `flow.cli.agent` switch to `build.sbt` that, when `on`, adds `-agentlib:native-image-agent=config-merge-dir=cli/target/native-image-agent` to `cli`'s `Test / javaOptions` (a GraalVM JDK must be the test JVM); run `sbt cli/test -Dflow.cli.agent=on` and copy the merged `reflect-config.json`, `resource-config.json`, `serialization-config.json` (and `jni-config.json`/`proxy-config.json` if non-empty) into `NI/`. Prune each to the packages `flow` reaches (`com.thinkmorestupidless.ankka.flow`, `io.fabric8`, `com.fasterxml.jackson`, `org.yaml`, `scala`), and add a `README.md` in `NI/` saying how they were generated and when to regenerate (a fabric8 or Jackson upgrade, a new command).
- [ ] T011 [US2] Build the binary: `sbt cli/stage cli/GraalVMNativeImage/packageBin`; then `sbt cli/test -Dflow.cli.binary=$PWD/cli/target/graalvm-native-image/flow`. For each failing case, find the missing reachability entry (the binary's error names the class or resource), add it to the right file under `NI/`, rebuild, re-run, until every case passes with none skipped (SC-002). Record in `research.md` what the agent missed and why.
- [ ] T012 [US2] Write `cli/native-smoke.sh <binary> [expected-version] [jvm-flow]` per `contracts/cli-and-tests.md`: version, usage and exit codes, `verify` and `generate` on each `samples/*/blueprint.conf` with its descriptors, `k8s/in-cluster.conf` and an `--image` per streamlet, `reset` against a kubeconfig naming `127.0.0.1:1` (a connection refusal, not a stack trace), and, when a JVM `flow` is given, a byte diff of every `verify` and `generate` output; one line per check, exit 1 naming the first failure. Run it against the binary with the staged CLI (SC-003).
- [ ] T013 [US2] Break the image on purpose — remove the `resource-config.json` entry for the CRD's YAML, rebuild — and show the suite and the smoke script both fail naming it; restore. Record it in `research.md`.
- [ ] T014 [US2] Add a `cli-native` job to `.github/workflows/ci.yml` on `ubuntu-22.04` (`graalvm/setup-graalvm@v1`, Java 25 community, `cache: sbt`): `sbt cli/stage cli/GraalVMNativeImage/packageBin`, the suite against the binary, the smoke script with the diff; a `cli-native` path filter over `cli/**`, `crd/**`, `blueprint/**`, `protocol/**`, `build.sbt`, `project/**` and the workflow (R8). Add `cli-native` to `Justfile` as `just cli-native`, one command.

**Checkpoint**: the suite passes both ways; the smoke script passes with the diff; CI builds the Linux image on this branch.

---

## Phase 4: User Story 3 — The release does it, every time (Priority: P2)

**Goal**: a tag attaches four archives and four checksums and updates the tap's formula with no
step by hand, and a failed platform leaves the formula untouched.

**Independent Test**: a pre-release tag on the branch (quickstart tier 4).

- [ ] T015 [P] [US3] Write `homebrew/Formula/ankka-flow.rb` after `ankka/homebrew/Formula/ankka.rb`: class `AnkkaFlow`, desc "Command-line client for ankka-flow, streaming pipelines beside ankka", homepage `https://flow.ankka.cloud/`, `version "0.0.0"`, the four URLs on `thinkmorestupidless/ankka-flow`'s releases named per `contracts/release-artifacts.md`, four zeroed `sha256` lines each with its `# <platform>` comment, `bin.install "flow"`, and a `test do` that runs `flow version`.
- [ ] T016 [US3] In `.github/workflows/release.yml`, add `release-page` (creates the tag's release with `gh release create --verify-tag --title "ankka-flow <version>" --generate-notes` when `gh release view` fails; `contents: write`) and the four-leg `cli-native` matrix (`needs: release-page`; ankka's runners and `graalvm/setup-graalvm`; `fetch-depth: 0`; the dirty-tree refusal; `sbt cli/stage cli/GraalVMNativeImage/packageBin`; `sbt cli/test -Dflow.cli.binary=…`; `cli/native-smoke.sh <binary> <version> cli/target/universal/stage/bin/flow`; `tar -czf` and `shasum -a 256`; `gh release upload --clobber`). Leave `images`, `sdk-python` and `marketplace` without a `needs` on the new jobs (FR-014).
- [ ] T017 [US3] Add the `homebrew` job (`needs: cli-native`): download the four `.sha256`, fill `homebrew/Formula/ankka-flow.rb`'s version and checksums (refusing a surviving zero placeholder), clone `thinkmorestupidless/homebrew-tap` with `HOMEBREW_TAP_TOKEN`, copy the formula to `Formula/ankka-flow.rb`, commit as "ankka-flow <version>", push without force, and on a rejected push fetch, rebase and push once more; fail naming the secret when it is unset (R6, R7).
- [ ] T018 [US3] In the **ankka** repository, on a branch: change its `homebrew` job from `git subtree split` + `git push --force` to clone, write `Formula/ankka.rb`, commit, push without force with the same retry; open the pull request and link it from `specs/004-native-cli/research.md`. This feature's first release waits for it (V5).
- [ ] T019 [US3] Record in `specs/004-native-cli/research.md` the secret to set (`HOMEBREW_TAP_TOKEN`, contents write on the tap) and that it is the maintainer's; confirm V4 by reading ankka's `cli` job, which creates its release with the workflow's token on the same organisation.

**Checkpoint**: the workflow is complete on the branch; its run is Phase 6.

---

## Phase 5: User Story 1 — Install `flow` with one command (Priority: P1)

**Goal**: `brew install thinkmorestupidless/tap/ankka-flow`, or an archive, puts `flow` on a
machine with no JVM.

**Independent Test**: quickstart tier 4, steps 2 and 3, on a Mac with no JVM.

- [ ] T020 [US1] Build the macOS arm64 binary locally, `tar -czf` it as the release would, and `brew install --formula` a copy of `homebrew/Formula/ankka-flow.rb` edited to a `file://` URL and the archive's checksum; `flow version` answers; `brew uninstall ankka-flow`. Record the Homebrew version used in `research.md`. (The tap itself is proven at the pre-release tag, Phase 6.)
- [ ] T021 [US1] Confirm the four platform conditions of the formula match ankka's (`on_macos`/`on_linux`, `on_arm`/`on_intel`) and that a platform outside them is refused by Homebrew naming it (run `brew install` with the formula on a stubbed `OS.linux?` is not practical; read Homebrew's documented behaviour and cite it in `research.md`).

**Checkpoint**: the formula installs a local archive; the install path is proven short of the tap.

---

## Phase 6: User Story 3 continued — the release, run (Priority: P2)

- [ ] T022 [US3] With ankka's pull request (T018) merged and `HOMEBREW_TAP_TOKEN` set, push the branch and tag `v0.4.0-rc.1` on it; watch `release-page`, the four `cli-native` legs and `homebrew`; check the release has eight assets, the tap has `Formula/ankka-flow.rb` at `0.4.0-rc.1` and `Formula/ankka.rb` unchanged; `brew install thinkmorestupidless/tap/ankka-flow` on a Mac with no JVM and `flow version` (SC-001, SC-004). Record each in `research.md`.
- [ ] T023 [US3] On the branch, break one platform's build (a flag only that platform's leg sees) and tag `v0.4.0-rc.2`: three archives attached, `homebrew` skipped, the failure naming the platform; then restore. Delete both rc tags and their releases, and the rc commit of the tap's formula is left (it is history, harmless) or reverted — say which in `research.md`.

**Checkpoint**: SC-004 observed on a real run.

---

## Phase 7: User Story 4 — The docs say "install", and contributors still build (Priority: P3)

**Goal**: the install page offers the tap, then the archive; no guide begins by building `flow`;
contributors find the build from source.

**Independent Test**: `just docs` clean; `git grep` for the build commands finds only the
contributing page.

- [ ] T024 [P] [US4] Rewrite `docs/get-started/install.md`: title and description unchanged in intent; `brew install thinkmorestupidless/tap/ankka-flow` first with `flow version`'s output; the archive per platform from the release with `shasum -a 256 -c`, the four platforms listed, and macOS's prompt for a downloaded binary; then the images as today; "a platform outside these builds from source" linking the contributing page. Nothing about sbt, a JVM or cloning before `flow` is on the path (SC-005).
- [ ] T025 [P] [US4] Write `docs/contributing/building.md` (*Build ankka-flow from source*, kind contributing): the JDK and sbt, `just cli` / `sbt cli/stage`, the images with `sbt docker:publishLocal sampleImage`, the native binary with a GraalVM and `sbt cli/GraalVMNativeImage/packageBin`, the suite against it, and the smoke script; add it to `mkdocs.yml`'s nav under Contributing and to the `ankka-flow` skill's `pages:`.
- [ ] T026 [P] [US4] Update `docs/reference/cli.md` (how `flow` is installed; `flow version`'s line), `README.md` (the laptop walkthrough installs `flow`), `samples/cart-router/README.md`, and any other page `git grep -n 'sbt cli/stage\|just cli' docs samples/*/README.md README.md tools/docs/skill` finds outside the contributing page, to say install.
- [ ] T027 [US4] Update `tools/docs/skill/ankka-flow/SKILL.md` and `tools/docs/skill/ankka-flow-deploy/SKILL.md` (the install path; a mistake to check for: a guide that builds the CLI); `just docs-sync && just docs` until clean; commit the rendered skills.

**Checkpoint**: the docs build; the only build-from-source instructions are on the contributing page.

---

## Phase 8: Polish

- [ ] T028 Add to `CLAUDE.md`'s commands and rules: the two new switches, `just cli-native`, the smoke script, that the image's reachability configuration under `NI/` is generated and pruned (and when to regenerate), that the tap is shared and never force-pushed, and that `logback` is not in the CLI.
- [ ] T029 Run the whole build from the branch (`caffeinate -i sbt scalafmtCheckAll scalafmtSbtCheck test mutationCheck`, the Python SDK's checks, `just features`, `just docs`), the suite against a fresh binary, and tick the reviewer's checklist in `quickstart.md`.
- [ ] T030 Write "Found during implementation" in `specs/004-native-cli/research.md` (each V-item's answer) and the pull request description: what a reader does now, what the release does now, the change in ankka it depended on, the secret, and that `flow`'s behaviour is unchanged.

---

## Dependencies & Execution Order

- **Phase 1 → 2 → 3**: the seam (Phase 2) must exist before the binary (Phase 3) can be tested; T003–T005 are one change to one suite and go in order.
- **Phase 4 (US3's workflow)** needs T011–T012 (what the legs run) and T015 (the formula). T015 and T018 can go in parallel with Phase 3.
- **Phase 5 (US1)** needs T011 and T015.
- **Phase 6** needs T016–T019, T018 merged in ankka, and the secret set: the one gate outside the repository.
- **Phase 7 (US4)** can go in parallel with Phases 4–6 once T012 exists (the pages describe the smoke script and the switches); T027 last.
- **Phase 8** after everything.

### Parallel opportunities

- T007 and T008; T015 beside Phase 3; T018 beside everything in this repository; T024, T025, T026.

## Implementation Strategy

**MVP is US2** (Phases 1–3): a native `flow` proven by the suite and the smoke script, built by CI
on Linux. It ships nothing to a user yet, but it is the part that can fail in ways that need
design, and it is green before any workflow is written.

Then US3's workflow and formula, US1's local install, the release run, US4's docs, polish. One
person: in the order the phases are numbered.

### Notes

- A reachability entry added by hand (T011) is recorded with the case that needed it, so the next
  agent run can be compared against it.
- Stop at a checkpoint that is not green. A case that passes in process and fails against the
  binary is never skipped or made conditional on the driver.
- The rc tags are deleted after Phase 6; nothing points at them.
