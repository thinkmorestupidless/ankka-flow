# Feature Specification: A Native `flow` Binary, Released and Brewable

**Feature Branch**: `004-native-cli`

**Created**: 2026-10-05

**Status**: Draft

**Input**: User description: "A native flow binary, released and brewable: the flow CLI ships as
one executable with no JVM to install, for macOS (Apple silicon and Intel) and Linux (x64 and
arm64), attached to every tagged release of ankka-flow with a checksum, and installable with brew
install thinkmorestupidless/tap/ankka-flow from the same Homebrew tap ankka's CLI ships through.
The native binary does exactly what the JVM-built flow does — verify, generate, reset, version,
every flag — proven by the CLI's own suite run against the binary rather than by trust, and by a
smoke script that asks the binary for each thing a native image can silently lose (classpath
resources, reflection-dependent paths through the Kubernetes client, YAML writing). Today flow
needs a JVM and is built from source (sbt cli/stage), which is the first thing every guide makes a
reader do. After this: the install page says brew install, or download the archive for the
platform; building from source stays documented for contributors; just cli still works. Out of
scope: a native build of the sidecar or operator; Windows; a package for apt, dnf or winget;
signing or notarisation beyond what a tar.gz and a checksum give; any change to what flow does."

## Context

`flow` is the command a person runs to use ankka-flow: it verifies a blueprint, writes the
`AnkkaFlow` resource a cluster runs, requests resets, and says which version of the protocol it
writes. Everything else in the platform runs in a cluster, from images the release publishes.
`flow` alone has to be on the reader's machine, and today the only way to put it there is to clone
the repository, have a JVM and sbt, and build it. The install page says so in its first sentence,
and every guide's first command is that build.

ankka solved the same problem for its own CLI: a release attaches one executable per platform,
with a checksum, and a Homebrew formula in `thinkmorestupidless/homebrew-tap` installs it with one
command. This feature gives `flow` the same, through the same tap, so that a person who has ankka
installed gets ankka-flow the same way.

The risk is not building the binary; it is a binary that builds and then does less than the JVM
one. A native image is assembled ahead of time from what the build can see, and what it cannot see
— a resource read by name, a class reached by reflection, a format written by a library that
discovers its writers at run time — is simply absent, with no error until the command that needed
it answers with nothing. `flow` reads descriptors, writes YAML and talks to a Kubernetes API, three
things that lean on exactly those mechanisms. So the binary is held to the JVM build by tests, not
by a successful build.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Install `flow` with one command (Priority: P1)

A person on a Mac or a Linux machine wants `flow`. They run one Homebrew command, or download one
archive for their platform from the release and unpack it, and `flow` is on their path. They have
no JVM and have not cloned anything.

**Why this priority**: It is the feature. Everything a reader does with ankka-flow starts with
`flow`, and today that start is a build from source.

**Independent Test**: On a clean machine of each supported platform with no JVM: install through
the tap, run `flow version`; separately, download the archive, verify its checksum, unpack, run
`flow version`.

**Acceptance Scenarios**:

1. **Given** a Mac or Linux machine with Homebrew and no JVM, **When** the person runs the tap's
   install command for ankka-flow, **Then** `flow` is on their path and `flow version` prints the
   release's version and the protocol version it writes.
2. **Given** a tagged release, **When** the person opens it, **Then** it offers one archive per
   supported platform — macOS on Apple silicon and Intel, Linux on x64 and arm64 — each with a
   checksum beside it.
3. **Given** a downloaded archive, **When** the person checks it against the published checksum
   and unpacks it, **Then** it holds one executable, `flow`, that runs without a JVM.
4. **Given** a person who already has ankka's CLI from the tap, **When** they install ankka-flow's,
   **Then** both are installed side by side from the same tap, and upgrading one does not touch the
   other.
5. **Given** a later release, **When** the person upgrades through Homebrew, **Then** they get that
   release's `flow`, and `flow version` says so.

---

### User Story 2 - The binary is the CLI, not an approximation of it (Priority: P1)

A person uses the installed `flow` exactly as the guides describe: verifies a blueprint with
descriptors and deploy-time configuration, generates a resource with images named, resets a
pipeline or one streamlet on a cluster. Every command, every flag and every message is what the
JVM-built `flow` gives, including the notes about compacted delta topics and the refusals.

**Why this priority**: A binary that installs in one command and then silently writes a resource
with a field missing is worse than the build from source. This story is what makes the first one
safe to ship.

**Independent Test**: The CLI's own test suite, run against the native binary instead of the
JVM-built one, passes whole; and a smoke script run on the binary of each platform exercises each
thing a native build can lose and fails loudly if any is missing.

**Acceptance Scenarios**:

1. **Given** the CLI's test suite, **When** it is pointed at the native binary, **Then** every case
   that passes against the JVM build passes against the binary, with no case skipped for the
   binary's sake.
2. **Given** a blueprint, descriptors and deploy-time configuration, **When** `flow verify` and
   `flow generate` are run with the binary and with the JVM build, **Then** their output is
   identical, byte for byte, including the resource YAML and every note.
3. **Given** a cluster with a pipeline, **When** `flow reset` is run with the binary, **Then** the
   reset is requested exactly as the JVM build requests it, with the same refusals when a streamlet
   is still running.
4. **Given** a native binary from which something the CLI needs at run time is missing, **When**
   the smoke script runs it, **Then** the script fails naming what was missing, and the release
   does not ship that binary.
5. **Given** `flow` run with no arguments, a wrong flag, or `--help`, **When** run with the
   binary, **Then** the usage text is the JVM build's.

---

### User Story 3 - The release does it, every time (Priority: P2)

A maintainer tags a release. Without any further action, the four binaries are built, each is
smoke-tested, each is attached to the release with its checksum, and the tap's formula is updated
to the new version and checksums. A release where any binary fails to build or to pass its smoke
test does not update the tap.

**Why this priority**: A binary built by hand once is a binary that is stale by the next release.

**Independent Test**: Tag a release and observe: four archives and four checksums on the release;
the tap's formula at the new version with the four checksums; `brew install` of the new version
works. Break one platform's build on a branch and observe the tap untouched.

**Acceptance Scenarios**:

1. **Given** a tag is pushed, **When** the release runs, **Then** it attaches four archives and four
   checksums to the tag's release and updates the tap's formula, with no step done by hand.
2. **Given** one platform's binary fails its build or its smoke test, **When** the release runs,
   **Then** the other platforms' archives are still attached, the formula is not updated, and the
   failure names the platform.
3. **Given** the release has already attached an archive for a platform, **When** that leg is run
   again, **Then** the archive is replaced, not duplicated, and the checksum matches the replacement.
4. **Given** a release of ankka-flow and a release of ankka on the same day, **When** both update
   the tap, **Then** neither overwrites the other's formula.

---

### User Story 4 - The docs say "install", and contributors still build (Priority: P3)

A reader of the install page sees one Homebrew command, or an archive to download, before any
mention of a JVM. Every guide that began with building the CLI begins with having it. A contributor
finds how to build from source where contributors look, and `just cli` still builds and stages the
JVM CLI as it does today.

**Why this priority**: The binary exists for readers; the pages are how they find it.

**Independent Test**: The install page's first instruction for `flow` is the install command; no
guide's first command is `sbt cli/stage`; the contributor page describes the build from source; the
docs build is clean; `just cli` works.

**Acceptance Scenarios**:

1. **Given** the install page, **When** a reader looks for how to get `flow`, **Then** the first
   thing offered is the tap's install command, then the archive, and the build from source is
   not there.
2. **Given** any guide or sample README that told the reader to build `flow`, **When** it is
   read after this feature, **Then** it tells them to install it, or assumes they have.
3. **Given** a contributor, **When** they look for how to build `flow` from source, **Then** the
   contributing page says `just cli` or the sbt task, and both still work.
4. **Given** the skills the docs site renders, **When** they describe getting started, **Then** they
   say install, not build.

### Edge Cases

- **A platform the release does not build for.** Homebrew on a platform with no bottle in the
  formula refuses to install, naming the platform; the install page lists the four.
- **A JVM on the machine.** The binary does not use it; `flow` runs the same with and without one.
- **A release where the binary's version is wrong.** The smoke script asks `flow version` for the
  tag's version and fails when it differs; a binary built from a dirty tree is refused before it is
  attached.
- **A resource or format the CLI gains later.** The suite run against the binary catches what the
  smoke script does not enumerate, which is why both exist: the smoke script for a fast, named
  failure on each platform's binary, the suite for completeness on one.
- **Two CLIs named differently on one tap.** ankka's formula installs `ankka`; this one installs
  `flow`. The formula names are `ankka` and `ankka-flow`.
- **A reader with the old instructions.** `just cli` and `sbt cli/stage` keep working, so a page or
  script that still builds from source is slower, not broken.
- **The Kubernetes client's reflection.** `flow reset` and `flow generate` reach the Kubernetes
  model through a client that finds classes by name at run time; a binary missing one answers with
  an error only when that command runs. The suite's reset and generate cases, run against the
  binary, are what hold it.
- **Checksum mismatch.** Homebrew refuses the install; a person downloading by hand is told to
  verify, and the page shows how.

## Requirements *(mandatory)*

### Functional Requirements

**The binary**

- **FR-001**: `flow` MUST be built as one native executable for each of four platforms: macOS on
  Apple silicon, macOS on Intel, Linux on x64, Linux on arm64. It MUST run with no JVM installed.
- **FR-002**: The native `flow` MUST accept every command and flag the JVM-built `flow` accepts
  and produce the same output for the same input: verification results and notes, the generated
  resource, reset requests and refusals, the version line, usage and errors.
- **FR-003**: `flow version` on the native binary MUST print the release's version and the
  protocol version, as the JVM build does.
- **FR-004**: Building the binary MUST fail, not succeed with something missing, when a resource,
  class or writer the CLI needs cannot be included; where the build cannot know, the smoke script
  (FR-008) MUST catch it.

**Proof**

- **FR-005**: The CLI's test suite MUST be runnable against the native binary in place of the
  JVM build, by one switch, with every case kept; a case that cannot run against a binary is a
  defect of the case, not a skip.
- **FR-006**: The suite run against the binary MUST be part of the release: a platform whose
  binary fails it does not ship.
- **FR-007**: `verify` and `generate` MUST produce byte-identical output from the binary and the
  JVM build for the samples in the repository, and the release MUST check it.
- **FR-008**: A smoke script MUST run each platform's binary and ask it for each thing a native
  build can silently lose — the version, the usage text, a verification with descriptors read from
  disk, a generated resource written as YAML with images and deploy-time configuration applied, and
  a reset request against a Kubernetes API double or a refusal that proves the client is reachable —
  failing with the name of whatever is missing.

**The release**

- **FR-009**: Every tagged release MUST attach one archive per platform, named for the version and
  platform, holding the single executable `flow`, with a checksum file beside each.
- **FR-010**: The release MUST build each platform's binary on that platform from a clean tree at
  the tag, and MUST refuse a dirty tree.
- **FR-011**: A platform's leg MUST be re-runnable, replacing its archive and checksum rather than
  adding a second.
- **FR-012**: The release MUST update the formula `ankka-flow` in the tap
  `thinkmorestupidless/homebrew-tap` with the version and the four checksums, only after every
  platform's archive is attached, and MUST NOT touch any other formula in the tap.
- **FR-013**: The formula MUST install the executable as `flow`, and MUST coexist with ankka's
  formula, which installs `ankka`.
- **FR-014**: The images, the Python SDK and the marketplace plugin the release already publishes
  MUST be published as before, whether or not the binaries' legs succeed; the formula alone waits
  on them.

**Documentation and the build**

- **FR-015**: The install page MUST offer, in this order: the tap's install command; the archive
  per platform with how to verify its checksum; and nothing about building from source.
- **FR-016**: Every page and sample README that told the reader to build `flow` MUST tell them to
  install it or assume they have.
- **FR-017**: Building from source MUST remain documented for contributors, and `just cli` and the
  JVM build MUST keep working as they do.
- **FR-018**: The skills rendered from the docs MUST reflect the install path.

### Key Entities

- **Native binary**: one executable, `flow`, for one platform, built at a tag, equal in behaviour
  to the JVM build of the same commit.
- **Release archive**: a compressed archive holding the binary, named by version and platform,
  with a checksum file beside it on the tag's release.
- **Formula**: the Homebrew definition `ankka-flow` in the shared tap, naming the version and the
  four archives' checksums, installing `flow`.
- **Smoke script**: a script that runs a binary and asks it for each thing a native build can lose,
  failing with the name of the first thing missing.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: On each of the four platforms, with no JVM installed, `flow` is installed and
  answers `flow version` within one minute of starting the install, through the tap or from the
  archive.
- **SC-002**: The CLI's test suite passes in full against the native binary — the same count of
  cases as against the JVM build, with zero skipped.
- **SC-003**: For every sample in the repository, `flow verify` and `flow generate` give
  byte-identical output from the binary and the JVM build.
- **SC-004**: A tagged release produces four archives, four checksums and an updated formula with
  no manual step; a release with one broken platform leaves the formula untouched.
- **SC-005**: A reader following the install page reaches a working `flow` without the words
  "sbt", "JVM" or "clone" appearing before it; no guide's first command builds the CLI.
- **SC-006**: `just cli` and the JVM build pass the same suite they pass today.

## Assumptions

- **The tap is ankka's.** `thinkmorestupidless/homebrew-tap` already carries `ankka`; this feature
  adds `ankka-flow` beside it, published the way ankka's release publishes its formula, and needs
  the same write access to the tap that ankka's release has.
- **The release attaches to a GitHub release of the tag.** ankka-flow's release publishes images,
  the SDK and the plugin but creates no release page today; attaching archives needs one, which the
  workflow creates for the tag if it does not exist.
- **The JVM build stays.** The native binary is a second artifact from the same code; the staged
  JVM CLI is what `just cli` builds and what contributors run from source.
- **The four platforms are ankka's four.** The same runners build them.
- **A Kubernetes API double is enough for reset in tests.** The suite already tests `reset`
  against one; the binary is held to the same.
- **Checksums are the integrity guarantee.** Signing and notarisation are out of scope; macOS
  users installing through Homebrew are not affected, and users running a downloaded binary may be
  asked by the system to allow it, which the page says.

## Out of Scope

- A native build of the sidecar or the operator; they run in the cluster from images.
- Windows, in any form.
- Packages for apt, dnf, winget, or any store.
- Signing, notarisation, or any attestation beyond a checksum.
- Any change to what `flow` does, prints or accepts.
- A native `flow` in the sidecar or operator images, or in the marketplace plugin.
