# Feature Specification: `flow init` — Start a Streamlet Project in One Command

**Feature Branch**: `006-flow-init`

**Created**: 2026-10-07

**Status**: Draft

**Input**: User description: "flow init: start a streamlet project in one command. `flow init <name>
--language scala|python` (default scala; also --dir and --package) writes a new directory holding a
working streamlet project: a streamlet with one inlet and one outlet that does something visible to
each record, its harness test, the descriptor the SDK writes for it (committed, and checked by the
project's own descriptor command), a blueprint wiring it between an input topic and an output topic,
the image build, the laptop loop, deploy-time configuration for a kind cluster, a README with the
commands, a .gitignore, a CI workflow that runs the tests and the descriptor check, and the
ankka-flow agent skills so a coding agent knows the platform. The project depends on the published
SDK at exactly the version of the flow that wrote it. The templates ship inside flow, the native
binary included, so init needs no network, no template tool and no clone. init refuses a name the
descriptor rules refuse, a directory that is not empty, and an option that belongs to the other
language. Proof: a suite renders each language's project, points its SDK dependency at this
repository's SDK, and runs the project's own tests, descriptor check and `flow verify`; the native
smoke script runs init for both languages; the first-streamlet tutorial and the install page start
from `flow init`. Out of scope: an MCP server and its project file; other languages; scaffolding a
multi-streamlet pipeline; a template fetched from elsewhere; any change to the SDKs, the sidecar,
the operator or the protocol."

## Context

`flow` installs in one command and both SDKs are published, but a person starting their own
streamlet still begins by reading a sample in the repository and copying its files: the build, the
streamlet, the test, the descriptor command, the image, the compose file and the sidecar's
configuration, the blueprint. Each sample also points at the SDK by path, so a copied sample does
not build outside the repository until it is edited.

ankka closed the same gap with `ankka init`, which writes a complete service project for each of its
languages from templates carried inside the CLI. `flow init` does the same for a streamlet: one
command gives a directory that builds, tests, writes and checks its descriptor, verifies its
blueprint, runs on a laptop beside the sidecar and builds its image — depending on the published SDK
of the same version as the `flow` that wrote it.

## Clarifications

### Session 2026-10-07

- Q: The five proposed glossary terms (project, template, image, package, tutorial): accept, reword or defer? → A: Accept all five, each with an `Avoid:` line; none remain proposed.
- Q: What does the generated streamlet do to each record? → A: It reads a JSON object, adds a `greeting` field whose value is the parameter `greeting` (default `hello, ankka-flow`), and emits it with the same key and headers; a value that is not a JSON object fails the batch.
- Q: How does the laptop loop put records in and read them out? → A: The README uses Kafka's own console producer and consumer through `docker compose exec`, the same commands for both languages; the project carries no produce or verify program.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - A working streamlet project from one command (Priority: P1)

A person with `flow` installed runs `flow init` with a name, in Scala or Python, and gets a
directory whose tests pass, whose committed descriptor matches its declaration, and whose blueprint
`flow verify` accepts — with nothing edited and nothing fetched but the SDK and its build tools'
own dependencies.

**Why this priority**: It is the feature. Everything else here is what that project carries.

**Independent Test**: For each language, run `flow init` into an empty directory, then run the
project's tests, its descriptor check, and `flow verify` of its blueprint; each passes.

**Acceptance Scenarios** *(each names a scenario in a living feature; none is written here)*:

- added `features/cli/init.feature`: a project from flow init passes its own tests
- added `features/cli/init.feature`: a project's committed descriptor is what its streamlet declares
- added `features/cli/init.feature`: a project's blueprint verifies against its descriptor
- added `features/cli/init.feature`: a project depends on the SDK of the flow that wrote it
- added `features/cli/init.feature`: the streamlet adds a greeting to each record
- added `features/cli/init.feature`: the greeting is the parameter's deploy-time value
- added `features/cli/init.feature`: a value that is not a JSON object fails the batch

---

### User Story 2 - Refusals that name the problem (Priority: P1)

A person who gives `flow init` a name the protocol would refuse, a directory that already holds
files, or an option that belongs to the other language is refused before anything is written, with
a message that says what to change.

**Why this priority**: A half-written directory, or a project whose streamlet name the sidecar
later refuses, is worse than no project.

**Independent Test**: Each refusal case leaves the target untouched and exits non-zero with a
message naming the problem.

**Acceptance Scenarios** *(each names a scenario in a living feature; none is written here)*:

- added `features/cli/init.feature`: flow init refuses what it cannot write a working project for

---

### User Story 3 - From the project to the laptop and the cluster (Priority: P2)

From the generated project, a person follows its README to run the streamlet on a laptop beside
Kafka and the sidecar and watch records go through it, to build its image, and to deploy it to a
local cluster with `flow generate`.

**Why this priority**: A project that tests green but cannot be run is a sample, not a start.

**Independent Test**: For each language, the README's laptop loop moves records from the input
topic to the output topic through the streamlet, and its image builds.

**Acceptance Scenarios** *(each names a scenario in a living feature; none is written here)*:

- added `features/cli/init.feature`: a project runs on a laptop beside the sidecar
- added `features/cli/init.feature`: a project builds its image
- added `features/cli/init.feature`: a project's CI checks its tests and its descriptor

---

### User Story 4 - In the native binary, and in the docs (Priority: P2)

The templates are inside `flow` itself, so the native binary writes the same projects as the JVM
build with no network. The first-streamlet tutorial and the install page start a reader from
`flow init`, and a generated project carries the ankka-flow agent skills, so a coding agent opened
in it knows the platform.

**Why this priority**: The native binary is how readers get `flow`; a template it cannot find is a
command that fails only for them.

**Independent Test**: The native binary's smoke script runs `flow init` for both languages and
finds every file the JVM build writes; the docs build is clean and the tutorial's first command is
`flow init`.

**Acceptance Scenarios** *(each names a scenario in a living feature; none is written here)*:

- added `features/cli/init.feature`: the native binary writes the same project as the JVM build
- added `features/cli/init.feature`: a project carries the agent skills of its flow's version
- added `features/cli/install-page.feature`: the first-streamlet tutorial starts from flow init

### Edge Cases

- **A name the protocol refuses** (capitals, a leading or trailing hyphen, longer than 63
  characters, an empty name): refused before anything is written, naming the rule.
- **A name starting with a digit**: the protocol allows it for a streamlet, but the name also
  becomes a class, a package and a module, so `flow init` refuses it, naming why.
- **A package name that is not one** (for Scala, not a dotted lowercase identifier; for Python, not
  a module name): refused, naming the rule.
- **A target directory that exists and is not empty**: refused; an existing empty directory is
  used.
- **`--package` with a language that does not take it**, or another option of the other language:
  refused, not silently ignored.
- **A `flow` built from source between releases**: its version is not on Maven Central or PyPI;
  the project names that version, and the README says to build against a released `flow`, or the
  repository's SDK, for such a build.
- **No network at init time**: init succeeds; only the project's own build fetches dependencies.
- **A generated project inside a git repository**: init writes files only; it does not run `git`.

## Requirements *(mandatory)*

### Functional Requirements

**The command**

- **FR-001**: `flow init <name>` MUST write a new streamlet project into `<name>` (or `--dir`), in
  the language `--language` names: `scala` (the default) or `python`.
- **FR-002**: `flow init` MUST refuse, before writing anything, a name the descriptor rules refuse
  for a streamlet name, a target directory that exists and is not empty, an invalid `--package`, and
  any option that does not apply to the chosen language; each refusal MUST name what to change and
  exit non-zero.
- **FR-003**: The templates MUST be carried inside `flow`, the native binary included; `flow init`
  MUST need no network, no template tool and no copy of the repository.
- **FR-004**: The project MUST depend on the published SDK at exactly the version of the `flow`
  that wrote it.

**The project**

- **FR-005**: The project MUST hold one streamlet with one JSON inlet and one JSON outlet that
  reads each record as a JSON object, adds a `greeting` field whose value is its one parameter,
  `greeting` (default `hello, ankka-flow`), and emits the result with the record's key and headers;
  a value that is not a JSON object fails the batch, as the cart router's does.
- **FR-006**: The project MUST hold a harness test of the streamlet that passes as written.
- **FR-007**: The project MUST hold its streamlet's descriptor, committed, equal to what the SDK
  writes for the declaration, and the project's descriptor command and its check MUST work as
  written.
- **FR-008**: The project MUST hold a blueprint wiring the streamlet between an input topic the
  pipeline reads and an output topic it owns, which `flow verify` accepts against the descriptor.
- **FR-009**: The project MUST build an image holding only the streamlet and the SDK: with the
  build's own packaging for Scala and a Dockerfile for Python.
- **FR-010**: The project MUST hold the laptop loop — Kafka and the sidecar in containers, the
  sidecar's configuration for that network — and deploy-time configuration for a local cluster. Its
  README MUST put records on the input topic and read the output topic with Kafka's own console
  tools through the compose file, the same commands for both languages; the project carries no
  program of its own for that.
- **FR-011**: The project MUST hold a README with every command it needs, a `.gitignore`, and a CI
  workflow that runs its tests and its descriptor check.
- **FR-012**: The project MUST carry the ankka-flow agent skills of the `flow` version that wrote
  it, where a coding agent opened in the project finds them.

**Proof and documentation**

- **FR-013**: A suite MUST render each language's project through the CLI, point its SDK
  dependency at this repository's SDK, and run the project's own tests, its descriptor check and
  `flow verify` of its blueprint.
- **FR-014**: The native smoke script MUST run `flow init` for both languages and find every file
  the JVM build writes.
- **FR-015**: The first-streamlet tutorial and the install page MUST start a reader from
  `flow init`; the CLI reference MUST describe it.
- **FR-016**: A template the build carries MUST NOT drift from what the CLI writes: a template file
  missing from the CLI's index, or an index entry with no file, MUST fail the build.

### Key Entities

- **project**: the directory `flow init` writes: a streamlet, its test, its descriptor, a blueprint,
  an image build, the laptop loop, deploy-time configuration, a README, a CI workflow and the agent
  skills.
- **template**: the files for one language that the CLI carries, with names to be replaced by the
  project's.
- **CLI**: `flow`, which carries the templates and writes the project.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: For each language, a project written by `flow init` passes its own tests, its
  descriptor check and `flow verify` with no file edited.
- **SC-002**: A person with `flow`, Docker and the language's build tool goes from no directory to
  records flowing through their own streamlet on a laptop in under ten minutes, following only the
  project's README.
- **SC-003**: The native binary writes byte-identical projects to the JVM build for both languages.
- **SC-004**: `flow init` itself completes in under a second, with no network.
- **SC-005**: The SDKs, the sidecar, the operator, the protocol and `flow`'s other commands pass the
  suites they pass today.

## Assumptions

- **ankka's embedded templates, not its Giter8 path.** ankka carries its Python, TypeScript and
  Rust templates in the CLI as resources with an index per language and plain token replacement, and
  fetches its Scala template with `sbt new`. The embedded path is the one pinned to the CLI's version
  and the one a native binary can serve; both languages here use it.
- **One streamlet, the cart router's shape.** The generated streamlet is smaller than the cart
  router: it adds a greeting to each record, so the change is visible in a console consumer and the
  parameter's deploy-time value is too. A multi-streamlet pipeline is out of scope.
- **The skills are the published plugin's.** The project's `.claude/skills/` is the ankka-flow
  plugin's skills as rendered for that `flow` version, as ankka's projects carry ankka's.
- **No MCP file yet.** A `.mcp.json` names a server command; it arrives with the MCP feature.
- **The Python project uses uv; the Scala project uses sbt**, as the SDKs' own projects and the
  samples do.

## Out of Scope

- An MCP server, `flow mcp`, and a `.mcp.json`.
- Languages other than Scala and Python; a Java template.
- A pipeline of several streamlets, or a built-in stage, in the generated project.
- Templates fetched from a repository, Giter8, `sbt new` or `uv init` templates.
- Any change to the SDKs, the sidecar, the operator or the protocol.
