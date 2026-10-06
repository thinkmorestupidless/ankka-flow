# Implementation Plan: A Scala SDK

**Branch**: `005-scala-sdk` | **Date**: 2026-10-06 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/005-scala-sdk/spec.md`

## Summary

A streamlet author writes a streamlet in Scala 3 as a `Streamlet` subclass whose ports and
parameters are declared by the base class's factories at construction, implements `process` over a
batch, tests it with a harness that is the Python testkit name for name, writes its descriptor with
a main the build runs, and serves it with `Serve.run` (R3–R5, R7). The SDK is a module of this
build that depends on the repository's `protocol` module rather than copying it — the canonical
descriptor writer, validation and fingerprints are already there (R1) — and both are published to
Maven Central for Scala 3.3 LTS on Java 21 by `sbt ci-release`, as ankka's artifacts are, from a
release job gated on a release tag (R2, R9). The conformance suite's in-process target becomes the
SDK's reference streamlet, so `sidecar/test` proves the SDK on every build (R6). The Scala cart
router is a module beside the Python sample, runs the Python sample's blueprint and compose file
unchanged because the `streamlet` part of its descriptor is the same, and ships its own image
(R8). Every page with Python code gets a Scala tab from that tested code, plus a Scala guide, a
Scala SDK reference and a fourth skill (R11).

## Technical Context

**Language/Version**: Scala 3.3 LTS (3.3.8) for `protocol`, `sdk` and the sample; Scala 3.9 for
the rest, unchanged; Java 21; YAML for the two workflows

**Primary Dependencies**: nothing new. ScalaPB runtime, grpc-stub and grpc-netty-shaded come
with `protocol`; munit for tests; slf4j-api in the SDK with no binding; `slf4j-simple` in the
sample. `sbt-ci-release` is already in `project/plugins.sbt`

**Storage**: none. Artifacts on Maven Central; one image in the registry

**Testing**: munit suites in `sdk` (fixtures, harness, server, `Descriptor`), the sidecar's
`ConformanceSuite` with the SDK as its in-process target, the sample's two tests and
`descriptorCheck`, the laptop walkthrough's `produce.py`/`verify.py`, `docs build`, the README
include check; a local publish rehearsal into a directory

**Target Platform**: any JVM 21+ for the SDK; the sample's image on `eclipse-temurin:21-jre`

**Project Type**: library (two published modules), a sample, a release job, documentation

**Performance Goals**: the harness runs a sample test in milliseconds; the in-process
conformance run adds seconds to `sidecar/test`; `process` concurrency per partition as the sidecar
drives it

**Constraints**: no change to `protocol/`'s files, the sidecar's behaviour, the Python SDK or
`flow`; the published modules on the LTS Scala; warning-free compile on both compilers; module
direction kept (`sdk → protocol`; `sidecar → sdk` in test scope only); the Python sample's
blueprint and compose file reused, not copied; the release's pre-release gate unchanged

**Scale/Scope**: `build.sbt` (+2 modules, publish metadata, aliases), `project/Dependencies.scala`
(`V.scalaLts`), `sdks/scala/` (about a dozen source files, four suites), `sidecar`'s
`ConformanceTarget`, `samples/cart-router-scala/` (streamlet, main, two tests, descriptor,
README), `ci.yml` (+1 filter, +1 step), `release.yml` (+1 job, +1 line), `Justfile` (+1), 2 new
docs pages, 5 pages gaining tabs, 3 pages gaining a sentence, `mkdocs.yml`, 1 new skill and 2
changed, `README.md`, `CLAUDE.md`, the contributing page's SDK rule

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

`.specify/memory/constitution.md` is the unfilled template, so the gate is `CLAUDE.md`'s rules
table:

| Rule | This feature |
|---|---|
| Module direction | Extended, not broken: `sdk → protocol`; `cartRouterScala → sdk`; `sidecar → sdk` in test scope, so the conformance suite can serve the SDK in process. Nothing depends on `sidecar` or `operator`. The table in `CLAUDE.md` is updated in the same commit. |
| The sidecar never decodes for a process | Untouched; the SDK decodes nothing either (FR-003). |
| Commit after the write | Untouched. |
| Never skip | Untouched; the SDK's rule is the Python one's: skipping is `process` emitting nothing. |
| Explicit registration | Kept: ports and parameters are registered by construction; no scanning (R3). |
| Pure rendering, one place for I/O | Untouched. |
| The resource says what runs | Untouched. |
| Kafka credentials reach only the sidecar | Untouched; the SDK sees no Kafka. |
| No literal image tags in tests | Kept; the sample's image is tagged by `Docker / version`. |
| Warning-free compile | Kept on both compilers; the LTS modules drop `-source:3.7` and keep `-Wunused:all`. |
| Built-in stages are declared values | Untouched. |
| Not in this build | Nothing added: grpc-java and ScalaPB are the protocol's; no pekko-grpc, no Avro. |
| Test switches must be forwarded | No new switch; `flow.conformance.target` and `flow.repo.root` are already forwarded and the SDK's suites read the latter. |
| Docs: a page stands alone; a new page in `nav` and a skill | Kept (R11). |
| Samples are included from tested code | Kept: every Scala block on a page or in the README comes from the sample or the SDK's tests. |

**Violations to justify**: none.

## Project Structure

### Documentation (this feature)

```text
specs/005-scala-sdk/
├── plan.md              # this file
├── research.md          # R1–R12 and what to verify first
├── data-model.md        # streamlet, ports, parameters, records, descriptor, harness, conversation, artifact, sample
├── quickstart.md        # four tiers and the reviewer's checklist
├── contracts/
│   ├── scala-sdk-api.md       # every public name and the rules it carries
│   └── build-and-release.md   # modules, commands, CI, the release job, the secrets
├── checklists/requirements.md
└── tasks.md             # /speckit-tasks output (not created here)
```

The acceptance scenarios live in `features/sdk/*.feature`; the spec names them.

### Source Code (repository root)

```text
project/Dependencies.scala                  # V.scalaLts; slf4jApi, slf4jSimple
build.sbt                                   # publish metadata; protocol on LTS; sdk; cartRouterScala; sidecar test dep; aliases; skips
sdks/scala/
├── src/main/scala/com/thinkmorestupidless/ankka/flow/sdk/
│   ├── Streamlet.scala                     # the base class, registration, runBatch, UndeclaredOutlet
│   ├── Ports.scala                         # JsonInlet, JsonOutlet, GraphDeltaOutlet, emit
│   ├── Parameters.scala                    # Parameter, the six factories, Config, HOCON duration and size parsing
│   ├── Records.scala                       # Record, Batch, Emit
│   ├── Descriptor.scala                    # spec, write, validate, main (--check)
│   ├── Serve.scala                         # the gRPC server, Conversation, the worker pool
│   └── graph.scala                         # Delta and its validation, as the Python graph module
├── src/main/scala/.../sdk/testkit/Harness.scala
├── src/test/scala/.../sdk/
│   ├── DescriptorFixturesSuite.scala       # the six fixtures, byte for byte
│   ├── HarnessSuite.scala                  # the scala-streamlet.feature scenarios
│   ├── ServeSuite.scala                    # discovery, start, concurrency, supersession, stop
│   ├── DescriptorMainSuite.scala           # write, --check, refusals
│   └── conformance/{Conformance,ConformanceMain}.scala   # the reference streamlet and its server
└── README.md
sidecar/src/test/scala/.../conformance/ConformanceTarget.scala   # InProcess serves the SDK's Conformance
samples/cart-router-scala/
├── src/main/scala/cart/{CartRouter,Main}.scala                   # docs:start router / main
├── src/test/scala/cart/CartRouterSuite.scala                     # docs:start routes-by-total / ordering; the fixture check
├── flow/descriptor.json                                          # written by sbt cartRouterScala/descriptor
└── README.md                                                     # the dependency line; the laptop loop with the Python sample's compose
.github/workflows/ci.yml                    # build filter + descriptorCheck and image steps
.github/workflows/release.yml               # sdk-scala; images publishes the Scala sample's image
Justfile                                    # just sdk-scala
docs/build/scala-streamlet.md               # new, beside python-streamlet.md
docs/reference/scala-sdk.md                 # new, beside python-sdk.md
docs/get-started/{install,first-streamlet}.md, docs/build/{testing,images}.md, docs/concepts/contracts.md   # Scala tabs
docs/build/{ankka-topics,graph-sink,graph-from-ankka}.md         # one sentence and a link
docs/contributing/language-sdks.md          # the copy rule is for SDKs outside this build
mkdocs.yml, tools/docs/skill/ankka-flow-scala/SKILL.md, tools/docs/skill/{ankka-flow,ankka-flow-python}/SKILL.md
README.md, CLAUDE.md
```

**Structure Decision**: two new modules of the one build, `sdk` at `sdks/scala` beside the Python
SDK and the sample at `samples/cart-router-scala` beside the Python one, so the paths a reader
sees are the Python paths with the language swapped. No new repository, no standalone build.

## Order of work

1. `protocol` on the LTS Scala, warning-free, with everything above it still compiling (R2;
   verify first).
2. The SDK's core: records, ports, parameters, `Streamlet` with registration and validation,
   `Descriptor`; the fixtures suite green (tier 1).
3. `Serve` and the `Conversation`; the sidecar's in-process target switched to it; the
   conformance suite green both ways (tier 2).
4. The harness and its suite; `graph` at parity.
5. The sample: streamlet, tests, descriptor, image, README; the laptop loop with the Python
   sample's compose (tier 3).
6. Publishing: metadata, skips, the rehearsal, the release job; CI's steps; `just sdk-scala`.
7. The docs, the skills, the README, `CLAUDE.md`, the contributing page.
8. The whole build; a pre-release tag to see the gate hold (tier 4).

## Complexity Tracking

Nothing violates a rule. What is added, and why nothing simpler does:

| Addition | Why Needed | Simpler Alternative Rejected Because |
|---|---|---|
| A second published module (`protocol`) | the SDK's POM must resolve the protocol classes it uses (R1) | shading hides a shared dependency; a copy duplicates generated code in one build |
| Two Scala versions in one build | the published modules must serve LTS projects; the rest was written for 3.9 (R2) | one version either strands LTS readers or moves three applications off their compiler |
| `sidecar → sdk` in test scope | the conformance suite lives in `sidecar` and must drive the SDK in process (R6) | a copy of the suite in `sdk`, or a second JVM per run |
| A `Descriptor` main instead of a plugin | the descriptor needs the constructed object; a plugin is out of scope (R4) | a macro at compile time cannot render defaults or fingerprints of a constructed object |
| Tabs on four pages | one tutorial, two languages (R11) | a page per language duplicates every step but one block |

## Constitution Check (post-design)

Re-evaluated after Phase 1: all kept. The design adds two modules and one test-scope edge to the
module graph, no library, no change to the protocol's files, the sidecar's behaviour, the Python
SDK or `flow`.

Found while planning, and written back to the spec and the features:

- The SDK depends on `protocol` instead of copying it (R1): FR-010 and the scenario "the Scala
  SDK's copy of the protocol is the repository's" are rewritten as "the Scala SDK ships no copy of
  the protocol".
- A descriptor's `sdk` block names its SDK, so the two routers' descriptors differ there and only
  there (R4): FR-012, SC-002 and the scenario "the Scala cart router and the Python one have the
  same descriptor" now say the `streamlet` part is identical.
- The Python sample's compose file and `streamlet.conf` run the Scala router unchanged (R8), so
  the laptop walkthrough is one set of files for two languages.
- Publishing needs four secrets on this repository (contracts/build-and-release.md).

## Not in this feature

In-process hosting inside the sidecar's JVM; a Java API; Scala 2; an sbt plugin or giter8
template; Scala twins of the checkout-feed and checkout-graph samples; any change to the protocol,
the sidecar, the operator, `flow` or the Python SDK; a test-only break switch in the Scala server
(R12).
