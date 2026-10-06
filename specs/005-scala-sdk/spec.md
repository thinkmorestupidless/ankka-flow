# Feature Specification: A Scala SDK

**Feature Branch**: `005-scala-sdk`

**Created**: 2026-10-06

**Status**: Draft

**Input**: User description: "A Scala SDK: write a streamlet in Scala 3 the way the Python SDK lets
you in Python, and run it the same way. A streamlet is a class declaring its typed inlets, outlets
and parameters and a process method that takes one batch and returns what to emit; the SDK decodes
nothing and keeps nothing, exactly as the Python one. The SDK writes the streamlet's descriptor at
build time (so the blueprint can be verified against it and the sidecar can refuse a difference),
serves the process on FLOW_PROCESS_PORT for the sidecar in its pod, and ships a test harness that
runs process with no Kafka and no sidecar, partitioning by key and batching like the sidecar does.
It copies protocol/ byte for byte, passes the descriptor fixtures and the conformance suite, like
the Python SDK, and is published to Maven Central as a Scala 3 artifact on every release (ankka's
own artifacts are published there the same way). A Scala sample of the cart router — the same
streamlet, the same blueprint, the same tests, its own descriptor and image — sits beside the
Python one, is run by CI and published by the release, and the README shows the router in Scala
beside the Python one, included from that tested code. The documentation gets the Scala side of
each page that has a Python side: the first-streamlet tutorial, a "write a streamlet in Scala"
guide, testing, images, the SDK reference, the install page, and the skills. Out of scope: embedded
in-process hosting of a streamlet inside the sidecar's JVM; a Java API; an sbt plugin or giter8
template that scaffolds a streamlet project; any change to the protocol or to what the sidecar
does; a Scala graph-delta outlet beyond what the protocol already carries."

## Context

A streamlet's logic is written in any language and shipped as an image holding only that code; the
sidecar in its pod owns everything Kafka and talks to the process over the streamlet protocol. Today
the only SDK that speaks that protocol for a streamlet author is the Python one, so "any language"
is, for a reader, Python. The repository is itself Scala — the sidecar, the operator, the CLI and
the conformance suite are — and ankka, the platform ankka-flow runs beside, has Scala as its first
language, so a person who writes ankka services in Scala and wants a pipeline beside them has to
change language to write a streamlet.

The protocol was designed so that an SDK is small: declare the streamlet, write its descriptor,
serve two services on a loopback port, and run the conversation the sidecar drives. What makes an
SDK trustworthy is not its size but two checks the repository already holds for any language: the
descriptor fixtures, which pin the bytes of the descriptor for six declared streamlets, and the
conformance suite, which drives a served streamlet through every conversation the sidecar can have
with it. The Python SDK passes both; a Scala SDK is held to the same.

The Scala SDK gives a streamlet author the same things the Python SDK gives, in the same shape: a
streamlet declared as a class with typed ports and parameters, a process method over one batch, a
descriptor written by the build, a server the sidecar dials, and a harness that runs the streamlet
with no Kafka and no sidecar. It is published to Maven Central with every release, as ankka's
artifacts are, so a project depends on it by version. The cart router sample is written in Scala
beside the Python one, so every page that shows Python can show Scala, and the README shows both.

## Clarifications

### Session 2026-10-06

- Q: The glossary's 53 proposed terms (21 from the native-binary feature, 32 from this one): accept, reword or defer? → A: Accept all as written, each with an `Avoid:` line for its obvious synonyms; none remain proposed.
- Q: Which Scala 3 and Java baseline does the published SDK require? → A: Scala 3.3 LTS and Java 21: the SDK is published for the LTS line and usable from any Scala 3.3 or later project, on Java 21 or later.
- Q: What is the artifact's name on Maven Central? → A: `ankka-flow-sdk` in group `com.thinkmorestupidless`, resolved as `ankka-flow-sdk_3`: a project depends on `"com.thinkmorestupidless" %% "ankka-flow-sdk" % "<version>"`.
- Q: How is the Scala sample built? → A: As a module of the repository's build, depending on the SDK module directly, so every commit proves the two together; the sample's README shows the published dependency line a reader's own project uses.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Write and test a streamlet in Scala (Priority: P1)

A streamlet author adds the SDK to a Scala 3 project, declares a streamlet — its name, its JSON
inlets and outlets by schema name, its typed parameters — and implements `process` over one batch,
returning what to emit. They test it with the SDK's harness: records put on an inlet, keyed, are
batched and partitioned by key as the sidecar would, and the outlets' records are asserted on.
The SDK decodes nothing and keeps nothing: a record is bytes with a key and headers, and the
streamlet decides what to make of it.

**Why this priority**: It is the feature. Without a streamlet an author can write and test, nothing
else here has a user.

**Independent Test**: The cart router, declared in Scala, passes a test that routes by total and
a test that keeps each cart's events in order across partitions and batch sizes — the same tests
the Python sample has — through the harness alone, with no Kafka and no sidecar.

**Acceptance Scenarios** *(each names a scenario in a living feature; none is written here)*:

- added `features/sdk/scala-streamlet.feature`: a streamlet declared in Scala routes a batch as the Python one does
- added `features/sdk/scala-streamlet.feature`: the harness batches and partitions records by key as the sidecar does
- added `features/sdk/scala-streamlet.feature`: a failing batch leaves no emit behind
- added `features/sdk/scala-streamlet.feature`: an emit to an undeclared outlet fails the batch
- added `features/sdk/scala-streamlet.feature`: a parameter's value comes from the deploy-time configuration, or its default
- added `features/sdk/scala-streamlet.feature`: the SDK decodes nothing and keeps nothing

---

### User Story 2 - The descriptor and the conversation are the protocol's (Priority: P1)

The build writes the streamlet's descriptor, byte for byte what the protocol defines, so `flow`
verifies a blueprint against it and the sidecar refuses a process whose discovery differs from the
deployed file. Served on the process port, the streamlet answers discovery, takes a start with its
configuration, processes batches of different partitions concurrently, emits before it
acknowledges, fails a batch its code failed, and stops cleanly. The descriptor fixtures and the
conformance suite are the proof, exactly as for the Python SDK.

**Why this priority**: A Scala streamlet that tests green on a laptop and is refused by the
sidecar, or reorders a batch's emits, is a streamlet nobody can deploy. The protocol is the
contract, and this story holds the SDK to it.

**Independent Test**: The SDK's own tests declare the six fixture streamlets and assert their
descriptors equal the fixtures' bytes; the SDK serves the reference streamlet on a port and the
conformance suite passes against it, every case that applies to a process.

**Acceptance Scenarios** *(each names a scenario in a living feature; none is written here)*:

- added `features/sdk/scala-protocol.feature`: the descriptor written in Scala equals the fixture's bytes
- added `features/sdk/scala-protocol.feature`: a declaration the protocol refuses is refused before a descriptor is written
- added `features/sdk/scala-protocol.feature`: the conformance suite passes against the Scala reference streamlet
- added `features/sdk/scala-protocol.feature`: the sidecar runs a Scala streamlet as it runs a Python one
- added `features/sdk/scala-protocol.feature`: the Scala SDK's copy of the protocol is the repository's

---

### User Story 3 - The Scala router beside the Python one (Priority: P2)

A reader finds the cart router in Scala beside the Python one: the same streamlet with the same
ports and parameter, the same blueprint, the same tests, its own descriptor and its own image. It
runs on a laptop with the sidecar and on a cluster with the operator exactly as the Python one
does, and CI runs its tests and checks its descriptor on every change. The README shows the router
in both languages, included from that tested code.

**Why this priority**: A sample is how a reader learns the shape, and how the build proves the
SDK does what the pages say. It is also what the pages and the README include.

**Independent Test**: From a clean checkout, the Scala sample's tests pass, its committed
descriptor equals what the build writes, its blueprint verifies with `flow` against that
descriptor, its image builds, and the laptop walkthrough's produce-and-verify round trip succeeds
with the Scala router in place of the Python one.

**Acceptance Scenarios** *(each names a scenario in a living feature; none is written here)*:

- added `features/sdk/scala-sample.feature`: the Scala cart router and the Python one have the same descriptor
- added `features/sdk/scala-sample.feature`: the Scala cart router runs the laptop walkthrough
- added `features/sdk/scala-sample.feature`: the Scala cart router's tests and descriptor are checked on every change
- added `features/sdk/scala-sample.feature`: the README shows the router in Scala and in Python from tested code

---

### User Story 4 - Published with every release (Priority: P2)

A streamlet author depends on the SDK by version from Maven Central, the version being the
ankka-flow release it belongs to, with the same protocol version that release's sidecar speaks. A
tagged release publishes it with no manual step, beside the images, the Python SDK and the plugin;
the Scala sample's image is published as the Python samples' are. A pre-release tag publishes
none of it.

**Why this priority**: An SDK that exists only in the repository is one an author has to build
from source, which is the thing the Python SDK's PyPI package and the native `flow` were done to
remove.

**Independent Test**: Tag a release and observe the artifact on Maven Central at the release's
version, a fresh Scala project depending on it by version compiling and running the router's test,
and the Scala sample's image in the registry; tag a pre-release and observe nothing published.

**Acceptance Scenarios** *(each names a scenario in a living feature; none is written here)*:

- added `features/sdk/scala-release.feature`: a release publishes the Scala SDK and the Scala sample's image with no manual step
- added `features/sdk/scala-release.feature`: a project depends on the published SDK by version
- added `features/sdk/scala-release.feature`: a pre-release publishes neither the SDK nor the image
- added `features/sdk/scala-release.feature`: a release run again does not publish the SDK twice

---

### User Story 5 - Every page with a Python side has a Scala side (Priority: P3)

A Scala reader follows the first-streamlet tutorial, writes a streamlet with a guide written for
Scala, tests it as the testing page shows, builds its image as the images page shows, and looks
names up in a Scala SDK reference. The install page says what a Scala author needs. The skills
rendered from the pages know the Scala SDK as they know the Python one.

**Why this priority**: The SDK exists for readers; the pages are how they find it, and a page
that shows only Python says the platform is Python.

**Independent Test**: Every page that shows Python streamlet code shows the Scala equivalent,
included from the Scala sample or the SDK's tested code; the docs build is clean; the skills
describe both SDKs.

**Acceptance Scenarios** *(each names a scenario in a living feature; none is written here)*:

- added `features/sdk/scala-docs.feature`: every page with a Python side has a Scala side from tested code
- added `features/sdk/scala-docs.feature`: a Scala reader is told what to install
- added `features/sdk/scala-docs.feature`: the skills describe the Scala SDK

### Edge Cases

- **A declaration the protocol refuses.** Two ports with one name, a port named like a parameter,
  a schema name that is empty, a parameter with a default of the wrong type: the SDK refuses it
  when the streamlet is constructed or when the descriptor is written, naming the problem, as the
  Python SDK and the descriptor rules do. No descriptor is written for it.
- **A process that fails a batch.** The exception's message is reported with the failure; the
  emits of that batch are discarded; the sidecar delivers the batch again. The harness shows the
  same: a failed batch's emits are not on the outlet.
- **Batches of two partitions at once.** `process` runs concurrently for different partitions and
  never twice at once for one partition; what a streamlet shares between calls is its own to make
  thread-safe, and the guide says so.
- **A restart of the sidecar mid-run.** A new run voids everything tied to the previous one; the
  SDK does not acknowledge a batch of a run that has ended.
- **A deployed descriptor that differs from discovery.** The sidecar refuses the process; this is
  the sidecar's behaviour and the SDK's job is to make discovery equal the written descriptor,
  which the fixtures prove.
- **A streamlet emitting with no key.** The record is produced with no key, as the protocol says;
  the harness places such a record as the sidecar would.
- **A port used in a blueprint that the Scala router does not declare.** `flow verify` refuses the
  blueprint, naming the port — the same as for the Python router, because the descriptors are the
  same.
- **A pre-release tag.** Nothing of this feature is published: a rehearsal of the binaries'
  release stays a rehearsal.
- **A JVM too old.** The SDK needs Java 21; a project on an older one fails to compile or resolve
  with a clear version message, not at run time.
- **A Scala 3 older than the LTS line.** The SDK is published for Scala 3.3 LTS; a project on an
  older 3.x fails to resolve it, and the install page names the floor.

## Requirements *(mandatory)*

### Functional Requirements

**Declaring and running a streamlet**

- **FR-001**: The SDK MUST let an author declare a streamlet in Scala 3 as a class with a name, a
  description, JSON inlets and outlets by schema name, a graph-delta outlet where the protocol
  carries one, and typed parameters of each of the six parameter types with optional defaults —
  the same declarations the Python SDK offers, and no streamlet discovered by scanning.
- **FR-002**: A streamlet MUST implement one method over one batch — the batch's inlet, partition
  and records in offset order, each record its bytes, optional key and ordered headers — returning
  the emits to make, each naming a declared outlet and carrying a record; an emit built from an
  inlet record MUST keep its key, headers and bytes unless the author replaces them.
- **FR-003**: The SDK MUST decode nothing and keep nothing: no codec is applied to a value, and no
  state is held across batches by the SDK on the streamlet's behalf.
- **FR-004**: A parameter's value MUST be readable in `process` from the configuration the start
  carried, typed as declared, falling back to the declared default.
- **FR-005**: The SDK MUST serve the streamlet on the loopback address and the port the platform
  gives the process, and nowhere else, with one call from the process's entry point.

**The protocol**

- **FR-006**: The SDK MUST write the streamlet's descriptor as the protocol's canonical JSON, with
  the SDK's name and version, from the build, to the path a sample commits and `flow` reads; a
  check MUST fail when the committed file differs from what the declaration writes.
- **FR-007**: For each of the six declared fixture streamlets, the descriptor the SDK writes MUST
  equal the fixture's bytes.
- **FR-008**: The SDK MUST refuse a declaration the descriptor rules refuse, naming the problem,
  before any descriptor is written or any port is served.
- **FR-009**: The SDK MUST pass the conformance suite, every case that applies to a process, when
  serving the reference streamlet; the SDK MUST offer one command that serves the reference
  streamlet and runs the suite.
- **FR-010**: The SDK MUST hold a byte-for-byte copy of the repository's protocol directory and
  generate its protocol code from that copy; the repository's build MUST fail when the copy
  differs.

**The harness**

- **FR-011**: The SDK MUST ship a test harness that runs a streamlet with no Kafka, no sidecar and
  no network: records put on an inlet with a key, value and headers are grouped into batches, one
  per partition in offset order, partitioned by a function of the key the test chooses and sized
  by a batch size the test chooses; emits are recorded on the outlets only when the batch
  succeeds; a thrown exception or an emit to an undeclared outlet fails the batch, discards its
  emits and records the failure; a record the streamlet acknowledged without emitting is recorded
  as skipped.

**The sample, the README and the build**

- **FR-012**: A Scala cart router sample MUST exist beside the Python one, declaring the same
  streamlet — the same name, ports, contracts and parameter — so that its descriptor equals the
  Python sample's, with the same blueprint, the same two tests, a committed descriptor and an image
  holding only the streamlet and the SDK.
- **FR-013**: CI MUST run the Scala sample's tests and descriptor check, the SDK's fixtures and the
  conformance run, and the protocol copy check, on every change that can affect them; the sample
  builds against the SDK's source as a module of the repository's build, so the two cannot drift.
- **FR-014**: The README MUST show the cart router in Scala beside the Python one, both included
  from the samples' tested code and checked for drift.
- **FR-015**: The Scala sample MUST run the laptop walkthrough as the Python one does — the
  sidecar in a container dialling the streamlet on the host — and deploy to a cluster with the same
  blueprint and `flow` invocation, the image name changed.

**The release**

- **FR-016**: Every release tag MUST publish the SDK to Maven Central as the Scala 3 artifact
  `ankka-flow-sdk` in the group `com.thinkmorestupidless`, the organisation ankka publishes to,
  versioned as the release, with no manual step, and MUST NOT publish twice for one version; a
  pre-release tag MUST NOT publish it.
- **FR-016a**: The published SDK MUST be usable from a project on Scala 3.3 LTS or any later Scala
  3, on Java 21 or later; the install page and the SDK reference MUST state both floors.
- **FR-017**: Every release tag MUST publish the Scala sample's image as it publishes the Python
  samples'; a pre-release tag MUST NOT.
- **FR-018**: The SDK MUST report its name and version in discovery and in the descriptor's SDK
  block as the published artifact's, so a descriptor records which SDK and version wrote it.

**Documentation**

- **FR-019**: Every page that shows Python streamlet code MUST show the Scala equivalent, included
  from the Scala sample or the SDK's tested code: the first-streamlet tutorial, the testing page,
  the images page, the contracts page, and the ankka-topics and graph pages where the Python sample
  shown has no Scala counterpart may instead say so and link the Scala guide.
- **FR-020**: A guide for writing a streamlet in Scala MUST exist beside the Python one, covering
  declaration, `process`, parameters, configuration, concurrency, the descriptor and serving.
- **FR-021**: A Scala SDK reference MUST list every public name, as the Python SDK reference does.
- **FR-022**: The install page MUST say what a Scala author needs, and the skills MUST describe the
  Scala SDK as they describe the Python one.

### Key Entities

- **streamlet**: one stage of a pipeline; here, one declared in Scala as a class.
- **descriptor**: the file the SDK writes, saying what the streamlet is.
- **SDK**: the library a streamlet is written against, which writes its descriptor, serves it to
  the sidecar and ships its test harness.
- **harness**: the SDK's in-memory stand-in for Kafka and the sidecar, batching and partitioning
  records as the sidecar does.
- **conformance suite**: the repository's suite that drives a served streamlet through every
  conversation the sidecar can have with it.
- **descriptor fixtures**: the six declared streamlets whose descriptor bytes every SDK must
  reproduce.
- **artifact**: the SDK as published to Maven Central, `com.thinkmorestupidless:ankka-flow-sdk_3`,
  at a release's version.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: The six descriptor fixtures are reproduced byte for byte by the Scala SDK, and the
  conformance suite passes against the Scala reference streamlet with every case that applies to a
  process, zero skipped beyond those the suite skips for every process.
- **SC-002**: The Scala cart router's committed descriptor is byte-identical to the Python cart
  router's, and `flow verify` of the shared blueprint gives the same output against either.
- **SC-003**: The Scala sample's two tests and descriptor check pass in CI from a clean checkout,
  and its laptop walkthrough's produce-and-verify round trip succeeds with the Scala router.
- **SC-004**: A tagged release puts the SDK on Maven Central at the release's version with no
  manual step; a fresh Scala project depending on it by version compiles the cart router and
  passes its tests within five minutes of the artifact being resolvable.
- **SC-005**: Every page with a Python code sample has a Scala one included from tested code, the
  docs build is clean, and a Scala reader following the first-streamlet tutorial reaches a running
  streamlet without reading a line of Python.
- **SC-006**: The Python SDK, its sample, the sidecar, the operator and `flow` pass the same suites
  they pass today; the protocol directory is unchanged.

## Assumptions

- **The protocol is sufficient.** The Python SDK proves the protocol hosts an SDK with nothing
  else; the Scala SDK is written to the same protocol, in the same shape, with no change to
  `protocol/` or the sidecar.
- **Graph deltas at parity.** The protocol carries a graph-delta outlet and the Python SDK offers
  it; the Scala SDK offers the same outlet with the same validation, and nothing beyond it.
- **The SDK's shape follows the Python one.** Names and structure mirror the Python SDK where
  Scala allows — a `Streamlet` base, port and parameter declarations as members, `process` over a
  batch, `serve`, a harness — so a reader moves between the two without relearning; the contributing
  page says nothing else about an SDK's shape is prescribed.
- **Scala 3.3 LTS and Java 21.** The SDK is published for the Scala 3 LTS line so that any Scala
  3.3 or later project can depend on it, while the rest of the repository stays on its own Scala
  version; its dependencies are the protocol's gRPC and protobuf libraries, as the sidecar's are.
- **Maven Central is ankka's.** ankka publishes its artifacts to Maven Central under its
  organisation with signing and portal credentials held as repository secrets; this feature
  publishes the same way and needs the same four secrets set on ankka-flow by the maintainer.
- **The sample is a module of the repository's build.** It depends on the SDK module directly, so
  CI proves the two together at every commit with the one build the repository already runs; a
  reader's own project depends on the published artifact by version, and the sample's README shows
  that dependency line.
- **The same blueprint, two images.** The Scala sample runs the Python sample's blueprint
  unchanged, because its descriptor is the same; only the image named at deploy time differs.
- **The release's gates are the ones in place.** A pre-release tag publishes only the release page,
  the binaries and the formula; the SDK and the image are gated as the Python SDK and the other
  images are.

## Out of Scope

- Hosting a streamlet inside the sidecar's own JVM; a Scala streamlet is a process like any other.
- A Java API, or publishing for Scala 2.
- An sbt plugin, a giter8 template or any scaffolding of a streamlet project.
- Any change to the protocol, the sidecar, the operator, `flow` or the Python SDK.
- A graph-delta outlet beyond what the protocol already carries, and any new built-in stage.
- Scala versions of the checkout-feed and checkout-graph samples; the pages that show them say
  the Scala side is the same SDK and link the Scala guide.
