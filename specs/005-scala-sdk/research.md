# Research: A Scala SDK

Each decision below settles something the spec leaves to the plan. The shape of the SDK follows the
Python one wherever Scala allows, so the two references read as one API in two languages.

## R1. The SDK's protocol is the `protocol` module, not a copy

**What is there**: `protocol/` is already a Scala module: the `.proto` files generate ScalaPB
messages and grpc-java stubs, and beside them sit `DescriptorJson` (the canonical JSON writer and
reader), `DescriptorValidation` (every rule of DESCRIPTOR.md, and `compare` for deployed versus
discovered), `Fingerprint`, `ProtocolVersion` and `Builtins`. The sidecar, the blueprint checker and
the CLI all use it. The Python SDK copies `protocol/` because it cannot depend on a Scala module; a
Scala SDK in the same build can.

**Decision**: `sdk` depends on `protocol`. The SDK writes descriptors with `DescriptorJson.write`,
validates with `DescriptorValidation.validate`, fingerprints with `Fingerprint`, and reports
`ProtocolVersion.Current`. `protocol` is published to Maven Central as `ankka-flow-protocol_3` at
the same version, and the SDK's POM depends on it, so a reader's project resolves both. No file
under `protocol/` is copied anywhere; a change to the protocol is a change to the SDK by
construction.

**Consequence for the spec**: FR-010 and the scenario "the Scala SDK's copy of the protocol is the
repository's" assumed a copy. Both are rewritten: the SDK holds no copy; its protocol code is the
repository's module, published beside it at the same version. (`features/sdk/scala-protocol.feature`,
`spec.md` FR-010.)

**Rationale**: a byte-for-byte copy of a Scala module inside the same build would be the same
generated classes twice, and the validation and canonical writer would have to be duplicated or
imported from the original anyway. The contributing page's "copy the protocol" rule exists for
SDKs outside this build; it is restated to say so.

**Alternatives considered**: a copy under `sdks/scala/protocol` with its own ScalaPB generation —
rejected above. Shading the protocol into the SDK jar — hides a dependency the sidecar shares and
would put two copies of `ankka.flow.v1` on a classpath that has both.

## R2. Scala 3.3 LTS for what is published; 3.9 for the rest

**Decision** (clarified in the spec): `protocol`, `sdk` and the Scala sample set
`scalaVersion := V.scalaLts` (3.3.8, the latest 3.3 release on Maven Central today; verify at
implementation). Every other module stays on `V.scala` (3.9.0). A library compiled by 3.3 is read
by a 3.9 compiler, so `blueprint`, `sidecar` and `cli` depend on `protocol` as before; the reverse
would not hold, which is why the published modules are the LTS ones. `-source:3.7` in
`commonSettings` is not accepted by a 3.3 compiler, so those three modules override
`scalacOptions` to drop it. `sbt ci-release` publishes each module at its own `scalaVersion`; no
`crossScalaVersions` is set, so `+publishSigned` publishes each once.

**Verify first**: that ScalaPB 0.11.11's generated code and `-Wunused:all` are clean on 3.3.8; that
munit 1.3.6 resolves for 3.3 (it does, `_3`); that sbt-dynver's version is the tag's on the LTS
modules as on the rest.

**Alternatives considered**: everything on 3.3 — moves the sidecar, operator and CLI off the
compiler they were written against for no reader's benefit. Cross-building `protocol` and `sdk`
for both — doubles the publish for artifacts whose 3.3 build already serves 3.9 consumers.

## R3. The SDK's shape: a class, members as declarations, `process` over a batch

**Decision**: package `com.thinkmorestupidless.ankka.flow.sdk`, artifact `ankka-flow-sdk`. A
streamlet is a subclass of `abstract class Streamlet(name: String, description: String = "")`.
Ports and parameters are `val`s built with the base class's protected factories — `inlet("in",
schemaName = "cart-events.v1")`, `outlet("valid", schemaName = ...)`, `graphDeltaOutlet("deltas")`,
`parameter.integer("review-threshold", default = 100, description = ...)` and the five other
types — each of which registers what it returns, so the declaration is the construction of the
object and nothing is discovered by scanning. A port name or parameter key used twice throws
`IllegalArgumentException` at construction; a name the descriptor rules refuse throws at
construction too, with `DescriptorValidation`'s message. `def process(batch: Batch):
Iterable[Emit]` is abstract. `config: Config` holds the parameter values, typed by the parameter:
`config(threshold): Int`. `outlet.emit(record)` builds an `Emit` keeping key, headers and value;
`emit(record, value = ..., key = ..., headers = ...)` replaces what is named; `emit(value, key,
headers)` builds a fresh record. `Record(value: Array[Byte], key: Option[Array[Byte]], headers:
Seq[(String, Array[Byte])], offset: Long, timestampMs: Long)`; `Batch(inlet, partition, records)`;
`Emit(outlet: String, record: Record)`. Contracts: `contracts/scala-sdk-api.md`.

**Rationale**: Python's declaration is class attributes found at class definition; Scala's nearest
equivalent that needs no macros, no reflection and no annotation processing is construction-time
registration through factories the base class owns. Every name is Python's, in Scala's case.

**Alternatives considered**: a `Streamlet` trait with abstract `inlets`/`outlets` sequences the
author lists by hand — the author can forget a port; registration cannot. Annotations with a
macro — a macro is a second thing to learn and to maintain across compiler versions.

## R4. The descriptor is written by a main, run from the build

**Decision**: `com.thinkmorestupidless.ankka.flow.sdk.Descriptor` is a `main` taking the
streamlet's class name and a path, with `--check`: it constructs the streamlet (a no-argument
constructor is the rule, as in Python), builds the discovery `Spec` with
`SdkInfo("ankka-flow-scala", BuildInfo.version)`, validates it, and writes `DescriptorJson.write`
to the path (or compares, exiting 1 on a difference, naming the file). The Scala sample's module
defines two input tasks, `descriptor` and `descriptorCheck`, that run it with the sample's class
and `flow/descriptor.json`; a reader's project runs `sbt "runMain ...sdk.Descriptor
cart.CartRouter flow/descriptor.json"`, which the Scala guide shows. The sample's munit suite
also asserts that the committed descriptor's `streamlet` equals the fixture's, as the Python one
does.

**Found while planning**: a descriptor carries an `sdk` block naming the SDK and its version, so
the Scala router's committed descriptor is *not* byte-identical to the Python router's — its
`streamlet` part is, and the sidecar's `DescriptorValidation.compare` and `flow verify` read only
that part. The spec's FR-012 and SC-002 and the scenario "the Scala cart router and the Python one
have the same descriptor" are reworded to say so.

**Alternatives considered**: an sbt plugin with a `flowDescriptor` task — out of scope by the spec,
and a reader's build can call a main without one. Writing the descriptor at compile time with a
macro — the descriptor needs the constructed object (defaults rendered, fingerprints computed),
not the source.

## R5. The server: grpc-java over the generated stubs, one conversation at a time

**Decision**: `Serve.run(streamlet)` (and a `Serve` main for a project whose entry point is the
SDK's) binds `127.0.0.1:$FLOW_PROCESS_PORT` (default 9010) with grpc-netty-shaded, which
`protocol` already depends on, and registers the two generated services. `Discovery.Discover`
answers the `Spec`; `ReportError` logs each problem. `Streamlet.Run` is one `Conversation`: it
waits for `Start`, applies `config_json` through `Config`, and for each `Batch` submits the batch to
a worker pool keyed so that two batches of one `(inlet, partition)` never run at once while
different partitions do; emits are sent as the iterator produces them, then exactly one `Ack`, or
one `Fail` with the exception's message when `process` throws or emits to an undeclared outlet. A
new `Run` ends the previous conversation: its in-flight batches finish but send nothing. `Stop`
waits for in-flight batches, then completes the stream. Message size limit 16 MiB, as Python and
the sidecar. Logging through slf4j, with no binding in the SDK itself; the sample binds
`slf4j-simple`.

**Rationale**: the structure is the Python server's, which the conformance suite already holds;
grpc-java is what the sidecar and `protocol` use, so nothing new enters the build.

**Alternatives considered**: pekko-grpc — not in this build by rule. A Pekko stream per
conversation — Pekko is the sidecar's, and an SDK that pulls Pekko into every streamlet process is
a heavy SDK for a function over a batch.

## R6. The conformance suite's in-process target becomes the Scala SDK

**What is there**: `ConformanceTarget.InProcess` serves the scriptable `ProcessDouble` as "the
Scala reference". The double exists to produce misbehaviour; it is the reference only because no
SDK existed.

**Decision**: `sidecar` depends on `sdk` in test scope (`sdk % "test->compile"`). The in-process
target serves the SDK's `Conformance` reference streamlet — the `conformance` declaration,
behaving by each record's key as `protocol/fixtures/declarations/conformance.md` says — on an
ephemeral port through `Serve`. So `sbt sidecar/test` runs the twenty conversation cases against
the real Scala SDK on every build, which is FR-009's proof, and `-Dflow.conformance.target` still
points the suite at any process. The `violation.*` and `version.*` cases keep the double
(`withDouble`), as they must. FR-009's "one command" is the alias `sdkConformance` =
`sidecar/testOnly *ConformanceSuite`; and `sdk/Test/runMain ...sdk.conformance.ConformanceMain
9010` serves the reference on a port for the suite's remote mode, which is what the Python SDK's
`uv run conformance` does from the other side.

**Module direction**: `sidecar → protocol` gains `sidecar → sdk (test)`. `sdk → protocol`. Nothing
depends on `sidecar`; `sdk` depends on nothing but `protocol`. The rules table is updated.

**Alternatives considered**: a copy of the conformance suite in `sdk`'s tests — two suites to keep
equal. Running the suite only in remote mode against a spawned SDK process — a second JVM per test
run for what an in-process server does in milliseconds.

## R7. The harness, name for name

**Decision**: `com.thinkmorestupidless.ankka.flow.sdk.testkit.Harness(streamlet, config:
Map[String, String] = Map.empty)`; `inlet(name).put(value, key, headers)`; `run(partitions:
Option[Array[Byte]] => Int = Harness.singlePartition, maxRecords: Option[Int] = None)`;
`outlet(name).records: Vector[Record]`; `failures: Vector[Failure(batch, error)]`; `skipped:
Vector[Record]`; `batches`. `Harness.hashPartitioner(n)` is CRC32 of the key modulo `n`, as
Python's, so a test written against one SDK reads the same against the other. The rules are the
sidecar's: one batch per partition in offset order, emits recorded only when the batch succeeds, an
exception or an undeclared outlet fails the batch and discards its emits, a record with no emit
derived from it is skipped. `run` is what the server and the harness share: `Streamlet.runBatch`
checks every emit names a declared outlet.

## R8. The sample: a module, one blueprint, two images

**Decision** (clarified): `samples/cart-router-scala` is `cartRouterScala` in `build.sbt`,
`dependsOn(sdk)`, on the LTS Scala, `publish / skip`, with `JavaAppPackaging` and `DockerPlugin`
over the repository's `dockerSettings` (`eclipse-temurin:21-jre`), image `sample-cart-router-scala`.
Its code is `CartRouter` with the Python router's declaration, name for name; its tests are the
Python tests transliterated; its `flow/descriptor.json` is committed and checked. The blueprint is
the Python sample's, `samples/cart-router/blueprint.conf`: the `streamlet` part of both descriptors
is the same, so one blueprint deploys either image. The laptop walkthrough reuses the Python
sample's `docker-compose.yml` and `flow/streamlet.conf` unchanged — the sidecar compares the
deployed `streamlet` declaration with discovery and the Scala process answers with the same one —
with `sbt cartRouterScala/run` in place of `uv run python -m cart_router.main`, and the Python
`produce.py`/`verify.py` prove the round trip. The sample's README shows the published dependency
line for a reader's own project. The root's `sampleImage` task stays the Python image (the k3s
suite's); `docker:publishLocal` now also builds the Scala sample's image, and `just images` with it.

**Alternatives considered**: a standalone sbt build under `samples/` — clarified away. A copy of
the blueprint and compose files — two copies of files whose point is being the same.

## R9. Publishing: `sbt ci-release` as ankka does, gated on a release tag

**What is there**: `sbt-ci-release` is already in `project/plugins.sbt` (for dynver) and
`versionScheme` is set; `publishTo`, `homepage`, `licenses` and `developers` are not; `sidecar`,
`operator`, `cli` and the root skip publishing; `protocol`, `blueprint` and `crd` do not, by
omission.

**Decision**: `build.sbt` gains ankka's publish metadata (`ThisBuild / homepage`, `licenses`,
`developers`, and the `-Dflow.release.local=<dir>` local-directory `publishTo` for a rehearsal);
`blueprint`, `crd` and the sample set `publish / skip := true`; `protocol` and `sdk` publish. A new
release job `sdk-scala`, gated like `sdk-python` on a release tag, checks the clean tree, asks repo1
whether `ankka-flow-sdk_3/<version>` is already there (a re-run must not upload twice), runs `sbt
ci-release` with `CI_SONATYPE_RELEASE: sonaBundle` and uploads the bundle to the Central Portal
with `publishingType=AUTOMATIC` — the step copied from ankka, with the bundle named
`ankka-flow-<version>`. Secrets: `PGP_SECRET`, `PGP_PASSPHRASE`, `SONATYPE_USERNAME`,
`SONATYPE_PASSWORD`, set on ankka-flow by the maintainer; the job fails naming the first one unset.
The `com.thinkmorestupidless` namespace is already claimed on the portal by ankka.

**Verify first**: the local rehearsal — `sbt -Dflow.release.local=/tmp/m2 publishSigned` with a
throwaway key — produces `ankka-flow-sdk_3` and `ankka-flow-protocol_3` POMs with the dependency
between them and a fresh sbt project resolves them from that directory (SC-004's shape, offline).

## R10. CI

**Decision**: `sdk` and `cartRouterScala` are in the root aggregate, so `sbt test` in the `build`
job covers the SDK's suites (fixtures, harness, server, descriptor main), the sample's tests, and
the conformance suite through `sidecar/test`. `ci.yml`'s `build` filter gains `sdks/scala/**` and
`samples/cart-router-scala/**`; the `sdk-python` filter does not change (its job still diffs the
Python copy). A `descriptorCheck` step for the Scala sample runs in `build` after `sbt test`, and
the sample's image builds there too (`cartRouterScala/docker:publishLocal`), so a broken Dockerfile
is found at the pull request. The release's `images` job adds `cartRouterScala/docker:publish`.

## R11. Documentation: tabs on shared pages, a page per language where the content differs

**Decision**: `mkdocs.yml` already enables `pymdownx.blocks.tab`, which ankka's index uses. Pages
that show one code sample per language — the first-streamlet tutorial, testing, images, contracts —
show Scala and Python in `/// tab | Scala` and `/// tab | Python` blocks, each block's sample
included from its own tested source, Scala first (ankka's order) — and `languages: [scala, python]`
in their frontmatter, with `scala` added to the `languages` facet in `mkdocs.yml`. Pages whose
content is one language's get a sibling: `build/scala-streamlet.md` beside `python-streamlet.md`,
`reference/scala-sdk.md` beside `python-sdk.md`. `ankka-topics.md`, `graph-sink.md` and
`graph-from-ankka.md`, whose samples have no Scala twin, keep their Python and say in one sentence
that the Scala SDK offers the same ports, linking the Scala guide (the spec allows this). The
install page gains what a Scala author needs (a JDK 21 and sbt, and the dependency line). A fourth
skill, `ankka-flow-scala`, mirrors `ankka-flow-python`; `ankka-flow`'s rules say both languages.
The README's "A streamlet" section shows the router in Scala and Python, both included.

**Alternatives considered**: separate tutorial pages per language, as ankka's first-service pages —
the ankka-flow tutorial's steps are identical but for one code block and one run command, so tabs
keep one page.

## R13. A pre-release tag no longer writes the tap's formula

**What is there**: feature 004's `homebrew` job runs on every tag, so its rc tags wrote
`Formula/ankka-flow.rb` to the public tap and the formula was removed by hand after each. With
`ankka-flow` 0.4.0 released, an rc would replace the formula every Homebrew user installs with one
pointing at assets the rehearsal then deletes.

**Decision**: `homebrew` is gated on a release tag like `images`, `sdk-python` and `marketplace`;
`release-page` and `cli-native` still run on a pre-release, so a rehearsal still proves the
binaries. The tap path itself was proven by 004's three rc tags and needs no rehearsal. The
workflow's comment, `CLAUDE.md`'s rule and feature 004's research note say so; T032 relies on it.

**Alternatives considered**: writing an rc formula under another name — a second formula to keep
out of readers' way; cleaning the tap after each rc by hand — what 004 did, and the mistake this
feature would make first.

## R12. Not carried: the Python SDK's test-only break switch

The Python server reads `ANKKA_FLOW_BREAK` to misbehave on purpose, which proved that the
conformance suite catches a broken SDK (version one's SC-005). The suite's `violation.*` cases
now prove the same with the double, and the Scala SDK's in-process target makes any regression in
the SDK itself fail `sidecar/test` directly. No break switch in the Scala SDK.

## Verify first

1. **R2**: ScalaPB-generated sources and the hand-written `protocol` sources compile warning-free
   on Scala 3.3.8, and `sidecar` (3.9) still compiles against them.
2. **R6**: the conformance suite passes against a minimal `Serve` before the SDK's API is
   finished — the server is the risk, not the declarations.
3. **R9**: the local publish rehearsal resolves from a fresh project.
4. **R8**: the Python sample's compose file runs the Scala router unchanged (the sidecar accepts
   the discovery because the `streamlet` part is equal).
5. **R11**: `docs check` accepts `/// tab` blocks around included samples (an include comment
   inside a tab block must still be followed directly by its fence).

## Found during implementation

- **V1**: `protocol` compiles warning-free on Scala 3.3.8 with `-java-output-version:21`, ScalaPB
  0.11.11's generated sources included, and `blueprint`, `sidecar`, `operator` and `cli` (3.9.0)
  compile and pass against it unchanged.
- **The descriptor rules have no "a streamlet declares a port" rule.** The scenario outline's
  example "no inlet and no outlet" was not something the protocol refuses; it is replaced by "a
  port name with a capital letter", which it does. The SDK refuses exactly what
  `DescriptorValidation` refuses, nothing more.
- **Per-item refusals happen where the item is declared.** A port's name and schema, a
  parameter's key and default, and the streamlet's name are each checked by
  `DescriptorValidation` as the factory registers them, so a refused declaration throws at
  construction, before a descriptor is written or a port bound.
- **The server does not serialise a partition's batches.** As in the Python server, every batch
  goes to the worker pool: the sidecar keeps at most one in flight per partition, so the SDK
  needs no queue of its own.
- **V2**: the conformance suite passes against the Scala SDK in process (23 of 23, with the
  double serving the `violation.*` and `version.*` cases) and against `ConformanceMain` on a port
  (18 passed, 5 skipped) — the Python SDK's numbers.
- **The emit API is two overloads and `copy`.** `outlet.emit(record)` forwards the record;
  `outlet.emit(value, key, headers)` builds a new one; replacing part of a record is
  `outlet.emit(record.copy(value = ...))`. Python's keyword arguments become the case class's
  `copy`, so the offset is kept and the harness counts the record as not skipped.
- **A graph delta's source record is a named last argument**, `source = Some(record)`, where the
  Python SDK takes it first and positionally: Scala's default arguments come last, and an
  `Option` makes "built from nothing" explicit.
