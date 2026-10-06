# Tasks: A Scala SDK

**Input**: Design documents from `/specs/005-scala-sdk/`

**Prerequisites**: [plan.md](./plan.md), [spec.md](./spec.md), [research.md](./research.md),
[data-model.md](./data-model.md), [contracts/](./contracts/), [quickstart.md](./quickstart.md);
the scenarios are in `features/sdk/*.feature`.

**Tests**: included, and first where a test can be written before the thing it holds. The two
proofs — the descriptor fixtures and the conformance suite — already exist; the work is to make
the SDK pass them and then to hold the sample, the release and the pages to the SDK.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: can run in parallel — different files, no dependency on an incomplete task
- **[Story]**: US1 (write and test a streamlet in Scala), US2 (the descriptor and the
  conversation are the protocol's), US3 (the Scala router beside the Python one), US4 (published
  with every release), US5 (every page with a Python side has a Scala side)

Paths are repository-relative. `SDK` = `sdks/scala/src/main/scala/com/thinkmorestupidless/ankka/flow/sdk`,
`SDKT` = `sdks/scala/src/test/scala/com/thinkmorestupidless/ankka/flow/sdk`, `SAMPLE` =
`samples/cart-router-scala`, `PY` = `sdks/python/src/ankka_flow`, `CONF` =
`sidecar/src/test/scala/com/thinkmorestupidless/ankka/flow/sidecar/conformance`. "R*n*" is a
section of `research.md`; "V*n*" an item of its *Verify first* list; the API is
`contracts/scala-sdk-api.md`.

---

## Phase 1: Setup — the LTS compiler under the published modules (R2)

- [X] T001 In `project/Dependencies.scala`, add `V.scalaLts` (the latest 3.3 release on Maven Central; 3.3.8 when planned), `slf4jApi` and `slf4jSimple` (`V.slf4j` exists); in `build.sbt`, a `ltsSettings` value: `scalaVersion := V.scalaLts`, `scalacOptions` without `-source:3.7` and with `-java-output-version:21` (the Java floor is deliberate, not the building JDK's), with a comment saying why the published modules are on the LTS line.
- [X] T002 Apply `ltsSettings` to `protocol` in `build.sbt`; `sbt protocol/test blueprint/test sidecar/compile cli/compile operator/compile` warning-free on both compilers (V1). Record in `specs/005-scala-sdk/research.md` the exact LTS version and anything the 3.3 compiler refused.
- [X] T003 In `build.sbt`, add the module `sdk` in `sdks/scala`: `ltsSettings`, `commonSettings`, `dependsOn(protocol)`, `name := "ankka-flow-sdk"`, `BuildInfoPlugin` with `version` and `protocolVersion` (`ProtocolVersion.Current.toString`, package `com.thinkmorestupidless.ankka.flow.sdk`), `libraryDependencies ++= Seq(slf4jApi)`; add it to the root aggregate.

**Checkpoint**: `sbt compile` passes with `protocol` and `sdk` on the LTS compiler and nothing else changed.

---

## Phase 2: Foundational — the SDK's core, held by the fixtures (R3, R4)

**Purpose**: every story needs a streamlet that can be declared and whose descriptor is the
protocol's. The fixtures suite is written first and fails until the declarations exist.

**⚠️ CRITICAL**: every user story depends on this phase.

- [X] T004 Write `SDKT/DescriptorFixturesSuite.scala`: the six declarations of `protocol/fixtures/declarations/*.md` as `Streamlet` subclasses in the SDK's API (`minimal`, `cart-router`, `every-type`, `many-ports`, `sink`, `conformance` — the last shared with T013), one test per fixture asserting `Descriptor.write(streamlet, sdk = SdkInfo("fixture", "0.0.0"))` equals the bytes of `protocol/fixtures/descriptors/<name>.json` under `flow.repo.root`; a test per refused declaration of `features/sdk/scala-protocol.feature`'s outline (two ports with one name, an empty schema name, a default of another type, no inlet and no outlet) asserting `IllegalArgumentException` at construction with the rule's message. Red.
- [X] T005 [P] Write `SDK/Records.scala`: `Record`, `Batch` (an `Iterable[Record]`), `Emit`, as the API says; `Record` equality by value bytes, key and headers for tests.
- [X] T006 [P] Write `SDK/Parameters.scala`: `Parameter[T]` with `key`, `type` (the protocol's `ConfigType`), `default`, `description`, `render` (as `Start.config_json` carries a value: the Python `parameters.py` rules, including HOCON durations and memory sizes through `DescriptorValidation.parseValue` where the protocol already parses them) and `parse`; `Config` with `apply[T](p)` and `get(key)`; the six factories under a `parameter` object the base class exposes.
- [X] T007 [P] Write `SDK/Ports.scala`: `Port`, `JsonInlet`, `Outlet`, `JsonOutlet` with the three `emit` shapes (same key, headers and value by default), fingerprints by `Fingerprint.fingerprint`; `GraphDeltaOutlet` declared here with its `emit` delegating to T017's `graph`.
- [X] T008 Write `SDK/Streamlet.scala`: `abstract class Streamlet(name, description)` with the protected `inlet`, `outlet`, `graphDeltaOutlet` and `parameter` factories registering into ordered buffers; duplicate port names and parameter keys refused at registration; `inlets`, `outlets`, `parameters`, `config`, `configure(values: Map[String, String])`, abstract `process`; `Streamlet.runBatch(streamlet, batch)` checking each emit names a declared outlet (`UndeclaredOutlet`); `validate()` called by `Descriptor` and `Serve` — `DescriptorValidation.validate(spec)` — throwing `IllegalArgumentException` with every problem joined, so a refused declaration never reaches a file or a port.
- [X] T009 Write `SDK/Descriptor.scala`: `spec(streamlet, sdk = SdkInfo("ankka-flow-scala", BuildInfo.version))` with `ProtocolVersion.Current`, ports sorted by name, parameters by key; `write` = `DescriptorJson.write`; `validate`; `main(args)`: `<class> <path> [--check]` constructing the streamlet by its no-argument constructor, writing the file (creating its directory) or, with `--check`, exiting 1 with the file's name when it differs or is missing. T004 green.
- [X] T010 Write `SDKT/DescriptorMainSuite.scala`: `main` writes a file equal to `write`; `--check` exits 0 on the same bytes and 1 on a changed file (capture `System.exit` through a `SecurityManager`-free seam: `Descriptor.run(args): Int` with `main` calling `sys.exit`); a class that is not a `Streamlet`, or that throws at construction, is reported by name with exit 2.
- [X] T011 Commit Phase 2: the fixtures reproduce byte for byte; `sbt sdk/test` green.

**Checkpoint**: SC-001's first half holds; nothing is served yet.

---

## Phase 3: User Story 2 — The descriptor and the conversation are the protocol's (Priority: P1)

**Goal**: `Serve` passes the conformance suite in process and on a port; the sidecar's
in-process target is the Scala SDK (R5, R6).

**Independent Test**: `sbt 'sidecar/testOnly *ConformanceSuite'` green with the SDK as its target;
the same with `-Dflow.conformance.target` against `ConformanceMain`.

- [X] T012 [US2] Write `SDK/Serve.scala`: `Serve.start(streamlet, port = 0): Server` and `Serve.run(streamlet)` binding `127.0.0.1` and `FLOW_PROCESS_PORT` (default 9010) with `NettyServerBuilder` (grpc-netty-shaded, from `protocol`), max message 16 MiB; `DiscoveryGrpc` answering `Discover` with `Descriptor.spec` and logging `ReportError`'s problems; `StreamletGrpc.run` as a `Conversation`: `Start` applies `config_json` through `configure` (the JSON object's values rendered as strings, as `PY/server.py` does); each `Batch` goes to a worker pool through a per-`(inlet, partition)` serial queue; emits are sent as `runBatch` produces them, then one `Ack`, or one `Fail` with the exception's message (or `UndeclaredOutlet`'s); a new `Run` ends the previous conversation (its batches finish, send nothing); `Stop` waits for in-flight batches and completes; an emit with no key leaves `Record.key` unset. `Serve.main(args)` takes the class name. Mirror `PY/server.py` structure for structure.
- [X] T013 [US2] Write `SDK/conformance/Conformance.scala` — in the SDK's main, as the Python SDK ships `_conformance.py`, so the sidecar's suite can serve it in process — the `conformance` declaration behaving by key exactly as `protocol/fixtures/declarations/conformance.md` says (echo, fan, skip, fail, late with a 300 ms sleep, rogue-outlet, multiply with header `n=<i>` `factor` times, unkeyed, header-echo reversing headers, default echo), and `SDK/conformance/ConformanceMain.scala` (`<port>`: serves it and blocks). T004's `conformance` fixture declaration becomes this class.
- [X] T014 [US2] In `build.sbt`, `sidecar.dependsOn(protocol, sdk % "test->compile")`; in `CONF/ConformanceTarget.scala`, `InProcess` starts `Serve.start(new Conformance, port = 0)` and names itself "in-process Scala SDK"; the double stays for `withDouble`. `sbt 'sidecar/testOnly *ConformanceSuite'` green; then `ConformanceMain 9010` and the suite with `-Dflow.conformance.target=127.0.0.1:9010` green with the `violation.*`/`version.*` cases skipped (V2). Add the alias `sdkConformance` to `build.sbt`.
- [X] T015 [US2] Write `SDKT/ServeSuite.scala` for what the conformance suite does not single out: discovery answers the spec; a refused declaration fails `Serve.start` before binding; two partitions' batches run concurrently while one partition's run in order (a latch); a second `Run` voids the first; `Stop` completes after in-flight batches; `FLOW_PROCESS_PORT` unset binds 9010 (through `Serve.port()`); the server binds loopback only (`Server.address`).
- [X] T016 [US2] Update `CLAUDE.md`'s module-direction rule (`sdk → protocol`; `sidecar → sdk` in tests; the sample → `sdk`) and its commands block (`sbt sdk/test`, `sbt sdkConformance`, the `ConformanceMain` line); commit Phase 3.

**Checkpoint**: SC-001 holds in full: fixtures byte for byte, conformance green both ways.

---

## Phase 4: User Story 1 — Write and test a streamlet in Scala (Priority: P1)

**Goal**: the harness, name for name with the Python testkit, and the graph module at parity
(R7).

**Independent Test**: `SDKT/HarnessSuite.scala` green, one test per scenario of
`features/sdk/scala-streamlet.feature`.

- [ ] T017 [P] [US1] Write `SDK/graph.scala`: the `Delta` model and validation of `PY/graph.py`, name for name (element kinds, keys, the refused shapes of `protocol/fixtures/graph-deltas/refused.json`), and `GraphDeltaOutlet.emit(delta, from)` keying by the element; `SDKT/GraphSuite.scala` runs `protocol/fixtures/graph-deltas/{keys,deltas,refused}.json` as the Python suite does.
- [ ] T018 [US1] Write `SDK/testkit/Harness.scala`: `Harness(streamlet, config)`, `inlet(name).put(value, key, headers)`, `outlet(name).records`, `run(partitions, maxRecords)` with offsets numbered per `(inlet, partition)` across runs, batches in partition order, `runBatch` through `Streamlet.runBatch`, `failures`, `skipped`, `batches`; `Harness.hashPartitioner(n)` = CRC32 of the key modulo `n`, `None` → 0; `singlePartition`. Unknown inlet or outlet names throw with the declared ones listed.
- [ ] T019 [US1] Write `SDKT/HarnessSuite.scala`: the six scenarios of `features/sdk/scala-streamlet.feature` — the cart router routes as the Python one (reuse T004's `cart-router` declaration with a `process`), partitions and batch sizes, a failing batch leaves no emit, an undeclared outlet fails the batch naming it, a parameter's configured value and its default, bytes unchanged and nothing held across two runs.
- [ ] T020 [US1] Commit Phase 4; `sbt sdk/test` green.

**Checkpoint**: a streamlet author has everything but a sample to copy.

---

## Phase 5: User Story 3 — The Scala router beside the Python one (Priority: P2)

**Goal**: `samples/cart-router-scala` as a module: the router, its tests, its descriptor, its
image, running the Python sample's blueprint and compose file (R8).

**Independent Test**: quickstart tier 3.

- [ ] T021 [US3] In `build.sbt`, add `cartRouterScala` in `samples/cart-router-scala`: `ltsSettings`, `commonSettings`, `dependsOn(sdk)`, `publish / skip := true`, `JavaAppPackaging`, `DockerPlugin`, `dockerSettings`, `Docker / packageName := "sample-cart-router-scala"`, `Compile / mainClass := Some("cart.Main")`, `libraryDependencies += slf4jSimple`, `noDocs`; input tasks `descriptor` and `descriptorCheck` running `Descriptor.main` with `cart.CartRouter` and `flow/descriptor.json`; in the root aggregate.
- [ ] T022 [P] [US3] Write `SAMPLE/src/main/scala/cart/CartRouter.scala` between `// docs:start router` and `// docs:end router`: the Python router's declaration and `process` (`json` parsing through the protocol's `Json`), and `SAMPLE/src/main/scala/cart/Main.scala` (`Serve.run(new CartRouter)`) between `main` markers.
- [ ] T023 [P] [US3] Write `SAMPLE/src/test/scala/cart/CartRouterSuite.scala`: `routes-by-total` and `ordering` between the same marker names as `samples/cart-router/tests/test_router.py`, transliterated assertion for assertion, and the committed-descriptor test: `flow/descriptor.json`'s `streamlet` equals `protocol/fixtures/descriptors/cart-router.json`'s and its `sdk.name` is `ankka-flow-scala`.
- [ ] T024 [US3] Run `sbt cartRouterScala/descriptor` and commit `SAMPLE/flow/descriptor.json`; `sbt cartRouterScala/test cartRouterScala/descriptorCheck` green; `diff <(jq .streamlet SAMPLE/flow/descriptor.json) <(jq .streamlet samples/cart-router/flow/descriptor.json)` empty; `flow verify samples/cart-router/blueprint.conf --descriptors SAMPLE/flow` gives the Python descriptors' output (SC-002).
- [ ] T025 [US3] `sbt sidecar/docker:publishLocal cartRouterScala/docker:publishLocal`; the laptop loop with the Python sample's compose: `docker compose up -d` in `samples/cart-router`, `sbt cartRouterScala/run`, `uv run python produce.py && uv run python verify.py` there (V4); then the cluster half of FR-015: `just up`, `kind load docker-image --name ankka sample-cart-router-scala:latest`, `flow generate samples/cart-router/blueprint.conf --descriptors SAMPLE/flow --conf samples/cart-router/k8s/in-cluster.conf --image router=sample-cart-router-scala:latest -n shop | kubectl apply -f -`, and the round trip as the Python sample's README runs it. Record in `research.md` that the sidecar accepted the Scala process against the Python sample's deployed descriptor, the image's size, and the cluster run.
- [ ] T026 [US3] Write `SAMPLE/README.md` on `samples/checkout-feed/README.md`'s pattern: the dependency line for a reader's project, the files table, the commands, the laptop loop with the Python sample's compose, the kind deploy with `flow generate samples/cart-router/blueprint.conf --descriptors samples/cart-router-scala/flow --image router=sample-cart-router-scala:latest`.
- [ ] T027 [US3] In `README.md`'s "A streamlet" section, show the Scala router first and the Python one after it (GitHub Markdown has no tabs), each with its include marker (`SAMPLE/src/main/scala/cart/CartRouter.scala#router`, then the Python one) and one sentence saying the two declare the same streamlet; `just readme-sync`; the check passes.
- [ ] T028 [US3] In `.github/workflows/ci.yml`, add `sdks/scala/**` and `samples/cart-router-scala/**` to the `build` filter and, after `sbt test` in `build`, `sbt cartRouterScala/descriptorCheck cartRouterScala/docker:publishLocal`; in `Justfile`, `sdk-scala` (`sbt sdk/test sdkConformance cartRouterScala/test cartRouterScala/descriptorCheck`) and `images` now notes four images. Commit Phase 5.

**Checkpoint**: SC-002 and SC-003 hold locally; CI runs the sample.

---

## Phase 6: User Story 4 — Published with every release (Priority: P2)

**Goal**: `sbt ci-release` publishes `ankka-flow-sdk_3` and `ankka-flow-protocol_3` from a
release job gated on a release tag; a pre-release publishes nothing (R9).

**Independent Test**: the local rehearsal resolves from a fresh project; a pre-release tag skips
the job; the first release tag puts the artifact on Central.

- [ ] T029 [US4] In `build.sbt`, add ankka's publish metadata (`ThisBuild / homepage`, `licenses`, `developers`, `publishTo` with the `-Dflow.release.local=<dir>` directory resolver, each with its comment); `publish / skip := true` on `blueprint` and `crd`; confirm `protocol` and `sdk` are the only publishing modules (`sbt 'show */publish/skip'`).
- [ ] T030 [US4] Write `sdks/scala/README.md` (what the module is, the dependency line, the commands, the conformance run) and the POM check: `sbt -Dflow.release.local=/tmp/m2 publishSigned` with a throwaway gpg key; `ls /tmp/m2/com/thinkmorestupidless/{ankka-flow-sdk_3,ankka-flow-protocol_3}/<version>/` shows jar, sources, javadoc, POM and signatures; the SDK's POM depends on the protocol at the same version; a fresh sbt project in the scratch directory with `resolvers += "local-release" at "file:///tmp/m2"` and the dependency line compiles `CartRouter` and runs its `routes-by-total` test on Scala 3.3 LTS (V3, SC-004's shape). Record the result in `research.md`.
- [ ] T031 [US4] In `.github/workflows/release.yml`, add `sdk-scala` (`needs: images`, `if: ${{ !contains(github.ref_name, '-') }}`): checkout with tags, Temurin 21, sbt; the clean-tree refusal; "already on Central?" by `https://repo1.maven.org/maven2/com/thinkmorestupidless/ankka-flow-sdk_3/$version/ankka-flow-sdk_3-$version.pom`; a step that fails naming the first unset of `PGP_SECRET`, `PGP_PASSPHRASE`, `SONATYPE_USERNAME`, `SONATYPE_PASSWORD`; `sbt ci-release` with `CI_SONATYPE_RELEASE: sonaBundle`; the portal upload step from ankka with `name=ankka-flow-$version`; and in `images`, `cartRouterScala/docker:publish` beside the sidecar and operator. Update the comment at the top of the file (a pre-release publishes neither the SDK nor the sample's image). `actionlint` clean.
- [ ] T031a [US4] In `.github/workflows/release.yml`, gate `homebrew` on a release tag (`if: ${{ !contains(github.ref_name, '-') }}` after its `needs`) and rewrite the comment at the top of the file: a pre-release gets the release page and the binaries, and nothing else, no formula included; in `CLAUDE.md`'s "The tap is shared" rule, say a pre-release touches no formula; add a line to `specs/004-native-cli/research.md`'s findings that the rc tags wrote the public formula and that feature 005 gates the job (R13). `actionlint` clean.
- [ ] T032 [US4] Commit Phase 6; push the branch; tag `v0.5.0-rc.1` on it and observe `sdk-scala`, `images`, `sdk-python`, `marketplace` and `homebrew` skipped and `release-page` and the four `cli-native` legs green; delete the tag and its release; the tap is untouched. Record in `research.md`.

**Checkpoint**: everything but the public upload is proven; the upload runs on the first release tag.

---

## Phase 7: User Story 5 — Every page with a Python side has a Scala side (Priority: P3)

**Goal**: Scala tabs on the shared pages, a Scala guide, a Scala SDK reference, the install page,
the skills (R11).

**Independent Test**: `just docs` clean; every page with `languages: [python]` now has `scala`
too, except the three that say why.

- [ ] T033 [P] [US5] In `mkdocs.yml`, `facets.languages: [scala, python]`; add `build/scala-streamlet.md` and `reference/scala-sdk.md` to `nav` beside their Python siblings; in `docs/contributing/documentation.md`, the `languages` sentence names both.
- [ ] T034 [P] [US5] Write `docs/build/scala-streamlet.md` on `python-streamlet.md`'s headings — start a project (the dependency line, Scala 3.3 LTS, Java 21), declare the streamlet, process a batch, concurrency, serve it, write the descriptor (`sbt "runMain ...Descriptor cart.CartRouter flow/descriptor.json"`), next steps — with the router, main and tests included from `SAMPLE`.
- [ ] T035 [P] [US5] Write `docs/reference/scala-sdk.md` on `python-sdk.md`'s headings from `contracts/scala-sdk-api.md`: names, `Streamlet`, ports, graph deltas, parameters, records, `Serve`, `testkit`, commands (`Descriptor`, `ConformanceMain`), the floors (Scala 3.3 LTS, Java 21) and the coordinates.
- [ ] T036 [US5] Add `/// tab | Scala` and `/// tab | Python` blocks, Scala first, to `docs/get-started/first-streamlet.md` (the streamlet, the entry point, the test command, the run command), `docs/build/testing.md` (both tests, the descriptor check), `docs/build/images.md` (the Scala image from the build beside the Python Dockerfile), `docs/concepts/contracts.md` (the port declaration), each Scala block included from `SAMPLE` or `SDKT`; `languages: [scala, python]` on each (V5).
- [ ] T037 [P] [US5] In `docs/build/ankka-topics.md`, `docs/build/graph-sink.md` and `docs/build/graph-from-ankka.md`, one sentence that the Scala SDK offers the same ports (`GraphDeltaOutlet` by name on the graph pages), linking the Scala guide; in `docs/get-started/install.md`, what a Scala author needs (a JDK 21, sbt, the dependency line); in `docs/contributing/language-sdks.md`, that the copy rule is for an SDK outside this build and the Scala SDK depends on the protocol module.
- [ ] T038 [US5] Write `tools/docs/skill/ankka-flow-scala/SKILL.md` on `ankka-flow-python/SKILL.md`'s shape (rules, mistakes, pages: the Scala guide, testing, images, the Scala reference, descriptor, sidecar, contracts, delivery, first-streamlet, graph-deltas; description ≤ 1024 chars); update `ankka-flow/SKILL.md` and `ankka-flow-python/SKILL.md` to name both SDKs; `just docs-sync && just docs` clean; the rendered plugin committed.
- [ ] T039 [US5] Commit Phase 7: `docs/`, `mkdocs.yml`, `tools/docs/skill/`, `marketplace/plugins/ankka-flow/skills/`.

**Checkpoint**: SC-005 holds: a Scala reader follows the tutorial without reading Python.

---

## Phase 8: Polish

- [ ] T040 Run the whole build from the branch (`caffeinate -i sbt scalafmtCheckAll scalafmtSbtCheck test mutationCheck`, `sbt cartRouterScala/descriptorCheck`, the Python SDK's checks, `just features`, `just docs`, `just readme-sync` with no diff) and tick the reviewer's checklist in `specs/005-scala-sdk/quickstart.md`.
- [ ] T041 Write "Found during implementation" in `specs/005-scala-sdk/research.md` (each V-item's answer) and the pull request description: what a Scala author does now, what the release publishes, the two Scala versions, the secrets, and that nothing changed for Python, the sidecar or `flow`.

---

## Dependencies & Execution Order

- **Phase 1 → 2**: the LTS compiler under `protocol` (T002) is the first thing that can fail in a way that needs design; the SDK module (T003) needs it.
- **Phase 2**: T004 first (red), T005–T007 in parallel, then T008, T009, T010.
- **Phase 3 (US2)** needs Phase 2; T012 before T013–T015; T014 is the one change to `sidecar`.
- **Phase 4 (US1)** needs Phase 2 only; T017 can go beside Phase 3; T018 before T019.
- **Phase 5 (US3)** needs Phases 3 and 4 (the sample serves and is tested); T022 and T023 in parallel after T021.
- **Phase 6 (US4)** needs Phase 5 (the sample's image is published); T031a before T032; T032 needs the branch pushed and nothing outside the repository (a pre-release publishes nothing and touches no formula).
- **Phase 7 (US5)** needs Phase 5 (the pages include the sample); T033–T035 and T037 in parallel; T036 then T038.
- **Phase 8** after everything.

### Parallel opportunities

- T005, T006, T007; T017 beside T012–T016; T022, T023; T033, T034, T035, T037.

## Implementation Strategy

**MVP is US2 with Phase 2** (Phases 1–3): a Scala SDK that reproduces the six fixtures and passes
the conformance suite as the sidecar's in-process target. It gives a reader nothing yet, but it is
the part whose failure needs design — the compiler change and the server — and it is green before
any API is polished, any sample written or any page changed.

Then US1's harness, US3's sample, US4's publishing, US5's pages, polish. One person: in the order
the phases are numbered.

### Notes

- A name in the Scala API is the Python name, in Scala's case; a deliberate difference is recorded
  in `research.md` with its reason, so the two references stay one API.
- Stop at a checkpoint that is not green. A conformance case that fails is a defect of `Serve`,
  never a case to skip.
- The first public upload to Maven Central happens on the first release tag after merge, not on
  this branch; the rehearsal (T030) and the rc tag (T032) are the proof short of it.
