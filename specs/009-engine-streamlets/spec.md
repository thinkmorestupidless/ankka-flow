# Feature Specification: Engine Streamlets — A Streamlet Is Run by the Engine It Is Written For

**Feature Branch**: `009-engine-streamlets`

**Created**: 2026-11-22

**Status**: Draft

**Input**: User description: "Rethink ankka-flow. Today a streamlet's logic runs in the streamlet's
own process, between Kafka topics. The original idea behind Cloudflow was that the logic is in the
streamlets and is executed by an engine: Spark, Akka Streams, whatever is supported. Bring that
back, as engine-specific streamlets: Flink where a stage needs windowing or state, Pekko Streams
where it needs neither and the developer wants something quick, with no windowing built into the
Pekko side; the developer chooses the engine per streamlet for the use case. It does not fit in
ankka's core and complements it: it runs between Kafka topics, the codecs between ankka events and
ankka-flow are already aligned, and it can clean incoming data and create aggregations that are
then persisted in ankka, connected directly over gRPC as well as through topics. ankka-flow is not
retired after all. Backwards compatibility with version one does not matter; keep this feature's
scope focused."

## Context

Version one runs every streamlet the same way: a process in any language, handed batches by a
sidecar that owns Kafka. ankka's feature 037 observed that such a streamlet and an ankka consumer
over a topic are the same machine, and gave ankka the few things its consumer lacked. That holds,
and it is why version one's shape is not the future of this project.

What it leaves without a home is computation over sets and time. ankka handles one message for one
key with durable state; 037 says an aggregation across messages belongs in an entity or a view,
which is right per key and has no answer for windows, records that arrive late, joins across
streams, state too large for a process, or data cleaned and reshaped at volume before it reaches a
project. Stream engines exist for exactly this, and none of it belongs in a platform's core.

So a streamlet becomes what it was in Cloudflow: logic written against one engine's own API and
run by that engine, under contracts, wiring and a deploy that are the same for every engine. Two
engines to begin with. **Flink**, where a stage needs time or state. **Pekko Streams**, where it
needs neither. No layer is built over the engines' logic, and nothing is added to Pekko Streams to
make it do what Flink does: needing a window is the signal to choose Flink.

Three things about ankka decide what this feature can do, all of which ankka already has:

- A project's declared topic may carry a **contract**: a name and a schema document, fingerprinted,
  which every side of the topic must state alike. Version one's contract is a schema name only.
- A system outside an installation is a **registered machine** of an organization. A project grants
  a machine one topic to read or publish, or one route or gRPC method to call, and the broker and
  the service enforce the grant. A pipeline can be such a machine with nothing new in ankka.
- The record a topic carries is JSON with CloudEvents headers, which ankka-flow's codecs already
  read and write.

Feature 008, a blueprint names an ankka topic, was written before 037 and paused with three open
questions. Two are answered by what ankka now has (the contract, the broker access); the third,
where the check against the project runs, is answered here. This feature takes 008's scope into
its fourth story and 008 is not built separately.

## Clarifications

### Session 2026-11-22

- Q: What becomes of version one's sidecar streamlets? → A: They go. Backwards compatibility does not matter: the sidecar, its protocol, the two SDKs written against it, the stage built into it and the samples are removed in this feature, and a descriptor must name an engine.
- Q: Who installs the Flink Kubernetes Operator? → A: Whoever installs ankka-flow's operator, as a prerequisite. ankka-flow's operator neither installs nor upgrades it, and refuses a Flink streamlet where it is absent.
- Q: How do an ankka project's declarations reach `flow verify`? → A: As a file a person exports with ankka's own CLI. `flow` holds no ankka login and calls no control plane.
- Q: Does a call straight into an ankka service belong in this feature? → A: No. It is the next feature. Here a result reaches ankka through a topic.
- Q: Which language is a Flink streamlet written in: the SDK, the sample and the documentation's tested code? → A: Java. The Flink SDK is a Java library, and the sample aggregator and every Flink sample on a page are Java. Writing one in Scala is neither prevented nor documented.
- Q: How does a streamlet author run a pipeline without Kubernetes, now the sidecar's laptop walkthrough is gone? → A: Each streamlet runs as an ordinary process against Kafka in a container: a Pekko Streams streamlet as itself, a Flink streamlet in Flink's local mode from the same main. A Flink streamlet's state there is not kept between runs.
- Q: Do the glossary's proposed terms stand, and do version one's terms go? → A: All stand. Terms only version one used are deleted from the glossary in this feature.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - A stage with no state, on Pekko Streams (Priority: P1)

A streamlet author writes a stage that cleans, filters, reshapes or routes records, as a Pekko
Streams flow from its inlets to its outlets, using the SDK's typed inlets and outlets and nothing
about Kafka. They build an image, name it in a blueprint, and the pipeline runs it: records are
read, the flow's results are written, and a record is committed only once what it produced is
confirmed. The streamlet keeps nothing between records, and the platform offers it no window and no
store.

**Why this priority**: It is the cheaper of the two engines to build, it stays on the family's own
Pekko line, and it proves the new shape end to end: a streamlet that owns its own reading and
writing, with nothing beside it.

**Independent Test**: A Pekko Streams streamlet that drops malformed cart events and rewrites the
rest is deployed between two managed topics; every well-formed event written to the first appears
rewritten on the second; a streamlet killed mid-flow resumes and loses nothing.

**Acceptance Scenarios** *(each names a scenario in a living feature; none is written here)*:

- added `features/pipelines/engines.feature`: a descriptor names the engine its streamlet is written for
- added `features/pipelines/engines.feature`: a Pekko Streams streamlet reads its inlet and writes its outlet
- added `features/pipelines/engines.feature`: a Pekko Streams streamlet commits a record only after what it wrote is confirmed
- added `features/pipelines/engines.feature`: a Pekko Streams streamlet is reset to read its topics again from the start
- added `features/pipelines/laptop.feature`: a streamlet runs as a command on the host in the laptop walkthrough

---

### User Story 2 - A stage with time and state, on Flink (Priority: P1)

A streamlet author writes a stage that aggregates records per key in windows, by the time each
record carries, as a Flink job from its inlets to its outlets through the SDK's typed sources and
sinks. The pipeline runs it on Flink. Its state survives a failure and a new image; a reset
discards the state and reads again from the start. A cluster that cannot run it says exactly what
it lacks.

**Why this priority**: It is the capability the project is being brought back for. Without it the
feature is a second way to write an ankka consumer.

**Independent Test**: A Flink streamlet counting cart events per cart in one-minute windows is
deployed between two managed topics; the counts on the output match the events written; the
streamlet is killed and the counts still match, with no window written twice; a new image is
deployed and an open window's count carries over.

**Acceptance Scenarios** *(each names a scenario in a living feature; none is written here)*:

- added `features/pipelines/engines.feature`: a Flink streamlet reads its inlet and writes its outlet
- added `features/pipelines/flink.feature`: a Flink streamlet groups records into windows by the time each record carries
- added `features/pipelines/flink.feature`: a Flink streamlet that fails continues from its state and counts no record twice
- added `features/pipelines/flink.feature`: a Flink streamlet keeps its state when a new image of it is deployed
- added `features/pipelines/flink.feature`: a new image that cannot read the state leaves the streamlet failed and says so
- added `features/pipelines/flink.feature`: a reset Flink streamlet discards its state and reads its topics again from the start
- added `features/pipelines/flink.feature`: a pipeline with a Flink streamlet is refused where the cluster lacks what Flink needs
- added `features/pipelines/laptop.feature`: a Flink streamlet on a laptop keeps its state only while it runs

---

### User Story 3 - One blueprint, several engines (Priority: P1)

A pipeline author wires a Pekko Streams streamlet and a Flink streamlet in one blueprint, over
topics, exactly as two streamlets have always been wired. `flow verify` checks every topic's ports
against each other whatever engines they belong to, before anything runs. The pipeline deploys as
one resource, and its status, lag and reset read the same for every streamlet.

**Why this priority**: The contract and the wiring being independent of the engine is the whole
design. If mixing engines needs anything special, the seam is in the wrong place.

**Independent Test**: The sample pipeline, a Pekko Streams cleaner feeding a Flink aggregator, is
verified, generated and deployed with `flow`'s existing commands; changing one port's contract
makes `flow verify` refuse it naming both ports.

**Acceptance Scenarios** *(each names a scenario in a living feature; none is written here)*:

- added `features/pipelines/engines.feature`: a blueprint wires streamlets of different engines over one topic
- added `features/pipelines/engines.feature`: a topic whose ports disagree on a contract is refused whatever their engines
- added `features/pipelines/engines.feature`: a descriptor naming an engine flow does not know is refused
- added `features/pipelines/engines.feature`: a descriptor that names no engine is refused
- added `features/pipelines/engines.feature`: lag is read for every streamlet whatever its engine

---

### User Story 4 - A pipeline reads and writes an ankka project's topics (Priority: P2)

A pipeline author names a topic as an ankka project's topic, by the project and the declared name,
and nothing else. `flow` fills the Kafka name by ankka's rule. Each port on the topic states the
contract the project declares, and a difference is refused before the pipeline runs. To ankka the
pipeline is a registered machine: its deploy-time configuration names the machine's credential, an
owner of the project grants the machine each topic, and a missing grant is reported by name rather
than as a silent, empty pipeline.

**Why this priority**: It is what makes the project complementary to ankka rather than merely
beside it, and it is second only because the first three stories are provable without ankka.

**Independent Test**: Beside the ankka shopping cart on kind, the sample pipeline reads the
project's `cart-checkouts` topic and writes a cleaned topic the project declares; an ankka consumer
in the project reads what the pipeline wrote; revoking the grant shows on the pipeline within a
minute, naming the topic.

**Acceptance Scenarios** *(each names a scenario in a living feature; none is written here)*:

- added `features/pipelines/ankka.feature`: an inlet reads an ankka topic by its project and name
- added `features/pipelines/ankka.feature`: an outlet writes an ankka topic by its project and name
- added `features/pipelines/ankka.feature`: a port whose contract differs from the ankka topic's is refused
- added `features/pipelines/ankka.feature`: an ankka topic the project does not declare is refused by name
- added `features/pipelines/ankka.feature`: an ankka topic declared managed is refused
- added `features/pipelines/ankka.feature`: a blueprint verified without the project's declarations says its ankka topics were not checked
- added `features/pipelines/ankka.feature`: a pipeline reaches ankka as the machine its deploy-time configuration names
- added `features/pipelines/ankka.feature`: a topic the machine holds no grant for is reported by name

---

### User Story 5 - Knowing which to choose (Priority: P2)

A person deciding how to build a stage reads one page and knows whether it is a Flink streamlet, a
Pekko Streams streamlet, or not a flow at all but an ankka consumer, and what each engine promises
about delivery.

**Why this priority**: A stateless Pekko Streams streamlet and an ankka consumer overlap by design.
An unexplained overlap is what retired version one; the line has to be written where a reader
meets it.

**Independent Test**: The engines page names the three choices with the need that selects each;
the limitations page states each engine's delivery promise; `just docs` passes.

**Acceptance Scenarios** *(each names a scenario in a living feature; none is written here)*:

- added `features/pipelines/engine-docs.feature`: the engines page says which need selects each engine and which an ankka consumer
- added `features/pipelines/engine-docs.feature`: the limitations page says what each engine promises about delivery

---

### User Story 6 - Version one is removed (Priority: P2)

A person reading the repository, the documentation or a release finds one kind of streamlet, the
engine's. The sidecar, the protocol it spoke, the Scala and Python SDKs written against it, the
stage built into it and the four samples are gone, with the pages and scenarios that described
them. `flow init`, whose templates wrote sidecar streamlets, goes with them until a later feature
gives it templates for the engines. What was published stays published and is not released again.

**Why this priority**: Two kinds of streamlet with different contracts, delivery and credentials
would have to be explained on every page and tested in every suite, for a kind nothing new is
built on. It follows the first three stories because it removes what they replace.

**Independent Test**: The build has no sidecar, protocol or SDK module of version one; `flow
verify` refuses a version one descriptor, saying it names no engine; the features checker passes
with every scenario below gone; the documentation build passes with no page about the sidecar,
the protocol, either SDK or the merge sink.

**Acceptance Scenarios** *(each names a scenario in a living feature; none is written here)*:

- removed `features/sdk/scala-docs.feature`: every page with a Python side has a Scala side from tested code
- removed `features/sdk/scala-docs.feature`: a Scala reader is told what to install
- removed `features/sdk/scala-docs.feature`: the skills describe the Scala SDK
- removed `features/sdk/scala-protocol.feature`: the descriptor written in Scala equals the fixture's bytes
- removed `features/sdk/scala-protocol.feature`: a declaration the protocol refuses is refused before a descriptor is written
- removed `features/sdk/scala-protocol.feature`: the conformance suite passes against the Scala reference streamlet
- removed `features/sdk/scala-protocol.feature`: the sidecar runs a Scala streamlet as it runs a Python one
- removed `features/sdk/scala-protocol.feature`: the Scala SDK ships no copy of the protocol
- removed `features/sdk/scala-release.feature`: a release publishes the Scala SDK and the Scala sample's image with no manual step
- removed `features/sdk/scala-release.feature`: a project depends on the published SDK by version
- removed `features/sdk/scala-release.feature`: a pre-release publishes neither the SDK nor the image
- removed `features/sdk/scala-release.feature`: a release run again does not publish the SDK twice
- removed `features/sdk/scala-sample.feature`: the Scala cart router and the Python one declare the same streamlet
- removed `features/sdk/scala-sample.feature`: the Scala cart router runs the laptop walkthrough
- removed `features/sdk/scala-sample.feature`: the Scala cart router's tests and descriptor are checked on every change
- removed `features/sdk/scala-sample.feature`: the README shows the router in Scala and in Python from tested code
- removed `features/sdk/scala-streamlet.feature`: a streamlet declared in Scala routes a batch as the Python one does
- removed `features/sdk/scala-streamlet.feature`: the harness batches and partitions records by key as the sidecar does
- removed `features/sdk/scala-streamlet.feature`: a failing batch leaves no emit behind
- removed `features/sdk/scala-streamlet.feature`: an emit to an undeclared outlet fails the batch
- removed `features/sdk/scala-streamlet.feature`: a parameter's value comes from the deploy-time configuration, or its default
- removed `features/sdk/scala-streamlet.feature`: the SDK decodes nothing and keeps nothing
- removed `features/cli/init.feature`: a project from flow init passes its own tests
- removed `features/cli/init.feature`: a project's committed descriptor is what its streamlet declares
- removed `features/cli/init.feature`: a project's blueprint verifies against its descriptor
- removed `features/cli/init.feature`: a project depends on the SDK of the flow that wrote it
- removed `features/cli/init.feature`: the streamlet adds a greeting to each record
- removed `features/cli/init.feature`: the greeting is the parameter's deploy-time value
- removed `features/cli/init.feature`: a value that is not a JSON object fails the batch
- removed `features/cli/init.feature`: flow init refuses what it cannot write a working project for
- removed `features/cli/init.feature`: a project runs on a laptop beside the sidecar
- removed `features/cli/init.feature`: a project builds its image
- removed `features/cli/init.feature`: a project's CI checks its tests and its descriptor
- removed `features/cli/init.feature`: the native binary writes the same project as the JVM build
- removed `features/cli/init.feature`: a project carries the agent skills of its flow's version
- removed `features/cli/init.feature`: a project connects Claude Code to flow mcp
- removed `features/cli/install-page.feature`: the first-streamlet tutorial starts from flow init
- removed `features/cli/release.feature`: the images, the Python SDK and the plugin are published whatever the native binaries do
- added `features/cli/release.feature`: the images and the plugin are published whatever the native binaries do
- changed `features/cli/mcp.feature`: a pipeline's lag is read through a tool
- changed `features/cli/mcp.feature`: a streamlet's logs are read through a tool

---

### Edge Cases

- A Flink streamlet on a cluster with no Flink operator, or with no state store configured: the
  pipeline is refused with a condition naming what is missing, and none of its streamlets is
  started. *(the last scenario of the second story, one row each)*
- A new image of a Flink streamlet whose state the new code cannot read: the streamlet is failed
  and says so; the remedy is the previous image or a reset. *(second story)*
- A Pekko Streams streamlet with several replicas: each partition of an inlet is read by one
  replica at a time, and per-key order holds within a partition.
- A record a Pekko Streams streamlet cannot process: the stream fails and the record is read
  again, indefinitely; skipping is the streamlet's own decision, made by writing nothing for it.
- A Flink streamlet writing an ankka topic: the write is at least once, whatever Flink can promise
  between managed topics, because the machine's rights on ankka's broker do not include what
  exactly-once needs. *(fifth story's limitations scenario)*
- A blueprint naming an ankka topic and a broker of its own for the same topic: refused, as ankka
  refuses a component that names both.
- A grant revoked while the pipeline runs: the broker refuses the next read or write; the pipeline
  reports the topic and the right, keeps retrying, and continues when the grant returns.
- A reset Flink streamlet's earlier state: no longer read by anything, and left in the state
  store for the store's own rules or a person to remove.
- A machine credential that is wrong or deleted: reported as a credential problem, distinct from a
  missing grant.
- A descriptor of version one, which names no engine: refused by `flow` and by the operator,
  saying a descriptor must name an engine. *(third story)*
- A pipeline of version one already running on a cluster when the new operator arrives: the
  operator reports it as refused and starts nothing for it; it is deleted by hand.

## Requirements *(mandatory)*

### Functional Requirements

**The engine and the descriptor**

- **FR-001**: A streamlet's descriptor MUST name the engine the streamlet is written for. A
  descriptor that names none MUST be refused.
- **FR-002**: `flow` and the operator MUST refuse a descriptor naming an engine they do not know,
  naming the engine and the ones they know.
- **FR-003**: There MUST be an SDK for each engine, in which a streamlet declares its inlets,
  outlets and parameters, and which writes the descriptor from that declaration. No
  streamlet is found by scanning.
- **FR-004**: An engine SDK MUST give the streamlet's logic its inlets as the engine's own sources
  and its outlets as the engine's own sinks, already configured for the topics, brokers and
  credentials the pipeline was deployed with. Logic MUST NOT name a topic, a broker or a consumer
  group.
- **FR-005**: An engine SDK MUST ship a harness with which a streamlet is tested with no broker
  and no cluster.
- **FR-029**: A streamlet of either engine MUST run as an ordinary process, with no cluster,
  against a Kafka its settings name, from the same main its image runs. `flow` MUST write those
  settings for every streamlet of a blueprint. A Flink streamlet run so keeps its state only
  while it runs, and says so when it starts.

**Pekko Streams**

- **FR-006**: A Pekko Streams streamlet MUST run as a workload of its own with nothing beside it, read
  its inlets and write its outlets, and commit a record only after every record it produced from
  it is confirmed by the broker.
- **FR-007**: A Pekko Streams streamlet MUST be at least once and never skip: a failure reads
  again from the last commit, indefinitely.
- **FR-008**: The Pekko Streams SDK MUST NOT offer windows, timers or a store of state, and the
  platform MUST NOT keep state for such a streamlet.
- **FR-009**: A Pekko Streams streamlet MUST run with the replicas its deploy-time configuration
  gives it, with each inlet partition read by one replica at a time.

**Flink**

- **FR-010**: A Flink streamlet MUST run as a Flink job the platform's operator deploys and
  watches, with the parallelism and resources its deploy-time configuration gives it.
- **FR-011**: A Flink streamlet MUST be able to group records by the time each record carries,
  with the engine's own windows, and nothing in the platform may narrow what the engine offers.
- **FR-012**: A Flink streamlet's state MUST survive the failure of any of its pods, and a new
  image of the streamlet MUST start from the state the previous one left.
- **FR-013**: Between managed topics a Flink streamlet MUST count each record once in what it
  writes, across a failure.
- **FR-014**: A new image that cannot read the state MUST leave the streamlet failed with a
  condition saying so, and MUST NOT discard the state.
- **FR-015**: A pipeline holding a Flink streamlet MUST be refused, with a condition naming what
  is missing and no streamlet started, on a cluster without the Flink operator or without a
  configured state store.

**One pipeline**

- **FR-016**: A blueprint MUST wire streamlets of any engines over topics with no engine named in
  the blueprint, and `flow verify` MUST check every topic's ports against each other from the
  descriptors alone.
- **FR-017**: A port's contract in an engine streamlet MUST be a name and, where the author gives
  a schema document, that document's fingerprint computed as ankka computes it. Two ports connect
  when their names are equal and, where both state a fingerprint, the fingerprints are equal.
- **FR-018**: A pipeline of any mix of engines MUST deploy as one resource and report one status,
  Ready only when every streamlet is running and reading.
- **FR-019**: Lag MUST be reported per streamlet and inlet, read the same way for every engine.
- **FR-020**: Reset MUST work per streamlet and per pipeline for every engine: a reset streamlet
  reads its topics again from the start, and a reset Flink streamlet discards its state first.
- **FR-021**: The sidecar, the streamlet protocol, the Scala and Python SDKs written against it,
  the stage built into the sidecar, the four samples and `flow init` with its templates MUST be
  removed, with their modules, images, release steps, pages and skills. Nothing already published
  is withdrawn, and none of it is published again.

**ankka**

- **FR-022**: A blueprint MUST be able to name a topic as an ankka topic, by project and declared
  name. `flow` MUST fill its Kafka name by ankka's rule. Such a topic is unmanaged; declaring it
  managed MUST be refused.
- **FR-023**: Given the project's declarations, `flow verify` MUST refuse a port whose contract
  name or fingerprint differs from the ankka topic's, a topic the project does not declare, and a
  port on a topic the project declares without a contract, each naming both sides. Without the
  declarations it MUST verify the rest and print a note that the ankka topics were not checked.
- **FR-024**: A pipeline MUST reach ankka only as the registered machine whose credential its
  deploy-time configuration names, under groups ankka allows that machine. `flow` and the operator
  MUST NOT register a machine or make a grant.
- **FR-025**: A read or write the broker refuses for want of a grant MUST be reported on the
  pipeline as a condition and an event naming the topic and the right, retried, and cleared when
  it succeeds. A credential the control plane refuses MUST be reported as that.
**Proof and documentation**

- **FR-026**: A sample pipeline MUST hold one Pekko Streams streamlet and one Flink streamlet in
  one blueprint, be the source of the documentation's samples, and run in the repository's
  cluster suite.
- **FR-027**: The documentation MUST gain an engines page saying which need selects Flink, which
  Pekko Streams and which an ankka consumer; the limitations page MUST state each engine's
  delivery promise and what this feature does not do; and every page MUST describe engine
  streamlets only.
- **FR-028**: `flow mcp`'s lag tool MUST read the lag this feature reports, for every engine, and
  its logs tool MUST read a streamlet's pods, there being no sidecar to choose.

### Key Entities *(each a term in the project glossary)*

- **engine**: What runs a streamlet's logic: Flink or Pekko Streams.
- **streamlet**: One stage of a pipeline, written against an engine's own API: a Flink streamlet
  or a Pekko Streams streamlet.
- **state**: What a Flink streamlet remembers between records, kept by the engine in the
  cluster's state store.
- **ankka topic**: A topic an ankka project declares, named in a blueprint by project and name.
- **machine**: The identity a pipeline has to ankka, a registered machine of an ankka
  organization, to which a project grants topics.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A streamlet author moves a stage from Pekko Streams to Flink, or back, by rewriting
  the stage alone: no line of the blueprint, of any other streamlet, or of any topic's declaration
  changes.
- **SC-002**: Every disagreement between two ports of one topic, and between a port and an ankka
  topic, is refused before anything is deployed, in every pairing of engines.
- **SC-003**: With a streamlet's pods killed at random for ten minutes under load, a Pekko Streams
  streamlet loses no record, and a Flink streamlet between managed topics neither loses nor
  repeats one in what it writes.
- **SC-004**: A missing grant, a missing Flink operator and a missing state store are each named
  on the pipeline within one minute of deploying it, in words that say what to do.
- **SC-005**: The repository, the documentation and a release hold one kind of streamlet: no
  module, image, page, skill or scenario describes the sidecar, its protocol or its SDKs.
- **SC-006**: A reader of the engines page can say which of the three to use for a stated need
  without reading another page.

## Assumptions

- **Nothing depends on version one.** No pipeline of version one is kept running anywhere that
  matters, so there is no migration, no deprecation period and no compatibility layer. The Neo4j
  merge sink already lives in `ankka-contrib` as an ankka service and is not rebuilt here.
- **Two engines, two SDKs, JVM only** (clarified). Pekko Streams streamlets are written in Scala
  on the repository's own Pekko line. Flink streamlets are written in Java against Flink's Java
  API, on Flink's own dependency line, which is pinned here and not mirrored from ankka. The
  plan confirms the Flink release and that nothing it brings carries a BSL licence.
- **The Flink operator and the state store are the cluster's** (clarified). The Flink Kubernetes
  Operator is installed by whoever installs ankka-flow's operator, as a prerequisite; ankka-flow's operator
  renders Flink's resource for each Flink streamlet and reads its status, and does not install or
  upgrade Flink's operator. The state store is durable storage named in the operator's settings.
- **Kafka credentials reach the streamlet.** With no sidecar, a streamlet's container mounts the
  broker credential. The plan says how that is bounded, in place of version one's rule that only
  the sidecar held it.
- **Lag is the consumer group's.** Both engines commit what they have read to a group the
  platform names, so lag is read from the broker for every engine; for Flink it trails by the
  interval at which state is saved.
- **The project's declarations reach `flow verify` as a file** (clarified). A person exports them with ankka's
  own CLI and passes the file; `flow` holds no ankka login and calls no control plane. The broker
  enforces grants at run time whatever was verified.
- **ankka needs no change.** Registered machines, grants of topics, the exposed broker and
  contracts on declared topics are released. A machine reads under groups named
  for it, which the platform's group names must fit.
- **At least once into ankka.** Writes to an ankka topic are at least once for both engines. Exactly-once applies only inside Flink between managed topics.
- **Feature 008 is absorbed.** Its branch stays as a record and is not merged.

## Out of Scope

- Spark, and any third engine.
- Flink SQL, a streamlet that is only a query, Flink streamlets in Python, and a documented or
  tested path for Flink streamlets in Scala.
- A layer that lets one streamlet's logic run on more than one engine.
- Windows, timers or state for Pekko Streams streamlets.
- A call from a streamlet straight into an ankka service, over a granted route or gRPC method: the
  next feature.
- Migrating a version one streamlet or pipeline, or keeping one running.
- `flow init` templates for the engines, and new `flow mcp` tools; each is a later feature.
- Autoscaling a streamlet by its lag, and metrics scraped from a streamlet: lag is on the
  pipeline's status, and a Flink streamlet has Flink's own reporters.
- Registering machines, making grants or declaring topics in ankka from `flow`.
- Connectors to systems other than Kafka and ankka as part of the SDKs; a Pekko Streams streamlet
  may use any library its author adds.
