# Feature Specification: ankka-flow version one

**Feature Branch**: `001-version-one`

**Created**: 2026-09-28

**Status**: Draft

**Input**: User description: "customer can write their streamlet logic in whatever language they
like and then ship it and the operator can deploy the sidecar as necessary which does the work of
consuming/publishing to/from kafka and just feeds to the user code ... create something entirely new
(ankka-flow?) and implement this new architectural approach there ... keep it separate from ankka in
its own repository ... prevent us from having to maintain the pre-deprecation baggage"

## Context

ankka-flow runs streaming pipelines beside ankka. A pipeline is a set of **streamlets**, each with
typed inlets and outlets, wired together by a **blueprint** over Kafka topics. A streamlet's logic is
written in whatever language its author likes and shipped as an image containing only that code.
The platform runs a **sidecar** beside it in every pod that owns everything to do with Kafka:
subscribing, batching, producing, committing offsets, consumer groups, lag, and rebuilds. The two
talk over a protobuf-encoded protocol on the pod's loopback interface, as ankka's polyglot services
do (ankka feature 009).

The lineage is [Cloudflow](https://github.com/lightbend/cloudflow), Lightbend's deprecated
Apache 2.0 project, and the [thinkmorestupidless fork](https://github.com/thinkmorestupidless/cloudflow)
of it that moved it to Apache Pekko and added what nakka's graph pipelines needed: records that keep
their key and headers, JSON contracts verified by schema name, topics the platform does not own,
resetting a pipeline to the start of its inputs, committing offsets only after an external write,
and consumer lag attributed to a streamlet. That fork stays as it is for the consumers it has. This
project starts fresh because the sidecar model removes the reason most of Cloudflow's structure
exists: streamlets discovered by scanning a JVM classpath, images built by sbt and Maven plugins, a
runtime compiled into every image, and the Scala 2.12 build tree that all of that drags along. What
the fork proved carries across as small, self-contained pieces with their tests, each keeping its
copyright notice.

**Where ankka ends and ankka-flow begins.** An ankka view or consumer can already read a Kafka
topic, and an ankka consumer can produce to one. A single service reacting to a topic is an ankka
consumer and stays one. ankka-flow is the layer above: a *graph* of stages with contracts verified
between them before anything runs, several typed outlets per stage with fan-out, topics the
platform creates and owns, per-key ordering preserved through a chain of stages, a pipeline rebuilt
from the start of its inputs, lag per stage, and sinks that batch and commit after writing
elsewhere. If a design needs none of those, it is not a flow.

Three things do not fall out of ankka's sidecar design and are the substance of the work:

1. **Verification before anything runs.** A pipeline's topics are created and its contracts checked
   from a resource the platform holds *before* any pod exists. So a streamlet's description (its
   inlets, outlets and contracts) must exist at build time as a file the SDK writes, not only at
   startup as an answer to discovery. Discovery still happens, but as a check: the process declares
   what it is and the sidecar refuses to start if that differs from what was deployed.
2. **A stream, not a request.** ankka's consumer protocol is one message in, at most one produce
   out. A streamlet reads a stream, emits zero or more records to any of several outlets per input,
   and must not lose or reorder work. The protocol is therefore a conversation per streamlet
   instance with explicit acknowledgement, and the sidecar commits an input's offsets only once its
   outputs are written and the process has acknowledged it.
3. **The sidecar never decodes.** Every record crosses the protocol as bytes with a key and headers,
   exactly as Kafka holds it. A contract is a format and a fingerprint the blueprint verifies, not a
   type the sidecar understands. That keeps the sidecar one program for every language and makes a
   JSON contract expressible from any of them as a name.

The technical shape behind this specification is in `docs/design/version-one.md`.

## Clarifications

### Session 2026-09-28

- Q: Build this into the Cloudflow fork, or start a new project? → A: A new project, `ankka-flow`,
  in its own repository. The fork stays as it is for its existing consumers.
- Q: One repository with ankka, or separate? → A: Separate, following ankka's conventions
  (operator, protocol, SDK and spec layout) so people move between the two without relearning.
- Q: Where does user logic run? → A: In the user's own container, in any language. A Pekko sidecar
  in the same pod owns all Kafka interaction and feeds the user's process over the protocol.
- Q: After a process fails a batch or dies with one in flight, what does the sidecar do next? → A:
  It ends the conversation, reconnects, and redelivers from the last commit with exponential
  backoff, indefinitely. The partition stalls, visible as lag; a warning event is recorded once the
  stall passes a threshold. The sidecar never skips a batch; skipping a record is the process's
  explicit choice, by acknowledging without emitting.
- Q: How is an Avro contract's fingerprint derived so schemas written in two languages connect? →
  A: Drop Avro from version one. The only contract format is JSON, fingerprinted by schema name.
  The descriptor still carries a format field so later formats (Avro, Protobuf) can be added without
  changing its shape.
- Q: What happens when a changed resource is re-applied to a running pipeline? → A: Full
  reconcile. Only streamlets whose image, descriptor or configuration changed are rolled; added
  streamlets start and removed ones stop; new managed topics are created; a changed setting on an
  existing topic is reported as a warning and not applied.
- Q: Where does a streamlet's replica count live? → A: A `replicas` field per streamlet in the
  pipeline resource, default 1. The operator owns one Deployment per streamlet and overwrites any
  direct scaling of it, so scaling and the reset flow are edits to the resource.
- Q: Does version one carry a performance target? → A: One floor, as a success criterion: the
  Python cart router sustains at least 1,000 records per second per partition on a laptop, with the
  sidecar adding under 10 ms median latency over a plain consumer, measured by a benchmark kept in
  the repository. Tuning beyond that is a later feature.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - A streamlet in another language, run on a laptop (Priority: P1)

A developer writes a streamlet in Python: one inlet of JSON cart events keyed by cart id, two
outlets, and a function that routes each event to one of them. They declare the inlets and outlets
with their contracts and the SDK writes the streamlet's descriptor. They start Kafka, the sidecar
and their process on their laptop, produce events to the inlet's topic with a plain Kafka client,
and see them arrive on the right outlet topics with their keys and headers intact. They kill the
process mid-stream and start it again, and nothing is lost.

**Why this priority**: It is the whole feature in miniature. If one streamlet in one other language
can read, fan out, acknowledge and recover through the sidecar, the protocol, the sidecar and the
SDK exist and agree. Everything else is more of the same.

**Independent Test**: Write the cart router in Python. Run Kafka, the sidecar and the router with
one compose file. Produce fifty events over ten cart ids, kill the router after twenty, restart it,
and find every event on exactly the expected outlet topic, each cart's events in order on one
partition, with the CloudEvents headers the producer wrote.

**Acceptance Scenarios**:

1. **Given** a streamlet declared through the SDK and a sidecar started beside it, **When** records
   arrive on an inlet's topic, **Then** the process receives them as bytes with their key and
   headers, and every record it emits is written to the named outlet's topic with the key and
   headers it gave.
2. **Given** a batch the process has acknowledged, **When** the sidecar has written every record
   the process emitted for it, **Then** and only then are that batch's offsets committed.
3. **Given** a process that fails a batch or dies while one is in flight, **When** the sidecar
   observes it, **Then** nothing from that batch on is committed, the conversation ends, and the
   sidecar reconnects and redelivers from the last commit with exponential backoff, for as long as
   it takes; the process sees that batch again before anything later.
4. **Given** records over several keys on one inlet, **When** the process emits each with its
   input's key, **Then** each key's records land on one partition of the outlet in the order they
   were read.
5. **Given** a process that is not running, **When** the sidecar starts, **Then** it reports itself
   not ready and waits, and becomes ready once the process has described itself.
6. **Given** a process that describes a different streamlet from the one deployed, declares an
   outlet twice, or speaks another major protocol version, **When** the sidecar starts, **Then** it
   refuses to start with a message naming every problem, delivered to the process first.
7. **Given** a record the process cannot handle, **When** it acknowledges the batch without
   emitting for that record, **Then** the record is skipped and the stream carries on; the sidecar
   never inspects the bytes.

---

### User Story 2 - A blueprint verified before it runs (Priority: P2)

A developer writes a blueprint naming three streamlets and the topics between them, and runs the
CLI over it together with each streamlet's descriptor and image reference. The CLI refuses when an
outlet is wired to an inlet with a different contract, when an inlet is left unconnected, or when a
port or streamlet is named that no descriptor declares, listing every problem. When it verifies, it
emits the pipeline resource the operator will run.

**Why this priority**: The contract check is what makes a pipeline a pipeline rather than a set of
consumers. It must happen before a pod exists, with no language runtime involved.

**Independent Test**: Verify a blueprint whose JSON outlet names `cart-events.v1` against an inlet
naming `cart-events.v2`, and see it refused with the two names. Rename one and see it verify.
Verify an outlet declared in Python against an inlet declared with the same schema name by the
reference implementation and see it connect.

**Acceptance Scenarios**:

1. **Given** descriptors and a blueprint, **When** an outlet and inlet share a format and
   fingerprint, **Then** they connect; **When** the formats or fingerprints differ, **Then**
   verification fails naming both ports and both contracts.
2. **Given** a blueprint with an inlet connected to nothing, **When** verified, **Then** it is
   refused; an outlet connected to nothing is allowed.
3. **Given** a blueprint that names a streamlet, port or topic no descriptor declares, **When**
   verified, **Then** it is refused naming each.
4. **Given** a topic declared as not managed with only consumers, its real name and its brokers,
   **When** verified, **Then** the consuming streamlet's port carries that name and those brokers.
5. **Given** a blueprint that verifies, **When** the CLI emits the resource, **Then** the resource
   carries every streamlet's descriptor, image and port mappings, and applying it needs nothing
   else.

---

### User Story 3 - Deployed like any other workload (Priority: P3)

An operator applies the pipeline resource to a cluster where ankka-flow's operator is installed. The
operator creates the topics the blueprint owns, runs each streamlet as a pod with the sidecar and
the developer's image side by side, and reports the pipeline `Ready` when every streamlet is. A
streamlet consuming a topic an ankka service publishes reads it without the operator touching that
topic. Nothing in the resource names the sidecar's image.

**Why this priority**: The promise is that a pipeline is deployed, scaled, observed and rebuilt the
same way whatever its stages are written in. Without this the feature is a local curiosity.

**Independent Test**: Apply the cart pipeline to a kind cluster beside a running ankka service that
publishes cart events. See its topics created, its pods `Ready`, records flowing from the ankka
service's topic through the router to both outlet topics, and a scale of the router to three pods
sharing its inlet's partitions.

**Acceptance Scenarios**:

1. **Given** a pipeline resource, **When** applied, **Then** the operator creates every managed
   topic it names with the declared partitions and replication, and no topic declared unmanaged.
2. **Given** a streamlet in the resource, **When** the operator renders it, **Then** its pod has two
   containers: the sidecar from the operator's configured image, carrying the ports, probes and
   Kafka credentials, and the developer's image with no ports and no probe.
3. **Given** a streamlet's pod, **When** its process has described itself and every inlet is
   subscribed, **Then** the streamlet reports ready, and the pipeline reports `Ready` only when
   every streamlet does.
4. **Given** a topic's settings at deploy time, in the blueprint and in the named or default Kafka
   cluster's secret, **When** a streamlet connects, **Then** the deploy-time setting wins, then the
   blueprint's, then the cluster's.
5. **Given** a streamlet whose `replicas` in the resource is set to several, **When** they run,
   **Then** they share one consumer group per inlet and each record is processed by one of them;
   **When** its Deployment is scaled directly, **Then** the operator restores the resource's count.
6. **Given** the operator's sidecar image is not configured, **When** a resource is applied,
   **Then** it is refused with that reason and nothing is created.
7. **Given** a running pipeline, **When** a changed resource is applied, **Then** only the
   streamlets whose image, descriptor or configuration changed are rolled, added streamlets start,
   removed streamlets' pods are deleted, new managed topics are created, and a changed setting on an
   existing topic is recorded as a warning event and left as it is.

---

### User Story 4 - Rebuilt from the start, and watched (Priority: P4)

An operator rebuilding what a pipeline feeds sets its streamlets' `replicas` to zero in the
resource, asks for its inputs to be reprocessed from the beginning, and sets them back up. Every inlet's consumer group starts from
the earliest offset, and each group's outcome is a Kubernetes event on the pipeline. While it
catches up, a Prometheus dashboard shows each streamlet's lag per inlet under the streamlet's own
name.

**Why this priority**: A pipeline that projects into a graph or a store is only trustworthy if it
can be rebuilt, and only operable if its lag can be attributed. Both come from the sidecar owning
Kafka, so they cost little once the first three stories exist.

**Independent Test**: Run the cart pipeline to the end of its input, scale the router to zero,
request a reset, scale it up, and see every event delivered again and the lag rise and fall under
the router's name.

**Acceptance Scenarios**:

1. **Given** streamlets scaled to zero with no pods left, **When** a reset is requested for them,
   **Then** the operator resets each inlet's consumer group to the earliest offset over that
   streamlet's own Kafka connection and records each group's outcome as an event.
2. **Given** a streamlet still running, **When** a reset is requested, **Then** the request is
   refused, and if it reaches Kafka regardless, Kafka's own refusal of a group with members is
   reported as a warning, never as a pipeline error.
3. **Given** a reset carried out, **When** the operator restarts, **Then** it is not repeated.
4. **Given** a running streamlet, **When** its metrics are scraped, **Then** each inlet's lag is
   labelled with an identifier naming the pipeline, the streamlet and the inlet.

---

### User Story 5 - A second SDK can prove itself (Priority: P5)

An SDK author in a third language implements the protocol, points the conformance suite at their
process, and gets a pass or a list of the exact conversations that failed. The suite also runs
against a reference implementation in-process, so a failure is attributable to the SDK, not to the
suite.

**Why this priority**: The protocol outlives every SDK and every sidecar. A suite that any SDK must
pass is what keeps a second language from being a second platform.

**Independent Test**: Run the suite against the reference and against the Python SDK and see both
pass; break one rule in the Python SDK (acknowledge before emitting) and see exactly that
conversation fail.

**Acceptance Scenarios**:

1. **Given** a process speaking the protocol on a port, **When** the suite runs against it,
   **Then** every conversation the protocol defines is exercised and each is reported pass or fail
   by name.
2. **Given** the protocol's fixtures, **When** an SDK's descriptor writer runs over the sample
   streamlets, **Then** its output matches the fixtures byte for byte.

---

### Edge Cases

- A process emits to an outlet the streamlet did not declare: the stream fails, the batch is not
  committed, and the failure names the outlet.
- A process acknowledges a batch it was never sent, or twice: the stream fails.
- A process fails the same batch every time it is redelivered: the partition stalls behind it and
  its lag grows; after a configurable stall threshold the sidecar records a warning event naming the
  inlet, partition and offset. The sidecar never skips or dead-letters the batch; if the process
  wants to move past a record it acknowledges the batch without emitting for it.
- A process emits a record with no key: the outlet's partitioner decides; the default spreads
  records round robin, so per-key order is only promised for keyed records.
- A record or a batch exceeds the protocol's message limit: the sidecar bounds batches by bytes as
  well as by count; a single record over the limit fails the stream naming its offset.
- A consumer group rebalance takes a partition away while its batch is in flight: the batch's
  acknowledgement is discarded and nothing is committed for it; the new owner reads it again.
- The process container is restarted by Kubernetes while the sidecar runs: the sidecar drops every
  in-flight batch, reports not ready, reconnects, repeats discovery and resumes from the last
  commit.
- The sidecar is restarted while the process runs: the process sees a new conversation and must
  discard any state tied to the old one.
- A managed topic already exists with different partitions: the operator keeps it, reports the
  difference as a warning, and does not alter it.
- An unmanaged topic does not exist: the streamlet never becomes ready and the reason is an event.
- Discovery declares a protocol *minor* the sidecar does not know: accepted if the major matches
  and the SDK's minor is not later than the sidecar's; refused otherwise.
- A pipeline is deleted: its pods and managed topics' ownership go with it; whether managed topics
  are deleted is a setting, default keep.

## Requirements *(mandatory)*

### Functional Requirements

**Descriptors and blueprints**

- **FR-001**: A streamlet MUST be described by a descriptor file: its name, its inlets and outlets
  each with a format, a fingerprint and the schema name the fingerprint was derived from, its
  configuration parameters, and the protocol version it was written against. The descriptor's
  format is part of the protocol and versioned with it. Version one defines one format, `json`;
  the field exists so later formats add a value, not a shape.
- **FR-002**: An SDK MUST write the descriptor from the streamlet's declaration, so a descriptor is
  never written by hand, and the same declaration MUST produce the same descriptor in every
  language (checked by fixtures).
- **FR-003**: A JSON contract's fingerprint MUST be derived from its schema name alone. Equal
  format and equal fingerprint connect; nothing else does. Verification MUST refuse any format other
  than `json` in version one, naming it.
- **FR-004**: A blueprint MUST name streamlets, connect outlets to inlets through named topics, and
  declare each topic's settings; verification MUST refuse a connection whose formats or
  fingerprints differ, an unconnected inlet, and any name no descriptor declares, reporting every
  problem in one pass.
- **FR-005**: A topic MAY be declared unmanaged with its real name and its brokers or cluster; the
  platform MUST never create, alter or delete it, and MUST allow only consumers on it.
- **FR-006**: The CLI MUST verify a blueprint over descriptors and image references with no
  language runtime, and emit the pipeline resource on success.

**Protocol and sidecar**

- **FR-007**: The sidecar MUST dial the process on the pod's loopback interface at a fixed port and
  serve nothing on any other interface; the process MUST bind loopback only.
- **FR-008**: On start, the sidecar MUST ask the process to describe itself and MUST refuse to run
  if the description differs from the deployed descriptor, naming every difference, after
  delivering the problems to the process.
- **FR-009**: Records MUST cross the protocol as bytes with their key and headers; the sidecar MUST
  never inspect a record's value.
- **FR-010**: The sidecar MUST deliver records in batches bounded by count, bytes and time, with at
  most one batch in flight per inlet partition, and MUST deliver a partition's batches in offset
  order.
- **FR-011**: The process MUST answer a batch with zero or more emits, each naming an outlet, then
  one acknowledgement or one failure. Emits after the acknowledgement MUST fail the stream.
- **FR-012**: The sidecar MUST write every emitted record to its outlet's topic and have the
  broker confirm it before committing the batch's offsets; a failure to write MUST fail the
  stream with nothing from that batch on committed.
- **FR-012a**: When a stream fails (the process reports failure, dies, breaks a protocol rule, or a
  write fails), the sidecar MUST end the conversation, reconnect, and redeliver from the last commit
  with exponential backoff, indefinitely. It MUST NOT skip, commit past, or dead-letter a batch. It
  MUST record a warning event once a partition has been stalled longer than a configurable
  threshold, naming the inlet, partition and offset.
- **FR-013**: An emitted record MUST be written with the key and headers the process gave; a record
  with no key MUST be partitioned by the outlet's partitioner, round robin by default.
- **FR-014**: The sidecar MUST report readiness only when the process has described itself and
  every inlet is subscribed, and MUST report not ready whenever the process is unreachable.
- **FR-015**: Each inlet's consumer group MUST be named `<pipeline>.<streamlet>.<inlet>` and each
  connection's client identifier `<pipeline>.<streamlet>.<port>`, so lag and throughput are
  attributable from Kafka's own metrics.
- **FR-016**: A streamlet's configuration parameters MUST reach the process at the start of the
  conversation, resolved from the deploy-time configuration and the blueprint.
- **FR-017**: The protocol MUST carry its version as `MAJOR.MINOR`; a sidecar MUST accept an
  earlier minor within its major and refuse another major, naming both.
- **FR-018**: The sidecar MUST be able to host stages built into its own image with no process
  container, so that generic, performance-sensitive stages can ship with the platform. Version one
  ships none; the design must not preclude them.

**Operator**

- **FR-019**: The operator MUST create every managed topic a resource names, with its declared
  partitions and replication, before running the streamlets that use it, and MUST leave an existing
  topic as it finds it.
- **FR-020**: The operator MUST run each streamlet as a pod with the sidecar from its own configured
  image and the developer's image as a second container; the resource MUST NOT name the sidecar's
  image.
- **FR-020a**: Each streamlet in the resource MUST carry a `replicas` count, default 1. The operator
  MUST own one Deployment per streamlet with that count and MUST restore it if the Deployment is
  scaled by other means. Zero is valid and is how a streamlet is stopped for a reset.
- **FR-021**: A streamlet's Kafka connection MUST resolve from the deploy-time topic configuration,
  then the blueprint, then the named or default cluster's secret, and MUST reach the sidecar as a
  mounted secret the process container does not see.
- **FR-022**: The operator MUST report each streamlet's and the pipeline's status, and MUST record
  every refusal and warning as a Kubernetes event on the resource.
- **FR-023**: The operator MUST carry out a reset request for streamlets with no running pods:
  resetting each inlet's group to the earliest offset over the streamlet's own connection, recording
  each group's outcome as an event, and marking the request done so a restart does not repeat it.
- **FR-024**: The operator MUST refuse a resource when it has no sidecar image configured.
- **FR-024a**: On a change to a running pipeline's resource, the operator MUST reconcile to it: roll
  only the streamlets whose image, descriptor or configuration changed, start added streamlets,
  stop removed ones, create new managed topics, and record a warning event for any changed setting
  on an existing topic without applying it. Unchanged streamlets MUST NOT be restarted.

**SDKs and conformance**

- **FR-025**: Version one MUST ship one SDK, for Python, that declares a streamlet, writes its
  descriptor, serves the protocol, and offers a local test harness that runs a streamlet against
  in-memory inlets and outlets without Kafka or the sidecar.
- **FR-026**: A conformance suite MUST exercise every conversation the protocol defines against any
  process on a port, and against a reference implementation in-process, reporting each by name.
- **FR-027**: The protocol directory (proto files, descriptor format, fixtures) MUST be the one
  artefact an SDK copies in, and CI MUST check every copy is identical.

**Local development**

- **FR-028**: A developer MUST be able to run Kafka, the sidecar and their process on a laptop from
  one compose file the SDK's project template provides, with the sidecar configured by files, not
  by a cluster.

### Key Entities

- **Streamlet**: a unit of stream processing with named, typed inlets and outlets; one per process,
  one process per pod.
- **Contract**: a port's format and fingerprint; for JSON the fingerprint is derived from a schema
  name that names a versioned message shape.
- **Descriptor**: the file that describes a streamlet, written by an SDK, checked at startup against
  what the process declares.
- **Blueprint**: the wiring of streamlets through topics, verified before deployment.
- **Topic**: a Kafka topic between stages, managed (created by the operator) or unmanaged (owned by
  someone else, consumed only), with settings resolved from deploy time, blueprint and cluster.
- **Pipeline resource**: the Kubernetes custom resource the CLI emits and the operator runs, carrying
  every descriptor, image, per-streamlet replica count and topic.
- **Record**: value bytes, an optional key and ordered headers, as Kafka holds it.
- **Batch**: the unit of delivery and acknowledgement; one in flight per inlet partition.
- **Sidecar**: the platform's container beside every process, owning Kafka.
- **Reset request**: an operator-carried request to move a streamlet's consumer groups to the
  earliest offset, with a done marker.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: The Python cart router runs end to end on a laptop from one compose file, and the
  independent test of User Story 1 passes: after a kill and restart, every event is on the expected
  outlet exactly once per delivery, each key in order on one partition, headers intact.
- **SC-002**: Blueprint verification over descriptor files runs with no language runtime and
  reports every problem in one pass; a contract mismatch is refused naming both contracts.
- **SC-003**: The cart pipeline deploys to a kind cluster beside a running ankka service, consumes
  that service's topic, and reports `Ready`; scaling the router to three shares its partitions.
- **SC-004**: A reset after scale-to-zero redelivers every input, each group's outcome is an event,
  and the router's lag is visible in Prometheus labelled with its name and inlet.
- **SC-005**: The conformance suite passes against the reference and the Python SDK, and a
  deliberately broken rule fails exactly the conversation that covers it.
- **SC-006**: Removing the commit-after-write guard in the sidecar makes User Story 1's restart
  test fail (records lost), and moving the commit ahead of the emit write does the same.
- **SC-007**: Every piece carried over from the Cloudflow fork arrives with the real-Kafka test that
  proved it there, passing here.
- **SC-008**: A benchmark in the repository, run on a laptop against the compose file, shows the
  Python cart router sustaining at least 1,000 records per second per inlet partition, and the
  sidecar path adding under 10 ms median latency per record compared with a plain Kafka consumer
  reading the same topic. Performance beyond this floor is out of scope for version one.

## Assumptions

These are the defaults chosen where the discussion did not decide; each is easy to revisit before
the plan.

- **Process hosting only.** Version one has no in-process (embedded JVM) hosting; a Scala streamlet
  is written against the protocol like any other, with a Scala SDK as a later feature. The sidecar
  is itself Pekko, so embedding is a small step when something needs it.
- **The only contract format in version one is JSON**, fingerprinted by schema name. Avro and
  Protobuf are follow-ons; each needs schema parsing in the CLI, and Avro needs a canonical form
  agreed across SDKs before two languages can be trusted to fingerprint one schema identically.
- **Batching**: a batch is whatever arrived while the previous one was in flight, capped at 100
  records and 1 MiB, configurable per inlet. (Earlier: "100 records, 1 MiB, 100 ms, whichever
  first"; a timer made every record of a quiet stream wait, which SC-008's latency floor forbids.)
- **At-least-once only.** No transactional or exactly-once delivery; process logic must be
  idempotent, as ankka consumers must be.
- **The CRD has its own group**: `flow.ankka.thinkmorestupidless.com` (under ankka's real domain,
  decided in the plan; `flow.ankka.dev` was the earlier proposal), kind `AnkkaFlow`, plural
  `ankkaflows`, short name `aflow`, and no compatibility with Cloudflow's
  `cloudflow.lightbend.com` resources.
- **The sidecar's loopback ports mirror ankka's** (process 9010, sidecar 9011), so a developer who
  knows one knows the other.
- **The Python SDK lives in this repository** under `sdks/python`, as ankka's does, so protocol,
  suite and SDK move together.
- **Pieces from the Cloudflow fork are copied, not depended on**: blueprint verification, topic
  settings resolution, consumer-group reset, client-id naming with the Prometheus rules, and the
  commit-after-write ordering, each with its test and its copyright notice.
- **Kafka is the only broker**, as in ankka.
- **Local runs use docker compose**, not a platform-provided local runner.

## Out of Scope

- Embedded (in-process JVM) streamlets and a Scala SDK.
- Stages built into the sidecar image (the design admits them; the first, a batched merge sink into
  a graph database, is the next feature).
- Server streamlets: HTTP or gRPC ingress into a pipeline.
- Sharded sources, partition-aware processing across pods, and per-partition state in the process.
- Avro and Protobuf contracts, schema registries, and schema evolution beyond "a new contract is a
  new name".
- Exactly-once delivery.
- A TypeScript SDK (ankka's exists; it follows once the protocol is stable).
- A UI, a hosted control plane, and multi-cluster Kafka.
- Migration of any Cloudflow application or blueprint.
- Performance work beyond the SC-008 floor: tuning, zero-copy paths, and per-language SDK
  optimisation.
