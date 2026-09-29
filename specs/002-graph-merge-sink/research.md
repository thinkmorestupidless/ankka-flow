# Research: a graph merge sink built into the sidecar

**Feature**: [spec.md](./spec.md) | **Plan**: [plan.md](./plan.md)

Each finding says whether it was verified against this repository (read on 2026-09-29), against a
public source (a documentation page or Maven Central, fetched the same day), or is an assumption a
named task settles at implementation. The list at the end collects the latter.

## R1 — The seam: `BatchProcessor` is enough; the stage is its second implementation

**Verified in this repository.** `sidecar/.../BatchProcessor.scala:37-41` is
`trait BatchProcessor { def process(batch: InputBatch): Future[Outcome]; def revoke(inlet, partition, generation): Unit }`,
and its scaladoc names FR-018 of version one as the seam for a stage built into the sidecar. The
only implementation is `Conversation` (`Conversation.scala:19-22`; the plan of version one called it
`RemoteProcessor`, a name that never landed). `InletGraph` (`InletGraph.scala:150-171`) takes any
`BatchProcessor`: per partition, `mapAsync(1)` calls `process`, an `Acked(emits)` becomes
`(emits, CommittableOffsetBatch)`, and `CommitAfterWrite.sinkCommittingAfter` produces the emits and
only then commits. A `process` that completes its Future after an external write therefore gets
commit-after-write for nothing: with no emits, `producers.sendAll(Vector.empty)` completes at once
and the offsets are committed. A failed Future fails the stream, and the whole failure path —
teardown, not ready, backoff, redeliver from the last commit, the stall warning carrying `lastError`
(`Supervisor.scala:64-102`, `Stalls.scala:51-59`) — is reused unchanged. Zero outlets already works:
`outlets` is optional in `StreamletConfig` (`StreamletConfig.scala:105-108`), `Producers(Map.empty)`
creates nothing, and the `sink` fixture has none.

**Decision**: `Neo4jMergeStage extends BatchProcessor` in the sidecar. `process` folds the batch,
runs one write transaction, and completes `Acked(Vector.empty)` only after the transaction has
committed; on any error it fails the Future with `StreamFailed("neo4j merge failed for inlet 'in'
partition 3: <reason>")`. `revoke` is a no-op: a write that finishes after revocation is turned into
`None` by `InletGraph.scala:157`, nothing is committed, and the next owner redelivers, which the
version guard makes harmless. `InletGraph`, `CommitAfterWrite`, `Producers`, `Probes`, `Stalls`,
the events and the metrics are untouched.

**Alternatives considered**: doing the write in `InletGraph`'s `write` callback — needs `InletGraph`
generalised, gains nothing. A separate stream engine for stages — duplicates the graph the feature
exists to reuse.

## R2 — What ties the sidecar to a process, and how stage mode avoids it

**Verified in this repository.** Three places assume a process: `Supervisor.run` always opens a
gRPC channel and each loop pass runs `Discovery` before `Session.start` (`Supervisor.scala:46-62,
64-102`); `Session` is typed to `Conversation` for `failed`, `fail` and `stop` (`105-144`); `Main`
logs the process address. Readiness is "every inlet subscribed" (`Supervisor.scala:124-128`).
`Settings` reads `FLOW_PROCESS_ADDRESS` (`Settings.scala:47`) and nothing about credentials.

**Decision**: `Session` holds a `Stage` — a small trait over what `Session` needs (`processor:
BatchProcessor`, `failed: Future[Throwable]`, `fail(cause)`, `stop(reason)`, `ready: Future[Unit]`)
— with two implementations: `ProcessStage` wrapping today's discovery-and-conversation path, and
`Neo4jMergeStage`. `Supervisor.loop` asks the stage to `open()` where it ran discovery: the process
stage dials and discovers as now; the Neo4j stage verifies connectivity, checks the server version,
ensures the constraints, and is open. The channel is created only by the process stage. The pod is
ready when every inlet is subscribed **and** the stage's `ready` has completed (for Neo4j, after
`verifyConnectivity`). Stage mode is selected by `streamlet.conf`: a `flow.stage { name =
neo4j-merge-sink, neo4j { credentials-dir = "/etc/flow/neo4j" } }` block; its presence means no
process, and `FLOW_PROCESS_ADDRESS` is not set on the container. The descriptor file is unchanged.

**Alternatives considered**: a `FLOW_STAGE` environment variable — a second source of truth beside
the file the operator already renders. A field in the descriptor — changes the protocol, its
fixtures, the canonical JSON and every SDK's copy, for a fact the process never needs.

## R3 — Explicit registration for a stage: the sidecar refuses a descriptor that is not its own

**Verified in this repository.** For a process, the sidecar compares discovery with the deployed
`descriptor.json` and refuses a difference (`Discovery.scala:65-70`, `Descriptor.scala:15-18`). A
stage has no discovery, but the same skew exists: the CLI that generated the resource embedded the
built-in descriptor it knew, and the sidecar image the operator runs carries the one it knows.

**Decision**: at `open()`, the Neo4j stage compares the deployed descriptor with
`Builtins.neo4jMergeSink` from `protocol` using `DescriptorValidation.compare`, and refuses every
difference the way discovery does: logged, and exit 1. (There is no process to `ReportError` to.)
`StreamletConfig.check` still checks the inlet names against the descriptor.

**Alternatives considered**: trusting the deployed descriptor — a CLI newer than the sidecar could
deploy a parameter the sidecar ignores silently.

## R4 — Built-in descriptors are Scala values in `protocol`, named with a `builtin/` prefix in blueprints

**Verified in this repository.** `Blueprint.parseConfig` (`Blueprint.scala:67-72`) stores each
`streamlets { name = descriptorName }` string verbatim, and `StreamletRef.verify`
(`StreamletRef.scala:29-39`) is the only place a name is resolved, by `find(_.name == descriptorName)`
over `Vector[StreamletDescriptor]`. `DescriptorValidation.StreamletName` (`[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?`)
forbids `/`, so `builtin/neo4j-merge-sink` cannot be a descriptor's name. HOCON reads a single `/`
in an unquoted string, so the blueprint needs no quotes. `Verify.run` (`cli/.../Verify.scala:46-49`)
builds the descriptor vector from `Descriptors.load`, which refuses a missing or empty directory
(`Descriptors.scala:21-22, 48`), and `Blueprint.verify` adds `EmptyStreamletDescriptors` for an
empty vector. `protocol/fixtures` is copied verbatim into the Python SDK and CI diffs it
(`ci.yml:71-74`); `sdks/python/tests/test_descriptor_fixtures.py:16` asserts the set of
`fixtures/descriptors/*.json` equals the Python fixture streamlets, so a file there forces a Python
declaration.

**Decision**: `protocol/.../Builtins.scala` holds `val neo4jMergeSink: Spec` (name
`neo4j-merge-sink`, one inlet `in` of `ankka.graph-delta.v1`, no outlets, the parameters of R8) and
`val all`. `blueprint.StreamletDescriptor` gains `builtin: Boolean = false`; `StreamletRef.verify`
matches `"builtin/" + d.name` for a built-in and `d.name` otherwise, so a file named
`neo4j-merge-sink` cannot shadow it. A `builtin/` name with no such built-in is a new problem,
`UnknownBuiltin(streamlet, name)`, whose message lists the built-ins that exist. `Verify.run`
appends `Builtins.all` to the loaded descriptors; `--descriptors` becomes optional and a blueprint
of built-ins alone verifies. The canonical JSON of each built-in is committed under
`protocol/fixtures/builtin/<name>.json` and checked byte for byte by `BuiltinsSuite` on the
`DescriptorFixturesSuite` pattern (same `-Dflow.fixtures.regenerate=on` switch); the Python copy
follows through `scripts/proto.py`, and the Python glob does not see the new directory. The `sdk`
block of a built-in is `{"name": "ankka-flow-sidecar", "version": "0.0.0"}`, pinned like the
fixtures' `fixture`.

**Alternatives considered**: a `stages { }` section in the blueprint — a second concept in every
tool for one stage. A built-in registry the CLI reads from the sidecar image — needs Docker at
verify time, which `flow verify` must never need.

## R5 — The resource records `builtin: true`; `image` stops being required

**Verified in this repository.** `StreamletSpec` (`crd/.../AnkkaFlow.scala:40-51`) has
`image: String = ""`; the CRD schema (`kustomization/components/crd/ankkaflow.yaml:71`) requires
`[name, image, descriptor]`; `CrdSchemaSuite` asserts schema properties equal the case class's
fields. The operator compares `s.image` with the observed process container's image for roll
detection and settling (`Rendering.scala:180`, `LifecycleRules.scala:23-28`,
`Fabric8Executor.scala:62-64`), and `""` on both sides already compares equal. `generateResource`
refuses a streamlet with no image (`cli/.../Main.scala:127-132`) and `ResourceWriter.write`
indexes `images(s.name)` (`ResourceWriter.scala:35`).

**Decision**: `StreamletSpec.builtin: Boolean = false`, appended last; `image` stays a `String`
and is `""` for a built-in; the schema gains `builtin: {type: boolean}` and drops `image` from
`required`. `generateResource` exempts built-ins from the missing-image refusal and refuses an image
given for one (`Streamlet 'graph' is built in and takes no image.`); `ResourceWriter` writes
`builtin = true, image = ""`. The operator refuses a resource whose built-in streamlet carries an
image (FR-021) and reads `builtin` nowhere else than rendering.

**Alternatives considered**: `image: Option[String]` — cleaner, but touches every comparison and
test for no behavioural gain. A separate `stages:` list in the spec — the same duplication as in
the blueprint.

## R6 — The Neo4j connection is a Secret in the pipeline's namespace, named by a parameter, mounted into the sidecar

**Verified in this repository.** Kafka cluster Secrets are not mounted: the operator lists
`kafka-cluster-*` in its own namespace, parses them (`Fabric8Executor.scala:97-117`,
`TopicResolution.scala:41-61`) and copies the values as text into `streamlet.conf` inside the
rendered Secret `flow-<pipeline>-<streamlet>` (`StreamletFiles.scala:58-69`). A missing one is a
`Refused` event and `Failed` status with no other action (`Rendering.scala:56-71`). There is no
per-streamlet Secret reference anywhere; parameters land in the resource's `config` in plain text.
The operator's ClusterRole already grants `get` on Secrets in every namespace
(`operator.yaml:30-32`). A pod can mount only a Secret in its own namespace.

**Decision**: the sink declares a required `STRING` parameter `secret`, the name of a Secret **in
the pipeline's namespace** holding `uri`, `username`, `password` and optionally `database`. It
travels as any parameter (`--conf`: `flow.streamlets.graph.config { secret = neo4j-shop }`) into
`spec.streamlets[].config`, so the resource says what runs and a Secret's *name* is not a secret.
The operator's `observe` fetches the named Secret for every built-in streamlet; a missing one, or
one lacking `uri`, `username` or `password`, is refused like a missing Kafka cluster
(`Refused`: `streamlet 'graph' names Secret 'neo4j-shop', which does not exist in namespace
'shop'`). Rendering mounts it read-only at `/etc/flow/neo4j` (a `secret` volume, `defaultMode`
0400) and writes `flow.stage.neo4j.credentials-dir = "/etc/flow/neo4j"` into `streamlet.conf`; the
password never enters the rendered config Secret or the resource. The Secret's `resourceVersion` is
folded into the config hash so a rotated credential rolls the pod (a Secret informer is not added;
as for Kafka clusters, a Secret created after a refusal is seen at the next resync).

**Alternatives considered**: the operator's namespace, like Kafka clusters — a pod cannot mount
across namespaces, and copying the password into `streamlet.conf` would put it in a second Secret
and in the config hash. A `secret` field on `StreamletSpec` — the parameter path already exists and
keeps the CRD change to one boolean. Environment variables from the Secret — files are what the
Kafka rule ("mounted from a Secret into the sidecar container") already says.

## R7 — Neo4j 5.26 LTS, Cypher 5, the Java driver 5.28, testcontainers `neo4j`

**Verified against public sources.** Cypher 5 sets labels from a parameter (`SET n:$($labels)`,
introduced 5.24), removes them (`REMOVE n:$(labels(n))`, 5.24), and `MERGE` takes a dynamic label or
relationship type (`MERGE ()-[r:$(d.type)]->()`, introduced 5.26) — the Neo4j Cypher manual, SET,
REMOVE and MERGE pages. `SET n = $map` replaces every property. Docker Hub has `neo4j:5.26-community`
(updated 2026-09-26) as the current 5.x LTS line; the 2025.x calendar-versioned line exists beside
it. Maven Central has `org.neo4j.driver:neo4j-java-driver` 5.28.5 and
`org.testcontainers:neo4j:1.21.4`, the repository's testcontainers version.

**Decision**: the sink targets Neo4j 5.26 or later and refuses to open against an older server
(the driver reports the server agent on `verifyConnectivity`; a server below 5.26 cannot run the
merge statement, so the refusal names the version and the reason). `V.neo4jDriver = "5.28.5"`,
`V.neo4jImage = "neo4j:5.26-community"`, `V.testcontainers` unchanged; `neo4jDriver` on `sidecar`,
`testcontainersNeo4j % Test` on `sidecar` and `operator`. `-Dflow.neo4j.image=${V.neo4jImage}` is
passed to forked test JVMs beside `flow.kafka.image`, keeping the no-literal-image-tags rule. The
driver is a new dependency under the "not in this build" rule, recorded here: it brings
`neo4j-bolt-connection-*`, reactive-streams and (optionally) reactor and micrometer; nothing else in
the sidecar uses unshaded Netty (grpc is `grpc-netty-shaded`), so no clash is expected — verified at
implementation (item 1).

**Alternatives considered**: a driver-free Bolt client — no. The HTTP query API — a second port,
no transactions across statements in the way the driver gives, and not what nakka's design named.
Supporting 2025.x now — the Cypher used is in both; add the tag to the matrix when something needs
it.

## R8 — The sink's parameters

**Decision**: `secret` (STRING, no default, so required: the connection Secret's name);
`transaction-timeout` (DURATION, default `30s`: the driver's per-transaction timeout, so a hung
database fails the batch rather than the liveness probe). Nothing else: batch size is the inlet's
(`batch { max-records, max-bytes }`), the labels and types come from the deltas, and the database
name is in the Secret. A parameter is declared in the built-in descriptor, resolved by the CLI like
any other, and read by the stage from `flow.config` in `streamlet.conf` via
`StreamletConfig.configJson`.

**Alternatives considered**: an `on-malformed` parameter — decided against in the spec (a bad
delta fails its batch). A `constraints` switch — decided against in the spec (create when possible,
warn otherwise).

## R9 — The delta contract and how a batch becomes one transaction

**Decision.** Schema name `ankka.graph-delta.v1`, JSON, one object per record, with `kind` one of
`node`, `edge`, `tombstone` ([contracts/graph-delta.md](./contracts/graph-delta.md)). The stage
parses records with the protocol's own `Json.parse` (`protocol/.../Json.scala:113`; `Num` is a
`BigDecimal`, so an integral number becomes a Neo4j integer and any other a float) — no new JSON
dependency, and the throughput floor of 1,000 deltas/s per partition is far below what a
hand-written parser of 200-byte documents costs. A record that does not parse, is not an object,
lacks a required field, has a nested-object property, or names an unknown kind fails the batch with
a message naming the record's offset.

Within a batch the stage **folds** deltas to one per element id — the highest version wins; on a
tie the first in the batch wins and the rest count as stale — so one `UNWIND` per kind sees each
element once and the order of kinds within the statement cannot matter. It then runs, in one
explicit write transaction: the node merges, the edge merges, the node tombstones, the edge
tombstones, each as one parameterised statement over its list; commits; and completes the batch.
Every node the stage writes carries the fixed label `Element` beside the delta's labels, because
identity is the id alone and a uniqueness constraint needs one label; edges are found from their
endpoints, so no relationship constraint is needed. The version and the mark are stored as
`_version` and `_deleted` on the element. The statements are in
[contracts/neo4j-merge-sink.md](./contracts/neo4j-merge-sink.md).

Batches of different partitions run concurrently and may touch the same nodes (an edge's endpoints
live on other partitions), so Neo4j may detect a deadlock and abort one; the stage runs the
transaction through the driver's managed `executeWrite`, which retries transient failures, and the
merge is idempotent, so a retry is safe.

**Alternatives considered**: applying deltas row by row in batch order without folding — correct
but slower and lets a kind ordering bug in; folding is what a state-shaped, versioned contract
allows. A relationship uniqueness constraint per type — types are dynamic, so the set is unknown at
startup. Storing versions in a side node — a second lookup per delta.

## R10 — Constraints: created when possible, a warning when not

**Verified against the Cypher manual.** `CREATE CONSTRAINT element_id IF NOT EXISTS FOR
(n:Element) REQUIRE n.id IS UNIQUE` is idempotent and needs the `CREATE CONSTRAINT` privilege.

**Decision**: at `open()` the stage runs it; a failure for want of privilege is a warning event
`ConstraintNotCreated` on the pod (`<pipeline.streamlet>: could not create constraint element_id:
<reason>; merges will scan until it exists`) and the stage opens anyway; any other failure (the
database is unreachable) is a failed open, retried with the reconnect backoff.

## R11 — Metrics and events

**Verified in this repository.** Sidecar metrics are JMX beans registered by `Metrics.refresh`
(`Metrics.scala:25-40`) under `ankka.flow:type=sidecar,inlet=…,partition=…` and exported by the
JMX exporter with the rules in `sidecar/src/universal/agent/prometheus.yaml:44-55`, checked by the
carried `PrometheusRulesSuite`. Events are `EventSink.warning(reason, note)` on the pod.

**Decision**: a second bean per partition, `ankka.flow:type=stage,inlet=…,partition=…`, with
`DeltasWritten`, `DeltasStale` and `BatchesFailed` counters, exported as
`ankka_flow_stage_deltas_written_total`, `ankka_flow_stage_deltas_stale_total` and
`ankka_flow_stage_batches_failed_total`, three new rules with tests. One new warning reason,
`ConstraintNotCreated`; `PartitionStalled` is reused as it is.

## R12 — Tests: two Neo4j suites in the sidecar, one rendering suite, one k3s scenario

**Verified in this repository.** `KafkaSuite` starts one Kafka container from `flow.kafka.image`
with `publish`, `committed(group)` and `eventually`; `SidecarRun` runs a whole sidecar in-process
from a `Spec` and a `streamlet.conf` text; `SinkCommittingAfterKafkaSuite` is the template for
"the write fails, nothing is committed, everything is written after"; `RestartKafkaSuite` for
readiness and resume; `RenderingSuite` asserts pods from `Rendering.render` with fixtures;
`FlowClusterSuite` runs k3s with images imported by `ClusterImages.importInto`, which auto-pulls
only `apache/kafka*` (`ClusterImages.scala:18`).

**Decision**:
- `Neo4jMergeSuite` (sidecar, Neo4j container only): the stage's `process` against a scripted
  sequence — create, higher version replaces and removes properties, equal and lower are stale,
  labels change, edge creates placeholders, tombstones mark and win over lower versions, fold
  within a batch, every kind of malformed record fails the batch naming its offset, numbers and
  arrays round-trip, a server below 5.26 or a wrong password is refused with the reason and no
  credential in the message, the constraint is created and a user without the privilege gets the
  warning event.
- `Neo4jSinkKafkaSuite` (sidecar, Kafka and Neo4j containers): the sidecar in stage mode through
  `SidecarRun`: ready only once Neo4j answers; commit only after the write; Neo4j paused
  (`dockerClient.pauseContainerCmd`) mid-run → not ready, lag grows, the stall warning names the
  Neo4j error, nothing committed; unpaused → drains, every delta once; the sidecar killed after the
  transaction and before the commit → redelivered and stale; the group reset to the start →
  identical graph; a descriptor that is not the sidecar's built-in → exit 1.
- `RenderingSuite` additions (operator): one container, the `neo4j` volume and mount, no
  `FLOW_PROCESS_ADDRESS`, `stage.neo4j` in `streamlet.conf` without the password, `builtin: true`,
  the refusals (missing Secret, missing key, image given).
- `FlowClusterSuite`: one scenario with a Neo4j Deployment in the cluster and the sample's mapper
  image — the sink pod has one container, the Secret is mounted, and a delta produced to the topic
  is in the graph (queried through a NodePort with the driver from the test JVM).
- `CliBuiltinSuite`/additions to `VerifySuite` and `GenerateSuite`; `BuiltinsSuite` in `protocol`;
  `CrdSchemaSuite` round trip with a built-in streamlet; `StreamletFilesSuite` for the stage block.
- The Python sample's mapper: `Harness` tests, and the descriptor fixture check.

The k3s scenario is worth its minute because the operator's whole contribution — the pod shape and
the mount — is only proven by a real kubelet.

## R13 — The sample: `samples/checkout-graph`, a Python mapper in front of the sink

**Verified in this repository.** `samples/checkout-feed` reads ankka's checkout notices (JSON
`{"cartId","at"}`, key `cartId`, CloudEvents headers) with `JsonInlet("in",
schema_name="ankka.checkout-notice.v1")`, emits with `outlet.emit(record, value=…)`, and is tested
with `Harness`; `emit(value=…, key=…)` builds a re-keyed record. The local cluster's Kafka is at
`kafka.kafka.svc:9092`; `just up` installs the CRD, the operator and Kafka through
`kustomization/overlays/local` and `deploy-local.sh`.

**Decision**: `samples/checkout-graph`: a Python streamlet `checkout-graph` with inlet `in`
(`ankka.checkout-notice.v1`) and outlet `deltas` (`ankka.graph-delta.v1`) that emits, per notice,
three deltas keyed by their element ids: node `cart:<cartId>` (`Cart`), node
`checkout:<cartId>:<at>` (`Checkout`, `at` as an ISO timestamp), and edge `checked-out:<cartId>:<at>`
(`CHECKED_OUT`, cart → checkout), each with version `at`. The blueprint wires `cart-checkouts`
(unmanaged) → `mapper` → `graph-deltas` (managed, 3 partitions) → `graph = builtin/neo4j-merge-sink`.
A `docker-compose.yml` with Kafka, Neo4j and two sidecars (the mapper's, dialling the host; the
sink's, in stage mode with a mounted credentials directory) for the laptop; a
`kustomization/overlays/neo4j` (a `neo4j` namespace, a StatefulSet on `V.neo4jImage`, a Service,
and the sample Secret `neo4j-local` in `shop`) installed by `just neo4j-up`, so `just up` stays
Kafka-only. CI tests the sample; the release pushes `sample-checkout-graph`.

**Alternatives considered**: extending `checkout-feed` with a second outlet — two samples that each
show one thing read better than one that shows two. Neo4j in `just up` — every developer would run
a database only the graph sample needs.

## R14 — Rules that change, and documentation

**Decision**: CLAUDE.md's rule "The sidecar never decodes" becomes "The sidecar never decodes for a
process. A record's value is bytes from Kafka to the process and back. A built-in stage decodes its
own contract and nothing else." The skills' rule text follows. `reference/limitations.md` drops "no
stages built into the sidecar" and gains "one built-in stage, and no way to add one from outside the
sidecar image". New pages: `reference/graph-deltas.md` (the contract), `reference/neo4j-merge-sink.md`
(parameters, the Secret, what it writes, metrics, events), `build/graph-sink.md` (the guide, from
the sample). Updated: `reference/blueprint.md` (`builtin/`), `reference/cli.md`, `reference/resource.md`
(`builtin`), `reference/operator.md`, `reference/sidecar.md` (stage mode, metrics, the mount),
`deploy/observe.md`, `deploy/troubleshooting.md`, `concepts/sidecar.md`, `reference/glossary.md`,
`index.md`. Skills: `ankka-flow` carries the guide and the delta reference; `ankka-flow-deploy` the
sink reference and the guide; `ankka-flow-python` and `ankka-flow-protocol` the delta reference.

## Verify at implementation

1. **Netty**: `neo4j-java-driver` 5.28.5's `neo4j-bolt-connection-netty` — shaded or not; either
   way confirm `sidecar/docker:publishLocal` starts and `evicted` shows no conflict with
   `grpc-netty-shaded` (first sidecar task).
2. **`executeWrite` and dynamic labels in one transaction**: `REMOVE n:$(...)` followed by `SET
   n:$(...)` and `MERGE ()-[r:$(d.type) {id: d.id}]->()` against `neo4j:5.26-community` through
   the driver, with the statements of `contracts/neo4j-merge-sink.md` exactly (the first
   `Neo4jMergeSuite` case).
3. **Server version from `verifyConnectivity`**: the driver's `ServerInfo.agent()` string format
   (`Neo4j/5.26.x`) — the refusal below 5.26 parses it.
4. **`SET r = map` on a relationship** replaces every property as it does on a node.
5. **The `CREATE CONSTRAINT` privilege failure** is a `ClientException` with code
   `Neo.ClientError.Security.Forbidden` — the branch that warns rather than fails.
6. **`Neo4jContainer` in testcontainers 1.21.4**: `withAdminPassword`, `getBoltUrl`, and pausing
   through `getDockerClient().pauseContainerCmd(id)` from the test JVM.
7. **`resourceVersion` in the config hash**: confirm `Fabric8Executor.observe` can fetch the Secret
   with one `get` per built-in streamlet without a measurable reconcile cost.
8. **The k3s NodePort for Bolt**: `FlowClusterSuite` reaches Neo4j from the test JVM through a
   NodePort mapped by the k3s container, as Kafka's 30094 is.

## Found during implementation

- **Item 1, Netty** — answered: `neo4j-java-driver` 5.28.5 brings `neo4j-bolt-connection-*` 2.0.0,
  reactor-core 3.6.16 and **unshaded** Netty 4.1.119 (`netty-handler`, `-codec`, `-transport`, …).
  Nothing else in the sidecar uses unshaded Netty: gRPC is `grpc-netty-shaded` and kafka-clients
  uses none, so the two coexist; `sidecar/evicted` shows only error-prone annotations.
- **Items 2–4, the statements through the driver** — answered by `Neo4jMergeSuite` against
  `neo4j:5.26-community`: `REMOVE n:$([...])` with an empty list is a no-op, `SET n:$(d.labels)`
  and `MERGE ()-[r:$(d.type) {id: d.id}]->()` run as written inside one `executeWrite`, and
  `SET r = map` replaces a relationship's properties as it does a node's. The server's agent
  (`Neo4j/5.26.x`) is not on the `Driver` in 5.28; it comes from a query's summary
  (`session.run("RETURN 1").consume().server().agent()`).
- **Item 5, the privilege failure** — not testable here: Neo4j Community has no role-based
  privileges (every user can create constraints), so the `Neo.ClientError.Security.*` branch that
  records `ConstraintNotCreated` is verified by inspection only. An Enterprise image would test it.
- **Item 6, pausing the container** — answered: `getDockerClient.pauseContainerCmd` from the test
  JVM freezes Neo4j, and the driver then waits for a response indefinitely.
- **A frozen database hung the batch forever** — found by `Neo4jSinkKafkaSuite`'s outage case. The
  transaction timeout is enforced by the server, so a server that stops answering enforces nothing:
  the batch never completed, the pod stayed ready and the partition stalled silently. Now each batch
  has a client-side deadline (`transaction-timeout` + 5 s), the driver has bounded connection and
  acquisition timeouts and a retry budget equal to the transaction timeout, and a driver whose batch
  failed is discarded (closed asynchronously) rather than reused.
- **Credentials are re-read on every connection attempt** — a mounted Secret's files change in
  place when the Secret does, so a corrected or rotated password is used without a restart; the
  first version cached the first read and waited on a wrong password forever.
- **`PartitionStalled` never fired for a batch that fails every time (a version-one defect)** —
  found by the same outage case. `InletGraph` forgot a partition's stall whenever its substream
  ended, and a failed batch ends its substream (and a teardown ends them all), so every reconnect
  reset the clock; `SupervisorSuite` tested `Stalls` alone and missed it. A stall is now forgotten
  only when a substream *completes* while the sidecar is not tearing down (a revocation), and an
  assignment forgets stalls of partitions that moved away. `RestartKafkaSuite` has the regression
  test (a process batch that always fails is warned within the threshold); it failed before.
- **`version` written with an exponent** — `1e3` parses to a whole `BigDecimal` and is accepted as
  version 1000; the contract's "a JSON integer" is read as "a whole number".
