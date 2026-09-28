# Research: ankka-flow version one

**Feature**: [spec.md](./spec.md) | **Plan**: [plan.md](./plan.md)

Each finding says whether it was verified against a repository on this machine (`ankka` at
`../ankka`, the Cloudflow fork at `../cloudflow`, both read on 2026-09-28) or is an assumption a
named task settles at implementation. The list at the end collects the latter. Where this file
disagrees with `docs/design/version-one.md`, this file wins and the design doc is updated by the
first task.

## R1 — Transport: gRPC on loopback with grpc-java and ScalaPB, not pekko-grpc

**Verified in ankka.** `project/plugins.sbt` carries `sbt-protoc` 1.0.6 and ScalaPB
`compilerplugin` 0.11.11; `project/Dependencies.scala` says in a comment why: "grpc-java with
ScalaPB, not pekko-grpc: pekko-grpc runs on pekko-http and every artifact it pulled would need
adding to the family pin". The `protocol` project uses `scalapb.gen(grpc = true)`,
`grpc-netty-shaded` and `grpc-stub` at `scalapb.compiler.Version.grpcJavaVersion`, and its own
`scalacOptions := Seq("-encoding", "UTF-8", "-source:3.3")` because ScalaPB's output warns under
3.9's default source level and `-Wunused`. ankka's research R1 and its "verify at implementation"
item 1 record both facts as answered.

**Decision**: the same. The design doc's `pekko-grpc 1.2.0` is dropped. The sidecar has no HTTP
server at all (R7), so nothing in this repository depends on pekko-http.

**Alternatives considered**: pekko-grpc — an sbt plugin, pekko-http, and the family pin for no
gain, since the sidecar's streams are Pekko streams already and the gRPC stream is just their
source and sink. HTTP/1.1 — no bidirectional stream.

## R2 — Directions, ports and environment: the process listens, the sidecar dials; `FLOW_` prefix

**Verified in ankka.** The process serves on `127.0.0.1:9010` (`ANKKA_PROCESS_PORT`), the sidecar
dials `ANKKA_PROCESS_ADDRESS` (default `127.0.0.1:9010`) and serves its callback service on
`ANKKA_SIDECAR_PORT` 9011 bound to `ANKKA_SIDECAR_BIND` (default `127.0.0.1`). The operator injects
these per container (`operator/.../Rendering.scala:103-104`). Discovery retries with backoff from
500 ms doubling to 10 s.

**Decision**: the process serves `Discovery` and `Streamlet` on `127.0.0.1:${FLOW_PROCESS_PORT}`
(9010); the sidecar dials `FLOW_PROCESS_ADDRESS` (default `127.0.0.1:9010`). Version one has **no
callback service**: every message the process sends travels on the `Run` stream, so the sidecar
binds no gRPC port. 9011 is reserved for a callback service a later feature may need, and the
variable names `FLOW_SIDECAR_PORT`/`FLOW_SIDECAR_ADDRESS` are reserved with it. The prefix is
`FLOW_`, not `ANKKA_`, so a container image that carries both SDKs (an ankka service that also runs
a streamlet is not a design anyone should build, but a shared base image is) cannot read the
wrong sidecar's address. Env vars are listed in `contracts/sidecar.md`.

**Alternatives considered**: `ANKKA_FLOW_` — correct but long in every compose file; the two
platforms are separate repositories and images, so a short distinct prefix is enough. A callback
service in version one — nothing calls it; an emit on the stream is ordered with its ack, which a
callback could not be.

## R3 — The conversation: one `Run` stream per streamlet instance, batches by id, emits before ack

**Verified in the fork.** Cloudflow's Pekko runtime runs one committable source per inlet and
commits through `sinkCommittingAfter` (R9). Nothing in the fork is a protocol; the shape here is
new and follows ankka's per-instance conversation (research R3 of feature 009) in one respect:
strictly bounded in-flight work, correlated by an id the sidecar chooses.

**Decision**: the sidecar dials `Streamlet.Run` once per conversation and sends `Start` first
(pipeline, streamlet, resolved config as JSON, port-to-topic bindings, a conversation id). Then
`Batch{batch_id, inlet, partition, records}` messages, at most one in flight per (inlet,
partition); batches for different partitions interleave. The process answers each with zero or
more `Emit{batch_id, outlet, record}` then exactly one `Ack{batch_id}` or `Fail{batch_id, error}`.
The sidecar buffers emits per batch id and produces them on `Ack` (R9). An `Emit` for an unknown
batch id, an undeclared outlet, or after its `Ack`; an `Ack` or `Fail` for an unknown or completed
id: each is a protocol violation and fails the stream (R10). `Stop` is sent by the sidecar before
it closes the stream on shutdown; the process finishes nothing and exits its handler. Messages are
in `contracts/protocol.md`.

**Alternatives considered**: one stream per (inlet, partition) — simpler correlation, but a
rebalance would open and close streams constantly and every SDK would juggle N handlers. An
`Emitted` acknowledgement from the sidecar back to the process — not needed: commit-after-write
is the sidecar's promise, and the process learns nothing useful from it.

## R4 — Contracts: JSON only, fingerprint = Base64(SHA-256(schema name)), carried from `cloudflow-json`

**Verified in the fork.** `core/cloudflow-json/src/main/scala/cloudflow/streamlets/json/Json.scala`:
`Format = "json"`, `fingerprint(schemaName) = Base64.getEncoder.encodeToString(
MessageDigest.getInstance("SHA-256").digest(schemaName.getBytes(UTF_8)))`, and the descriptor
carries `name`, `schema` (equal to the name), `fingerprint`, `format`. `JsonSchemaVerificationSpec`
proves same name connects, different name refuses, different format never connects. The
clarification of 2026-09-28 dropped Avro from version one.

**Decision**: a `Contract` is `{format: "json", schema_name, fingerprint}` with exactly that
algorithm, computed by every SDK and re-checked by the CLI (a descriptor whose fingerprint does not
match its name is refused: it was hand-edited). Verification connects equal `format` and equal
`fingerprint` and nothing else; any format but `json` is refused by name. The `format` field stays
so Avro or Protobuf can be added as a minor protocol version. `Topic.checkCompatibility`'s Avro
and Protobuf branches are **not** carried over.

## R5 — The descriptor file is the `Spec` message in canonical JSON, defined by `DESCRIPTOR.md`

**Verified in the fork and ankka.** Cloudflow's descriptor is spray-json with snake_case keys and
sorted ports and parameters (`StreamletDescriptor.jsonDescriptor`). ankka's SDK fixtures are
compared byte for byte in each SDK's own test runner (feature 009, FR-028). Protobuf's JSON
mapping alone is not canonical: ScalaPB's json4s printer and Python's `json_format` differ in key
order and whitespace.

**Decision**: `descriptor.json` is the discovery `Spec` message rendered by these rules: proto
field names (snake_case, not lowerCamel), keys sorted at every level, `inlets` and `outlets` sorted
by `name`, `config_parameters` by `key`, enums as their names, fields at their proto3 default
omitted, two-space indent, LF, one trailing newline, UTF-8. Python does this with
`json.dumps(MessageToDict(spec, preserving_proto_field_name=True), sort_keys=True, indent=2)`. The
`protocol` sbt project carries a small hand-written jsoniter codec (`DescriptorJson`) for the same
shape, used by the CLI, the operator and the sidecar; it is checked against the fixtures by a
`protocol` test so it cannot drift from the document. The sidecar compares descriptors field by
field, not byte by byte; the fixtures compare SDK output byte by byte (FR-002).

**Alternatives considered**: scalapb-json4s — a second JSON library in the build and still not
canonical without the same rules on top. A bespoke non-proto descriptor — a second shape to keep
in step with the proto.

## R6 — Blueprint format: HOCON, parsed by the code carried from `cloudflow-blueprint`

**Verified in the fork.** `Blueprint.parseConfig(config, descriptors)` reads
`blueprint.streamlets` and `blueprint.topics` with the keys `topic.name`, `managed`, `producers`,
`consumers`, `cluster`, `bootstrap.servers`, `connection-config`, `producer-config`,
`consumer-config`, `partitions`, `replicas`, `topic`. `UnmanagedTopicSpec` (75 lines) proves a
`managed = false` topic with only consumers and its own brokers. The problem types are a sealed
`BlueprintProblem` with `toMessage`; `BlueprintSpec` (622 lines) and `BlueprintParserSpec` (130)
are pure. The module cross-builds to Scala 3; its only non-standard dependencies are Typesafe
Config, spray-json (descriptor reading), Avro and ScalaPB descriptors (the compatibility
branches).

**Decision**: the blueprint is a HOCON file (`blueprint.conf`) with Cloudflow's keys, so the
carried verification parses it unchanged in shape; HOCON is a JSON superset, so a JSON blueprint
also works. Streamlet entries name a **descriptor** (`streamlets { router = cart-router }` maps a
streamlet name to a descriptor's `name`), not a class. Images are supplied to the CLI beside the
blueprint (`--images images.conf`, a HOCON map of streamlet name to image reference, or repeated
`--image router=ref`) because they change per build and the blueprint is source. The problem types
carried: `DuplicateStreamletNamesFound`, `StreamletDescriptorNotFound`, `InvalidStreamletName`,
`IncompatibleSchema`, `InvalidTopicName`, `InvalidPortPath`, `InvalidProducerPortPath`,
`InvalidConsumerPortPath`, `PortPathNotFound`, `PortBoundToManyTopics`, `InvalidKafkaClusterName`,
`UnconnectedInlets`, the config-parameter problems, `InvalidInletName`, `InvalidOutletName`, plus
new ones: `UnsupportedFormat(path, format)`, `FingerprintMismatch(descriptor, port)`,
`UnmanagedTopicHasProducers(topic, paths)`, `MissingImage(streamlet)`. `UnconnectedOutlets` is
computed but reported as a note, not a refusal (spec S2.2). Not carried: volume mounts, the
`connections` section, `StreamletRef` class-name resolution, the Avro and Protobuf branches.
Managed topics default their Kafka name to `<pipeline>.<id>`, overridable by `topic.name`, so two
pipelines with a `valid-carts` topic do not collide.

## R7 — Sidecar readiness and liveness are files; metrics are the JMX exporter agent

**Verified.** Cloudflow's Pekko runtime wrote readiness and liveness files read by exec probes and
shipped the Prometheus JMX exporter with `runtimes/pekko/prometheus.yaml`; `PrometheusRulesSpec`
proves the rules match JMX names and that `records-lag-max` hits its own rule first. ankka's
sidecar instead serves `/ready` through Pekko Management on 7626 and has no JMX exporter; its
operator renders a readiness probe by port name and no liveness probe.

**Decision**: the sidecar writes `${FLOW_STATE_DIR}/ready` when discovery has succeeded and every
inlet's consumer is subscribed, deletes it whenever the process is unreachable or the stream is
reconnecting, and touches `${FLOW_STATE_DIR}/alive` every second while its main loop runs. The
operator renders exec probes (`test -f`) on the sidecar container only. Metrics: the image
carries `jmx_prometheus_javaagent` on port `2050` with the carried rules file, so consumer
`records-lag` per `client_id`, `topic`, `partition` and producer `record-send-rate` are exported
with `client_id = <pipeline>.<streamlet>.<port>` (FR-015, S4.4). The sidecar registers one MBean
per inlet partition (`ankka.flow:type=sidecar,inlet=…,partition=…`) with `stalledSeconds` and
`inFlight`, and the rules file gains two rules for them. Pod annotations `prometheus.io/scrape`,
`prometheus.io/port: "2050"`. Choosing files over Pekko Management keeps pekko-http out of the
sidecar (R1); choosing the JMX agent over hand-rolled metrics reuses Kafka's own consumer metrics,
which is where lag is authoritative.

**Alternatives considered**: Pekko Management — pekko-http. A tiny hand-written HTTP server for
`/ready` and `/metrics` — a second metrics path beside the one Kafka already exposes over JMX.

## R8 — The sidecar's Kafka graph and the batch processor seam

**Verified in the fork.** `sinkCommittingAfter` (`PekkoStreamletLogic.scala` ~277-326):
`Flow[(T, Committable)].groupedWithin(batchSize, batchWithin).mapAsync(1)(write, then
CommittableOffsetBatch).to(Committer.sinkWithOffsetContext(settings.withCommitWhen(
CommitWhen.OffsetFirstObserved)))`, with the ScalaDoc explaining that `NextOffsetObserved` would
hold the last batch of a quiet topic. `SinkCommittingAfterKafkaSpec` (166 lines, testcontainers)
fails the write on record 12 of 20 and asserts nothing at or past 12 is committed, then a second
run writes and commits all 20. `RecordKafkaSpec` (171 lines) asserts keys, header order including
binary header values, one partition per key in offset order, over 53 partitions.

**Decision**: per inlet, `Consumer.committablePartitionedSource` (one sub-source per assigned
partition, which completes on revocation) → `groupedWeightedWithin(maxBytes, maxRecords, maxWait)`
→ `mapAsync(1)` through a `BatchProcessor` → `Committer` with `OffsetFirstObserved` on the
`CommittableOffsetBatch`. `BatchProcessor` is a trait: `process(batch: InputBatch): Future[Outcome]`
where `Outcome` is the emitted records plus ack or failure. `RemoteProcessor` implements it over
the `Run` conversation; a `BuiltInStage` (FR-018) would implement it in Scala, and version one
ships only the trait and its test double. The write step is "produce every emit with a
`SendProducer` and await every `RecordMetadata`", so the carried `sinkCommittingAfter` ordering
applies with `write = produce`. Emits carry the process's key and headers; a keyless record gets
no key and Kafka's default partitioner spreads it (`RecordKafkaSpec`'s behaviour, minus
Cloudflow's `RoundRobinPartitioner` which is not carried: the producer's default is the
documented behaviour). Producers run with `enable.idempotence=true` (the kafka-clients 3.x
default) so per-partition order holds with five requests in flight. A sub-source that completes
while a batch is in flight marks the batch revoked: its `Ack` is dropped and nothing is committed
(spec edge case). `Consumer.DrainingControl` for shutdown.

**Alternatives considered**: `committableSource` with `groupBy(partition)` — the same graph with a
manually managed substream and no revocation signal. Transactions — out of scope (exactly-once).

## R9 — Stream failure: tear down, back off, rebuild from the last commit; a stall is an event

**Decided in the clarification of 2026-09-28.** On `Fail`, a violation, a produce failure, or the
process becoming unreachable, the sidecar: completes every in-flight batch as failed (nothing
committed), stops the Kafka graph (`DrainingControl.drainAndShutdown` with commits disabled),
removes the ready file, closes the conversation, waits (500 ms doubling to 30 s), redials,
re-runs discovery (so a restarted process that describes a different streamlet is refused, S1.6),
sends a new `Start` with a fresh conversation id, and rebuilds the graph, which rejoins the groups
and resumes from committed offsets. A partition whose batch fails repeatedly stalls; when a
partition's oldest uncommitted batch is older than `FLOW_STALL_WARNING_AFTER` (default 5 m) the
sidecar records a warning once per stall: in a pod, a Kubernetes `Event` (`events.k8s.io/v1`)
regarding its own pod, created through the API server with the pod's service account token over
`java.net.http` (no client library); on a laptop, a `warn` log line. The MBean's `stalledSeconds`
makes it a metric too (R7).

**Alternatives considered**: reporting the stall through the operator — the operator cannot see
into the sidecar without a port to poll, and polling pods is the wrong direction. fabric8 in the
sidecar — 20 MB of client for one POST. Exiting the sidecar to crash-loop — loses the lag and
metrics of a pod that is otherwise healthy, and the spec chose indefinite redelivery.

## R10 — Batch bounds and the message limit

**Verified.** grpc-java's default `maxInboundMessageSize` is 4 MiB on both ends. The spec's
batching defaults are 100 records, 1 MiB, 100 ms.

**Decision**: per inlet, `max-records` 100, `max-bytes` 1 MiB (sum of key, value and header
bytes), `max-wait` 100 ms, configurable in the blueprint's topic `consumer-config` block under
`flow.batch.*` and by deploy-time configuration. The sidecar sets its inbound limit to 8 MiB and
tells the process (in `Start.max_message_bytes`) that a single `Emit` must stay under 4 MiB. One
input record larger than 4 MiB minus 64 KiB of framing fails the stream naming its topic,
partition and offset (spec edge case); the sidecar checks this before batching so the batch is
never sent. A record larger than `max-bytes` but under the limit goes as a batch of one.

## R11 — The resource: `AnkkaFlow` in `flow.ankka.thinkmorestupidless.com/v1alpha1`

**Verified in ankka.** The CRD is a fabric8-annotated class (`@Group("ankka.thinkmorestupidless.com")
@Version("v1alpha1") @Kind("AnkkaService") @Plural @ShortNames`), spec and status are case
classes with Jackson annotations, `AnkkaSerialization` is a Jackson mapper with
`DefaultScalaModule` and `FAIL_ON_UNKNOWN_PROPERTIES=false`, the YAML is hand-written in
`kustomization/components/crd/` with `subresources.status` and printer columns, symlinked into the
module's resources, and `CrdSchemaSuite` checks every case-class field is in the YAML schema. The
label domain is `ankka.thinkmorestupidless.com/`, not `ankka.dev`.

**Decision**: `AnkkaFlow`, group `flow.ankka.thinkmorestupidless.com`, `v1alpha1`, plural
`ankkaflows`, short name `aflow`, namespaced, status subresource. The spec's assumption named
`flow.ankka.dev` as a proposal; ankka's real domain wins and the spec is amended. Labels
`flow.ankka.thinkmorestupidless.com/{pipeline,streamlet,generation}` and
`app.kubernetes.io/managed-by: ankka-flow`. The descriptor is embedded as a `JsonNode`
(`x-kubernetes-preserve-unknown-fields`) holding the canonical descriptor object, so `kubectl get
-o yaml` shows it and the operator parses it with `DescriptorJson` (R5). Full shape in
`contracts/resource-and-operator.md`.

## R12 — Topic settings and Kafka clusters: precedence split between the CLI and the operator

**Verified.** Cloudflow's `TopicActions` read `partitions`, `replicas`, `bootstrap.servers` and
`topic.*` from the port mapping's config, falling back to a named cluster's secret, and refused a
topic with none; `withKafkaConnection` resolved a streamlet's secret then the topic then the
cluster. ankka has **no** Kafka cluster convention at all: Kafka is a bootstrap string in
`ANKKA_KAFKA_BOOTSTRAP_SERVERS`.

**Decision**: a Kafka cluster is a `Secret` named `kafka-cluster-<name>` in the operator's
namespace (`FLOW_KAFKA_CLUSTERS_NAMESPACE`, default `ankka-flow`) with keys `bootstrap.servers`
(required), `connection-config`, `producer-config`, `consumer-config` (Java properties text),
`partitions` and `replicas` (defaults for managed topics). `default` is the cluster a topic gets
when it names none. Precedence (FR-021): the CLI merges deploy-time configuration (`--conf`,
HOCON, keys `flow.topics.<id>.*` and `flow.streamlets.<name>.config.*`) over the blueprint at
`generate` time, so the resource carries the resolved topic settings and streamlet config; the
operator fills whatever is still unset from the cluster secret at reconcile time, because only
the operator can read secrets. The result for each streamlet is one `Secret`
`flow-<pipeline>-<streamlet>` holding `descriptor.json` and `streamlet.conf` (every port's topic,
group id, client id, resolved bootstrap servers and configs, batching), mounted read-only into
the sidecar at `/etc/flow/config` and never into the process container. Locally the same two
files sit in a directory the compose file mounts (FR-028).

**Alternatives considered**: the operator applying all three layers — then the resource does not
say what will run, which ankka's `CLAUDE.md` forbids. A ConfigMap plus a Secret — `consumer-config`
may carry SASL credentials, so one Secret.

## R13 — Operator: ankka's render → actions → executor, plus Kafka Admin actions and Events

**Verified in ankka.** `Rendering.render(resource, settings, …): Either[Vector[String],
Vector[Action]]` is pure; `enum Action` carries fabric8 POJOs; `Fabric8Executor` is the only I/O
and applies with server-side apply and a field manager; two informers enqueue `ServiceRef`s into a
`WorkQueue` with backoff; status is written with `editStatus` and skipped when unchanged; settings
come from system property, then env var, then default; the k3s suite uses
`org.testcontainers.k3s.K3sContainer` with `rancher/k3s:v1.35.1-k3s1`, loads the CRD from the
classpath, runs the operator in-process and imports images with `ClusterImages`. ankka's operator
writes **no** Kubernetes Events. Cloudflow's `ConsumerGroupReset.toEarliest(admin, groupId,
topic)` (53 lines, kafka-clients only) describes the group, fails on members, lists earliest
offsets and alters the group; `ResetOffsets` in the CRD module defines the request and done
annotations and `Request{id, streamlets}`; `ResetOffsetsSpec` is pure and `ConsumerGroupResetSpec`
needs Kafka.

**Decision**: the same architecture, with `Action` gaining `EnsureTopic(TopicInfo)`,
`ResetGroup(target)`, `RecordEvent(reason, note, type)`, `EnsureSecret`, `ApplyDeployment`,
`DeleteDeployment`, `EnsureServiceAccount/Role/RoleBinding` (for the sidecar's event RBAC, R9) and
`SetStatus`. `Fabric8Executor` executes Kubernetes actions; a `KafkaExecutor` executes topic and
reset actions with a cached `Admin` per bootstrap-and-config (Cloudflow's `KafkaAdmins`). Events
are `events.k8s.io/v1` with `regarding` the `AnkkaFlow`, `reportingController
flow.ankka.thinkmorestupidless.com/operator`; reasons in `contracts/resource-and-operator.md`.
A managed topic that exists is described, and a partition-count or replication difference is a
`Warning` event `TopicDiffers` (new; Cloudflow only logged "exists already"). Reset: annotation
`flow.ankka.thinkmorestupidless.com/reset-offsets` holds `{"id","streamlets"}`, the done marker
`…/reset-offsets-done` holds the id; the reconciler acts once per new id, refuses with an event
if any target's `replicas != 0` or pods remain, runs `toEarliest` per inlet group over the
streamlet's own resolved connection, records `ResetOffsets` or `ResetOffsetsFailed` per group,
and writes the done marker (FR-023, S4.1–4.3). Change reconcile (FR-024a): the config Secret's
content hash is a pod-template annotation, so only a streamlet whose image, descriptor,
config or bindings changed rolls; Deployments labelled with the pipeline but absent from the spec
are deleted; a changed setting on an existing topic is `TopicSettingsIgnored`. Delete: owner
references remove Deployments and Secrets; `spec.onDelete.managedTopics: Keep | Delete`
(default `Keep`) adds a finalizer only when `Delete`.

## R14 — CLI: decline, HOCON in, resource out; `reset` talks to Kubernetes

**Verified in ankka.** The CLI is decline 2.6.2, exits 0/1/2, never touches Kubernetes (it talks to
the control plane), and is a GraalVM native image with metadata under `META-INF/native-image`.
Cloudflow's `ResetOffsetsExecution` guards: every target `replicas == 0`, no pods remain, named
streamlets exist and have inlets, then writes the request annotation; `CliWorkflowSpec` covers
the refusals.

**Decision**: `flow verify`, `flow generate`, `flow reset`, `flow version` in decline. `verify`
and `generate` are pure over files (FR-006) and are what CI runs. `reset` needs the cluster: it
reads the `AnkkaFlow`, applies Cloudflow's guards, and patches the request annotation with a
fresh id; fabric8 is a dependency of the CLI for this one command. A JVM CLI (`sbt cli/stage`) is
version one; GraalVM is a later feature, so no native-image metadata is written now. Exit codes as
ankka's.

## R15 — Python SDK: `ankka-flow` on ankka's Python tooling, synchronous `process`

**Verified in ankka.** `sdks/python`: hatchling, `uv`, `grpcio>=1.84,<2`, `protobuf>=6,<8`,
dev `grpcio-tools`, `pytest`, `pytest-asyncio`, `mypy --strict`, no ruff; `scripts/proto.py`
copies `protocol/` into `sdks/python/proto/` (committed) and generates into `src/ankka/_proto/`
(gitignored), rewriting imports; `[project.scripts] conformance` runs the reference process and
then `sbt … -Dankka.conformance.target=…`; CI diffs the copied protocol against the source.

**Decision**: package `ankka-flow`, module `ankka_flow`, the same tooling and scripts
(`scripts/proto.py`, `uv run conformance`, plus `uv run descriptor` to write `descriptor.json`).
A streamlet is a class with `inlets`/`outlets` declared as `JsonInlet("in", schema_name=…)` /
`JsonOutlet("valid", schema_name=…)` and a synchronous `process(self, batch: Batch) ->
Iterable[Emit]`; the server is `grpc.server` with a thread pool, one thread per in-flight batch,
which is bounded by the sidecar to one per partition, so a user's code never sees two batches of
one partition at once. The SDK decodes nothing: `Record.value` is `bytes`; `ankka_flow.json`
offers `loads`/`dumps` helpers. The local harness (`ankka_flow.testkit.Harness`) runs a streamlet
over in-memory inlets and outlets and asserts on emitted records (FR-025). The project template
(`sdks/python/template/`) ships `pyproject.toml`, `Dockerfile`, `blueprint.conf`,
`docker-compose.yml`, `flow/streamlet.conf`, `src/<module>/main.py` and tests. Details in
`contracts/python-sdk.md`.

**Alternatives considered**: `grpc.aio` with `async def process` — ankka's finding 4 in feature
009 shows `grpc.aio` cannot distinguish cancel from half-close and its scheduling dominated the
latency measured there; a streamlet body is CPU-bound decoding, so threads are the simpler and
faster fit.

## R16 — Test infrastructure: munit, testcontainers Kafka, one k3s suite, a stand-in publisher

**Verified.** ankka: munit 1.3.6, testcontainers 1.21.4 (`kafka`, `k3s`), tests forked and
serialised, `-D` switches forwarded to the forked JVM, `-Dankka.cluster.tests=off` skips k3s.
The fork's Kafka tests use `confluentinc/cp-kafka:5.4.3` under ScalaTest.

**Decision**: every carried test is rewritten as a munit suite with its assertions intact and its
Lightbend header kept; Kafka tests use testcontainers' `org.testcontainers.kafka.KafkaContainer`
with `apache/kafka:3.9.1` (KRaft, no ZooKeeper) and the same image runs in compose and in the k3s
suite as a one-node StatefulSet the suite applies. `-Dflow.cluster.tests=off` skips the k3s
suite. SC-003's "beside a running ankka service" is proven in the automated suite by an
**unmanaged topic written by a test producer**, since the topic is all the platform sees of an
ankka service; the pairing with a real ankka service is a manual quickstart tier on the shared
kind cluster. Whether ankka's shopping-cart sample already produces to a topic is item 6 below.

## Verify at implementation

1. **ScalaPB 0.11.11 under Scala 3.9.0 with `-source:3.3`** compiles warning-free and
   `grpc-netty-shaded` serves and dials a bidirectional stream beside a Pekko `ActorSystem` in one
   JVM. ankka answered this; confirm in this build (first `protocol` task).
2. **testcontainers 1.21.4 `org.testcontainers.kafka.KafkaContainer` with `apache/kafka:3.9.1`**
   works with kafka-clients 3.9.x and pekko-connectors-kafka 1.2.0 (the carried Kafka tests).
3. **`committablePartitionedSource` revocation**: a sub-source completes when its partition is
   revoked, and a `CommittableOffsetBatch` whose partition is gone is refused or ignored by the
   `Committer` rather than committed by the new owner's consumer (the rebalance edge case).
4. **`OffsetFirstObserved` with batches from several partitions through one `Committer`**
   commits each partition's offsets independently (the carried `SinkCommittingAfterKafkaSpec`).
5. **JMX exporter**: the current `jmx_prometheus_javaagent` release exposes
   `kafka.consumer<…client-id=…,topic=…,partition=…>records-lag` for pekko-connectors-kafka
   consumers with the carried rules (the carried `ConsumerLagKafkaSpec` and `PrometheusRulesSpec`).
6. **ankka's shopping-cart sample**: whether it produces cart events to a Kafka topic today; if
   not, the manual tier uses ankka's `KafkaPublisher` in a small consumer, or a plain producer.
7. **Python `grpc.server` bidirectional stream with a thread pool**: emits and the ack for one
   batch are delivered in order, and two partitions' batches run on two threads concurrently.
8. **k3s exec probes on one container of a two-container pod** under a rolling update with surge:
   the new pod becomes ready only when its sidecar's file exists.
9. **A Kubernetes Event from the sidecar** via `POST /apis/events.k8s.io/v1/namespaces/…/events`
   with the mounted token needs only `create` on `events` in `events.k8s.io`; confirm the API
   server accepts `regarding` a Pod from a non-controller.
10. **Producer ordering**: kafka-clients 3.9 defaults `enable.idempotence=true` and
    `max.in.flight.requests.per.connection=5`, and pekko-connectors-kafka 1.2.0's
    `ProducerSettings` does not override either.
11. **Typesafe Config version**: the one pekko 1.7.0 pins, so `blueprint` adds no second copy.

## Found during implementation

Answers to the list above as they landed, and design changes the code forced. Each names the test
that proves it.

- **Item 1, ScalaPB under Scala 3.9.0** — answered: the generated code compiles warning-free with
  the build's normal flags when `-Wconf:src=.*/src_managed/.*:s` silences only generated sources,
  so `protocol` needs no `-source:3.3` of its own. grpc-netty-shaded serves and dials bidirectional
  streams beside Pekko in one JVM (`ConversationSuite`).
- **Item 2, testcontainers `apache/kafka:3.9.1`** — answered: works with kafka-clients 3.9.2 and
  pekko-connectors-kafka 1.2.0; every Kafka suite runs on it.
- **Item 3, revocation** — answered, and the design changed. Pekko's partitioned source *keeps* a
  partition's substream when the same rebalance hands the partition back (it waits
  `wait-close-partition` before closing), so `PartitionAssignmentHandler.onRevoke` is not "this
  partition is gone". Marking revocation there left a kept substream dropping every batch forever.
  The substream ending is now the only revocation signal; revocation is scoped to the substream's
  generation, so a late revoke from an old substream cannot hit a newer one's batch; and a revoked
  batch ends its substream (`takeWhile`), so nothing after it can be committed and skip it
  (`InletGraphKafkaSuite`, the revocation case, six consecutive passes).
- **Item 4, `OffsetFirstObserved` across partitions** — answered: one committer per partition
  substream commits each independently (`InletGraphKafkaSuite`, `RestartKafkaSuite`).
- **Item 5, the JMX exporter** — answered, with two corrections to the carried rules. The latest
  agent on Maven Central is 1.0.1. It matches rules against `domain<props><>Attribute: value`, with
  the attribute spelled as the bean spells it (`attrNameSnakeCase` changes only default names), and
  patterns are unanchored: Cloudflow's `records-lag` rule also matched `records-lag-avg` and
  exported duplicate series. Every pattern now ends in `:` and is quoted (an unquoted value ending
  in `:` is a YAML mapping key, and the agent refuses the file and exits). `PrometheusRulesSuite`
  parses the file as YAML and builds names exactly as the agent does; checked against the real agent
  in the sidecar image on the laptop run.
- **Item 7, Python `grpc.server` with a thread pool** — answered: emits and the ack of one batch
  arrive in order, and two partitions' batches run concurrently (`sdks/python/tests/test_server.py`).
- **Item 10, producer ordering** — answered, and the design changed: Pekko's `SendProducer` hops
  every send through a Future callback, so two sends of one batch can reach a partition out of
  order. The carried `RecordKafkaSuite` caught it on its first run. The sidecar calls Kafka's
  `Producer.send` directly, on the caller's thread, in order.
- **Readiness on a missing topic** — a subscription to a topic that does not exist is assigned
  nothing, which looked like "subscribed, nothing for this pod". An empty assignment now counts
  only when partition 0 of the topic has a beginning offset (`RestartKafkaSuite`).
- **FR-005 and consumer auto-creation** — Kafka consumers ask the broker to auto-create a topic they
  subscribe to, so on a broker that allows it the sidecar created an unmanaged topic the moment it
  read it. Every sidecar consumer now sets `allow.auto.create.topics=false`, after user config.
- **The descriptor's JSON** — a dependency-free canonical JSON reader and printer in `protocol`
  (`Json`) replaced the planned jsoniter codec; the descriptor only ever holds strings, objects and
  arrays, and matching Python's `json.dumps` byte for byte is easier to prove without a library in
  between (`DescriptorJsonSuite`, and a Python round trip over every fixture).
- **SC-005 needs an isolated rule** — breaking "ack before emitting" fails every emitting case (12
  of 18), because each sees its emits after the ack. SC-005 is proven with "a keyless emit carries no
  key" (`ANKKA_FLOW_BREAK=keyless-empty-key`), which fails exactly `run.unkeyed-emit`. The Python SDK
  passes all 18 cases that apply to an SDK; the five double-only cases are skipped against a process.
- **SC-008 and the sidecar's latency** — the first benchmark added a 64 ms median, and three causes
  were found by tracing a batch's timeline inside the sidecar (`LatencyBenchSuite`, the double in
  place of Python, no Docker):
  1. *Batching on a timer.* `groupedWeightedWithin(…, 100 ms)` made every record of a quiet stream
     wait for the batch to fill. Batching is now natural (`batchWeighted`): a batch is whatever
     arrived while the previous one was with the process, capped at max-records and max-bytes, and
     `max-wait` is gone. The spec's batching assumption was amended.
  2. *`groupedWithin(1, …)` in the carried commit-after-write.* It held each full group of one back a
     median 14 ms before emitting it. With a batch size of one the grouping is now a plain `map`;
     every other size is unchanged. The sidecar's median fell from 23.8 ms to 9.8 ms.
  3. *Pekko's consumer poll timeout.* The consumer actor blocks in `poll` for up to
     `poll-timeout` (50 ms), and demand for a paused partition waits for it; that was the 40 ms p90.
     5 ms measured best (2 ms thrashes); `poll-interval` is 10 ms and producers default to
     `linger.ms` 1.
- **Item 6, ankka's shopping cart** — answered: its `CheckoutNotifier` produces a
  `CheckoutNotice` (`{"cartId", "at"}`) to `cart-checkouts` when the service has a Kafka bootstrap
  address. But ankka's operator configures no Kafka for services in a cluster, so quickstart tier 7
  cannot run against a deployed ankka service without a change to ankka; the pipeline reading a
  topic someone else owns is proven by FlowClusterSuite's unmanaged topic instead (SC-003's
  automated form).
- **Item 8, exec probes on one container under a rolling update** — answered: FlowClusterSuite's
  S3.7 case rolls the router with surge and the pipeline returns to Ready.
- **Item 9, Events from the sidecar** — partly answered: the body and the RBAC are tested
  (`KubernetesEventSinkSuite`, `RenderingSuite`), but no automated case stalls a partition in k3s
  long enough to see the Event arrive. A stall there needs a process that fails for five minutes.
- **Item 11, Typesafe Config** — answered: Pekko 1.7.0 resolves `config` 1.4.9; `blueprint` now
  pins the same.
- **Fixture `sdk` block** — pinned to `{"name": "fixture", "version": "0.0.0"}` so one file is every
  SDK's expected output.

## Quickstart tiers 6 and 7

- **Tier 6** (kind, 2026-09-28): passed as recorded in `samples/cart-router/README.md`. It also
  caught a documentation error: `kubectl get events --field-selector regarding.kind=…` is refused by
  the core events API; the docs now use `involvedObject.kind`.
- **Tier 7** (beside a running ankka service): not run. ankka's operator configures no Kafka for
  services in a cluster (item 6), and the sample router expects cart events with a `total`, where
  ankka's `CheckoutNotice` carries `cartId` and `at`. Running it needs a Kafka setting in ankka's
  operator and a streamlet for checkout notices; both are follow-ups. What tier 7 proves, a pipeline
  reading a topic the platform does not own, is proven by FlowClusterSuite's unmanaged topic.

## Measurements at the end

SC-008 is measured by `samples/cart-router/bench.py` (quickstart tier 5): Kafka and the sidecar
from the sample's compose file, the Python router on the host, records of about 60 bytes, three
partitions. Throughput: 60,000 events preloaded, drained with the router and sidecar started.
Latency: 200 events/s for 15 s, send to receive, through the router against a plain consumer of
the input.

Machine: Apple M5, 32 GB, Docker 29.4.0 (Docker Desktop), on 2026-09-28. Four consecutive runs
after the fixes above:

| run | throughput per partition | median, plain consumer | median, through the router | added |
|---|---|---|---|---|
| 1 | 3,500 records/s | 1.5 ms | 8.7 ms | 7.2 ms |
| 2 | 4,221 records/s | 1.2 ms | 7.1 ms | 6.0 ms |
| 3 | 5,046 records/s | | | 6.2 ms |
| 4 | 5,640 records/s | | | 6.0 ms |

SC-008 (≥ 1,000 records/s per partition, < 10 ms added median) is met with room on throughput and
about 4 ms of latency headroom. Before the fixes the same benchmark measured 64 ms added. The
sidecar alone (`sbt -Dflow.benchmarks=on 'sidecar/testOnly *LatencyBenchSuite'`, no Docker, no
Python) adds about 7 ms median and 23 ms p90 at this rate; the p90 is the next thing to work on.
