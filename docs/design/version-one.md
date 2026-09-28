# Version one: the technical shape

> **Superseded in part.** The plan's research (`specs/001-version-one/research.md`) changed
> several things below; where they disagree, the research wins. In short: the protocol is served
> with grpc-java and ScalaPB, not pekko-grpc, and the sidecar has no HTTP server at all (R1). The
> platform's variables are `FLOW_*`, and version one has no callback service on 9011 (R2). The only
> contract format is JSON, fingerprinted as Base64(SHA-256(schema name)); Avro is out (R4). The
> descriptor file is canonical JSON defined in `protocol/DESCRIPTOR.md` (R5). Readiness and liveness
> are files, metrics come from the Prometheus JMX exporter with Cloudflow's rules (R7). A stalled
> partition is a Kubernetes Event raised by the sidecar (R9). The CRD group is
> `flow.ankka.thinkmorestupidless.com` (R11). Deploy-time overrides are merged by the CLI and
> cluster secrets by the operator (R12). The operator writes Kubernetes Events (R13).

The behaviour is in `specs/001-version-one/spec.md`. This is how it is built. Nothing here is
final until the plan; it exists so the spec's requirements have a concrete reading.

## The pod

Two containers, on the pod's loopback interface, exactly as ankka's `hosting = process`:

| container | image | owns |
|---|---|---|
| `sidecar` | the operator's configured sidecar image, never the resource's | Kafka: subscribe, batch, produce, commit, consumer groups; the descriptor check; readiness and liveness; the Prometheus JMX exporter; the mounted Kafka secret |
| `process` | the developer's image | the streamlet's logic; no ports, no probe, no secrets; its liveness is the sidecar's opinion |

Ports mirror ankka's so nobody has to learn two: the process serves the protocol on
`FLOW_PROCESS_PORT` (9010); the sidecar serves its callback service on `FLOW_SIDECAR_PORT`
(9011). Neither binds another interface.

## The protocol

A directory an SDK copies in whole, as ankka's `protocol/` is: `.proto` files, `DESCRIPTOR.md`
(the descriptor file's format), `fixtures/` (descriptors and conversations every SDK must
reproduce exactly), and a `README.md` stating the rules the messages do not.

```
flow/protocol/v1/
  payload.proto     Record { bytes key (optional), repeated Header headers, bytes value }, Error
  discovery.proto   Discovery.Discover(SidecarInfo) -> Spec; Discovery.ReportError(Problems)
  streamlet.proto   Streamlet.Run(stream ToProcess) -> stream FromProcess
```

`Spec` is the descriptor: protocol version, SDK name and version, the streamlet's name, its
inlets and outlets each with `format`, `fingerprint` and (JSON) `schema_name`, and its
configuration parameters. The SDK writes the same message as JSON to a file at build time; the
sidecar compares the file the operator deployed with the answer to `Discover` and refuses on any
difference, after `ReportError` has told the process why.

`Run` is one bidirectional stream per streamlet instance:

```
ToProcess   = oneof { Start { streamlet, config (JSON), inlets, outlets }
                    | Batch { batch_id, inlet, partition, repeated Record (with offset) }
                    | Stop {} }
FromProcess = oneof { Emit  { batch_id, outlet, Record }
                    | Ack   { batch_id }
                    | Fail  { batch_id, Error } }
```

Rules the messages do not state on their own:

- **One batch in flight per (inlet, partition).** The sidecar does not send the next batch for a
  partition until the previous one is acknowledged or failed. Batches for different partitions
  interleave freely.
- **Emits precede the ack.** An `Emit` after its batch's `Ack` fails the stream. An `Emit` naming
  an undeclared outlet fails the stream.
- **Commit after the write.** On `Ack`, the sidecar produces every buffered `Emit` for the batch,
  waits for the broker's confirmation of each, then commits the batch's offsets. A failed produce
  fails the stream; nothing from that batch on is committed. This is Cloudflow's
  `sinkCommittingAfter` with "write" = "produce the emits", and it commits with
  `OffsetFirstObserved` for the same reason: the last batch before a quiet topic must not wait.
- **A `Fail` fails the stream.** The sidecar drops every in-flight batch, reports not ready, and
  reconnects with a new `Start`. A process must discard anything tied to the old conversation.
- **A rebalance discards.** An `Ack` for a partition the sidecar no longer owns is ignored and
  nothing is committed for it.
- **Skipping is acking without emitting.** The sidecar never reads a value; a record the process
  cannot decode is the process's decision.
- **Batches are bounded by count, bytes and time**, so no batch exceeds the gRPC message limit.
  A single record over the limit fails the stream naming its offset.

Versioning is ankka's: `MAJOR.MINOR` carried by both sides in discovery; an optional field, a
message or a fixture is a minor; anything renamed, removed or re-meant is a major; the sidecar
refuses another major naming both.

## The sidecar

One generic Pekko Streams program built at startup from the descriptor. Per inlet: a committable
record source over `<pipeline>.<streamlet>.<inlet>`, grouped by partition, batched by count, bytes
and time, sent to the process, buffered emits produced to their outlets, offsets committed on
confirmation. The Kafka client id of every connection is `<pipeline>.<streamlet>.<port>` so the
JMX consumer and producer metrics, exported by the Prometheus agent in the image, are attributable.

Readiness: the process has answered `Discover` and every inlet is subscribed. Liveness: the stream
is running or reconnecting. Both are served as files the container probe reads, as Cloudflow does.

The sidecar's stream engine is written so a stage can also be a Scala value inside the sidecar
(FR-018): the process is one implementation of "given a batch, give me emits and an ack"; a
built-in stage is another. Version one ships no built-in stage.

## The operator

Follows ankka's operator: fabric8 7.x, a pure rendering function from resource to actions, an
executor that applies them, one k3s suite that proves the real thing. The CRD is `AnkkaFlow` in
`flow.ankka.dev`, short name `aflow` (proposed), whose spec carries the pipeline id and version,
every streamlet's descriptor, image, port mappings, config and replicas, and the topics with their
settings. The operator:

- creates managed topics with the declared partitions and replication, leaves existing ones alone
  and reports a difference as a warning;
- renders one Deployment per streamlet with the two containers, the Kafka secret mounted into the
  sidecar only, and the resolved topic settings written to the sidecar's config secret;
- resolves a topic's settings as deploy-time configuration, then blueprint, then the named or
  default cluster secret;
- carries out a reset request recorded on the resource as an annotation, over the streamlet's own
  connection, one event per group, and writes a done marker (Cloudflow's `reset-offsets`, ported);
- writes status per streamlet and for the pipeline, and refuses a resource when no sidecar image
  is configured.

## The CLI

`flow verify <blueprint> --descriptors <dir> --images <file>` and `flow generate …`: blueprint
verification over descriptor files, emitting the resource. Fingerprints are computed here: JSON
from the schema name (Base64 of SHA-256 of the name). Avro is not in version one.
`flow reset <pipeline> [<streamlet>…]` records the reset request after the same checks the
Cloudflow CLI made (every target scaled to zero, no pods left). GraalVM native image, as ankka's
CLI, but later; a JVM CLI is fine for version one.

## The SDK

`sdks/python`, on ankka's Python SDK's tooling (`uv`, `uv run proto` to copy the protocol in,
`uv run conformance`). A streamlet is a class with declared inlets and outlets and one
`process(batch)` method that yields emits; the SDK serves `Discovery` and `Streamlet`, writes the
descriptor (`flow descriptor` in the project's build), and offers a harness that runs a streamlet
over in-memory inlets and outlets under pytest. The project template ships a compose file: Kafka,
the sidecar configured by files, and the developer's process.

## What is copied from the Cloudflow fork, and with what

Each arrives with its real-Kafka test and its Lightbend copyright notice where the file carries
one (Apache 2.0 requires no more than that, plus the licence text).

| piece | from | becomes |
|---|---|---|
| Blueprint verification: format and fingerprint connection, unconnected inlets, unknown names, topic settings, unmanaged topics | `cloudflow-blueprint` (`UnmanagedTopicSpec`) | `blueprint/` |
| Consumer-group reset over a streamlet's own connection; the done marker; the CLI's guards | `cloudflow-operator`, `cloudflow-cli` (`ConsumerGroupResetSpec`, `ResetOffsetsSpec`, `CliWorkflowSpec`) | operator and CLI |
| Topic actions: create managed, skip unmanaged | `TopicActions` (`TopicActionsSpec`) | operator |
| Commit-after-write ordering, `OffsetFirstObserved` | `sinkCommittingAfter` (`SinkCommittingAfterKafkaSpec`) | the sidecar's commit path |
| Client-id naming and the Prometheus JMX rules | `PekkoStreamletContextImpl.clientId`, `runtimes/pekko/prometheus.yaml` (`ConsumerLagKafkaSpec`, `PrometheusRulesSpec`) | sidecar image |
| Record wire behaviour: keys, header bytes, partition placement | `RecordKafkaSpec` | sidecar tests |

Not copied: streamlet class scanning, the sbt and Maven plugins and everything Scala 2.12, the
Spark and Flink hooks, the JVM testkit and local runner, the vendored kube-actions, the kubectl
plugin and its GraalVM tooling, the Cloudflow docs, the `cloudflow.lightbend.com` CRD.

## Repository layout

```
protocol/        the artefact SDKs copy: proto, DESCRIPTOR.md, fixtures, README
blueprint/       model and verification (Scala 3, no Kafka dependency)
crd/             the AnkkaFlow resource model
sidecar/         the Pekko program, its image, the conformance suite and reference
operator/        rendering, actions, executor; one k3s suite
cli/             verify, generate, reset
sdks/python/     the SDK, its harness, its project template
samples/         the cart router pipeline, in Python, beside an ankka publisher
kustomization/   installing the operator, as ankka's
docs/            public documentation, as ankka's
specs/           spec-kit features
```

Pekko line as ankka and the fork: pekko 1.7.0, pekko-connectors-kafka 1.2.0, grpc-java via ScalaPB 0.11.11,
kafka-clients 3.9.2, families pinned whole. Scala 3 throughout; there is no 2.12 anywhere.
