# Data model: ankka-flow version one

**Feature**: [spec.md](./spec.md) | **Plan**: [plan.md](./plan.md) | **Contracts**: [contracts/](./contracts/)

Four representations of the same pipeline exist and the boundaries between them are fixed here:
the **descriptor** (what a streamlet is), the **blueprint** (how streamlets are wired), the
**resource** (what the operator runs) and the **sidecar's files** (what one pod does). Each is
derived from the previous by one program, never by hand.

## Protocol (package `ankka.flow.v1`, `protocol/`)

### Payload

| message | fields | notes |
|---|---|---|
| `Record` | `key: optional bytes`, `headers: repeated Header`, `value: bytes` | as Kafka holds it; `value` never inspected (FR-009) |
| `Header` | `key: string`, `value: bytes` | order preserved |
| `Error` | `message: string` | a process's failure or the sidecar's refusal reason |
| `Problem`, `Problems` | `message`; `problems: repeated` | discovery refusals, all at once |

### Discovery

| message | fields | validation |
|---|---|---|
| `SidecarInfo` | `protocol_version`, `sidecar_version` | |
| `Spec` | `protocol_version`, `sdk: SdkInfo`, `streamlet: StreamletDescriptor` | major equal, minor ≤ sidecar's (FR-017) |
| `SdkInfo` | `name`, `version` | informational; logged when it differs from the deployed descriptor |
| `StreamletDescriptor` | `name`, `description`, `inlets: repeated Port`, `outlets: repeated Port`, `config_parameters: repeated ConfigParameter` | `name` `[a-z0-9-]{1,63}`; port names unique across both lists |
| `Port` | `name`, `contract: Contract` | `[a-z][a-z0-9-]{0,62}` |
| `Contract` | `format`, `schema_name`, `fingerprint` | `format == "json"`; `fingerprint == Base64(SHA-256(schema_name))` |
| `ConfigParameter` | `key`, `description`, `type: ConfigType`, `default_value` | `key` `[a-z][a-z0-9-]*` unique; default parses as type; absent default = required |
| `ConfigType` | `STRING, INTEGER, DOUBLE, BOOLEAN, DURATION, MEMORY_SIZE` | |

### Run

| message | fields | rule |
|---|---|---|
| `Start` | `conversation_id`, `pipeline`, `streamlet`, `config_json`, `inlets: repeated PortBinding`, `outlets: repeated PortBinding`, `max_message_bytes` | first message; voids any earlier conversation |
| `PortBinding` | `port`, `topic` | |
| `Batch` | `batch_id: uint64`, `inlet`, `partition: int32`, `records: repeated InputRecord` | one in flight per (inlet, partition); offset order |
| `InputRecord` | `offset: int64`, `timestamp_ms: int64`, `record: Record` | |
| `Stop` | `reason` | sidecar shutting down |
| `Emit` | `batch_id`, `outlet`, `record` | outlet ∈ `Start.outlets`; before the batch's ack |
| `Ack` | `batch_id` | exactly one per batch, or a `Fail` |
| `Fail` | `batch_id`, `error` | fails the stream |

### Batch lifecycle (sidecar side)

```
Assembled ──send──► InFlight ──Ack──► Producing ──all confirmed──► Committed
                      │  │                 │
                      │  └──Fail/violation──┴──produce failure──► Failed  (stream fails; nothing committed)
                      └──partition revoked──► Revoked (ack/emits dropped; nothing committed)
```

A `Failed` batch is redelivered by the rebuilt graph from the last committed offset; a `Revoked`
one by whichever pod owns the partition next. A partition whose oldest uncommitted batch is older
than `FLOW_STALL_WARNING_AFTER` is **stalled**; the state clears when a batch on it commits.

### Conversation lifecycle

```
Connecting ──Discover ok──► Verified ──Run+Start──► Running ──Stop──► Closed
    ▲                          │                       │
    │                          └──refused──► Exit 1    └──stream failure──► Backoff ──► Connecting
```

`ready` exists only in `Running` with every inlet subscribed. `alive` is touched in every state
but `Exit`.

## Descriptor file (`descriptor.json`)

`Spec` in canonical JSON (contracts/descriptor.md). Written by an SDK; read by the CLI (as a
descriptor to verify against), embedded by the CLI into the resource, mounted by the operator into
the sidecar, compared by the sidecar with the discovered `Spec`. Identity: `streamlet.name`,
unique within a `--descriptors` directory.

## Blueprint (`blueprint/`, package `com.thinkmorestupidless.ankka.flow.blueprint`)

Carried from `cloudflow-blueprint` with the changes in research R6.

| type | fields | from |
|---|---|---|
| `Blueprint` | `name: Option[String]`, `streamlets: Vector[StreamletRef]`, `topics: Vector[Topic]`, `descriptors: Map[String, Spec]`, `globalProblems` | `Blueprint.parseConfig` |
| `StreamletRef` | `name`, `descriptorName`, `problems`, `verified: Option[VerifiedStreamlet]` | `blueprint.streamlets` |
| `Topic` | `id`, `producers: Vector[String]`, `consumers: Vector[String]`, `cluster: Option[String]`, `kafkaConfig: Config`, `problems`, `verified: Option[VerifiedTopic]` | `blueprint.topics.<id>` |
| `VerifiedBlueprint` | `streamlets: Vector[VerifiedStreamlet]`, `topics: Vector[VerifiedTopic]` | `Blueprint.verified` |
| `VerifiedStreamlet` | `name`, `descriptor: StreamletDescriptor`, `inlets: Vector[VerifiedInlet]`, `outlets: Vector[VerifiedOutlet]` | |
| `VerifiedPortPath` | `streamlet`, `port` | parses `router.in` |
| `VerifiedTopic` | `id`, `name` (Kafka name), `managed: Boolean`, `cluster: Option[String]`, `producers: Vector[VerifiedOutlet]`, `consumers: Vector[VerifiedInlet]`, `settings: TopicSettings` | |
| `TopicSettings` | `partitions: Option[Int]`, `replicas: Option[Short]`, `bootstrapServers: Option[String]`, `connectionConfig`, `producerConfig`, `consumerConfig: Map[String, String]`, `topicConfig: Map[String, String]`, `batch: BatchSettings` | precedence merged later by the operator |
| `BatchSettings` | `maxRecords = 100`, `maxBytes = 1 MiB` | `consumer-config.flow.batch.*` |
| `BlueprintProblem` | sealed; `toMessage` | the list in research R6, including the four new ones |

Rules (FR-004, FR-005): a `Topic` must have `managed == false` to have no producers; an unmanaged
topic must have no producers; an inlet appears in exactly one topic's `consumers`; an outlet in
at most one topic's `producers`; producers and consumers on one topic share `format` and
`fingerprint`; managed topics' Kafka name defaults to `<pipeline>.<id>`.

### Deploy-time overrides (`Overrides`, parsed by the CLI from `--conf`)

`flow.topics.<id>` → a partial `TopicSettings` merged over the blueprint's;
`flow.streamlets.<name>.replicas: Int` and `.config.<key>: value` typed by the descriptor's
parameter. Unknown ids and keys are problems.

## Resource (`crd/`, package `com.thinkmorestupidless.ankka.flow.crd`)

fabric8 `CustomResource[AnkkaFlowSpec, AnkkaFlowStatus]`, group
`flow.ankka.thinkmorestupidless.com`, `v1alpha1`, kind `AnkkaFlow`, plural `ankkaflows`, short
`aflow`, namespaced, status subresource. Jackson case classes, `NON_ABSENT`.

| type | fields |
|---|---|
| `AnkkaFlowSpec` | `pipeline: String`, `version: String`, `protocolVersion: String`, `onDelete: OnDelete`, `streamlets: Vector[StreamletSpec]`, `topics: Vector[TopicSpec]` |
| `OnDelete` | `managedTopics: "Keep" \| "Delete"` (default `Keep`) |
| `StreamletSpec` | `name`, `image`, `replicas: Int` (default 1, ≥ 0), `config: Map[String, JsonNode]`, `inlets: Map[String, String]` (port → topic id), `outlets: Map[String, String]`, `descriptor: JsonNode` (the canonical `streamlet` object) |
| `TopicSpec` | `id`, `name`, `managed: Boolean`, `cluster: Option[String]`, `bootstrapServers: Option[String]`, `partitions: Option[Int]`, `replicas: Option[Int]`, `connectionConfig`, `producerConfig`, `consumerConfig`, `topicConfig: Map[String, String]` |
| `AnkkaFlowStatus` | `observedGeneration: Long`, `phase: Pending \| Ready \| Degraded \| Failed`, `detail: String`, `lastTransitionTime: String`, `streamlets: Vector[StreamletStatus]`, `topics: Vector[TopicStatus]` |
| `StreamletStatus` | `name`, `desired: Int`, `ready: Int`, `detail` |
| `TopicStatus` | `id`, `exists: Boolean`, `detail` |

Annotations on the resource: `flow.ankka.thinkmorestupidless.com/reset-offsets` =
`ResetRequest{id: String, streamlets: List[String]}` as JSON; `…/reset-offsets-done` = the id
last completed. `pending = request.exists(r => !done.contains(r.id))`.

### Pipeline phase transitions

```
(created) ─► Pending ─► Ready ◄─► Degraded
              │  ▲        │
              │  └────────┘ (a change rolls a streamlet)
              └─► Failed (refusal; leaves when the resource or settings change and render succeeds)
```

`Ready` iff every streamlet `ready == desired` and every topic `exists`. `Degraded` iff some
streamlet `ready < desired` after a rollout has settled, or an unmanaged topic is missing.
`Pending` while a rollout is in progress. `Failed` iff `render` returned problems.

## Operator (`operator/`, package `com.thinkmorestupidless.ankka.flow.operator`)

| type | role |
|---|---|
| `Settings` | the table in contracts/resource-and-operator.md; property → env → default |
| `PipelineRef(namespace, name)` | the work-queue item |
| `KafkaCluster(name, bootstrapServers, connectionConfig, producerConfig, consumerConfig, partitions: Option[Int], replicas: Option[Int])` | read from `kafka-cluster-<name>` |
| `Observed(deployments: Map[String, Deployment], pods: Map[String, Int], topics: Map[String, Option[TopicDescription]], clusters: Map[String, KafkaCluster], events: …)` | the executor's snapshot `render` reads |
| `ResolvedTopic(id, name, managed, bootstrapServers, connectionConfig, producerConfig, consumerConfig, partitions, replicas, topicConfig)` | after cluster resolution; refusal if a managed one lacks partitions or replicas |
| `StreamletFiles(descriptorJson: String, streamletConf: String)` | the Secret's data; its SHA-256 is the config hash |
| `Action` | `EnsureTopic(ResolvedTopic)`, `EnsureSecret`, `EnsureServiceAccount`, `EnsureRole`, `EnsureRoleBinding`, `ApplyDeployment`, `DeleteDeployment(ns, name)`, `DeleteSecret`, `ResetGroup(ResetTarget)`, `MarkResetDone(id)`, `RecordEvent(reason, type, note)`, `SetStatus(AnkkaFlowStatus)`, `NoAction` |
| `ResetTarget(streamlet, inlet, groupId, topic: ResolvedTopic)` | carried from Cloudflow's `Target` |
| `Executor` | `execute(Action)`, `observe(PipelineRef): Observed`; `Fabric8Executor` + `KafkaExecutor` (Admin cache keyed by bootstrap + connection config) |

## Sidecar (`sidecar/`, package `com.thinkmorestupidless.ankka.flow.sidecar`)

| type | role |
|---|---|
| `Settings` | the env table in contracts/sidecar.md |
| `StreamletConfig` | `streamlet.conf` parsed: `pipeline`, `streamlet`, `config: Config`, `inlets: Map[String, InletConfig]`, `outlets: Map[String, OutletConfig]` |
| `InletConfig` | `topic`, `group`, `clientId`, `bootstrapServers`, `connectionConfig`, `consumerConfig`, `batch: BatchSettings` |
| `OutletConfig` | `topic`, `clientId`, `bootstrapServers`, `connectionConfig`, `producerConfig` |
| `Descriptor` | the deployed `Spec`; `Descriptor.compare(deployed, discovered): Vector[Problem]` |
| `InputBatch(id, inlet, partition, records: Vector[(InputRecord, CommittableOffset)])` | assembled by `groupedWeightedWithin` |
| `Outcome` | `Acked(emits: Vector[EmittedRecord]) \| Failed(error)` |
| `EmittedRecord(outlet, record)` | |
| `BatchProcessor` | `process(InputBatch): Future[Outcome]`; `RemoteProcessor` (over `Run`), `DoubleProcessor` (tests); the seam for FR-018 |
| `Conversation` | one `Run` stream: `start(Start)`, `send(Batch): Future[Outcome]`, `stop()`, `failed: Future[Throwable]`; correlates by `batch_id`, buffers emits, detects violations |
| `Supervisor` | the state machine in *Conversation lifecycle*: discovery, build graph, watch `Conversation.failed`, tear down, back off, repeat; owns `ready`/`alive` |
| `InletGraph` | per inlet: `committablePartitionedSource` → batches → processor → produce → commit |
| `Producers` | one `SendProducer` per outlet |
| `Stalls` | per (inlet, partition) oldest uncommitted batch time; MBeans; the one-shot warning |
| `EventSink` | `KubernetesEventSink` (raw API call) or `LogEventSink` |

## CLI (`cli/`, package `com.thinkmorestupidless.ankka.flow.cli`)

| type | role |
|---|---|
| `Verify`, `Generate`, `Reset`, `Version` | decline `Opts`; each returns `Either[Vector[String], Output]` |
| `Descriptors.load(dir): Either[problems, Map[String, Spec]]` | reads and validates every `*.json` |
| `Images` | from `--images` and `--image` |
| `ResourceWriter.write(VerifiedBlueprint, Overrides, Images, pipeline, version, namespace): AnkkaFlow` | the only place a resource is built |
| `ResetGuards.check(resource, pods): Either[problems, ResetRequest]` | Cloudflow's guards |

## Python SDK values (`ankka_flow`)

| type | fields |
|---|---|
| `Streamlet` | class attrs `name`, `description`, ports, parameters; `process(batch) -> Iterable[Emit]`; `config: Config` set on `Start` |
| `JsonInlet(name, schema_name)`, `JsonOutlet(name, schema_name)` | `contract` computed; `outlet.emit(record \| value=, key=, headers=) -> Emit` |
| `StringParameter(key, default=, description=)` and the five others | typed access via `config[param]` |
| `Record(key, headers, value, offset, timestamp_ms)` | frozen |
| `Batch(inlet, partition, records)` | iterable |
| `Emit(outlet, record)` | |
| `Harness(streamlet, config=)` | `inlet(name).put(...)`, `run(partitions=)`, `outlet(name).records`, `skipped`, `failures` |
