# Data Model: Engine Streamlets

Every name is a term of `GLOSSARY.md`. Wire shapes are in `contracts/`.

## Descriptor (`descriptor` module)

```scala
final case class Descriptor(descriptorVersion: String, sdk: Option[SdkInfo], streamlet: StreamletDescriptor)
final case class SdkInfo(name: String, version: String)
final case class StreamletDescriptor(
    name: String,                          // [a-z0-9-]{1,63}
    description: String,
    engine: Engine,
    inlets: Vector[Port], outlets: Vector[Port],
    parameters: Vector[Parameter],
    flink: Option[FlinkJob])               // present exactly when engine is Flink
enum Engine(val id: String): case PekkoStreams extends Engine("pekko-streams"); case Flink extends Engine("flink")
final case class Port(name: String, contract: Contract)
final case class Contract(name: String, fingerprint: Option[String])   // "sha256:<64 hex>"
final case class Parameter(key: String, description: String, kind: ParameterType, default: Option[String])
enum ParameterType: case String, Integer, Double, Boolean, Duration, MemorySize
final case class FlinkJob(entryClass: String, version: String)         // version "2.2"
```

Validation (`DescriptorValidation`), each a named problem:

- `descriptor_version` major is 2; a `protocol_version` field or a missing `engine` is "a
  descriptor must name an engine".
- `engine` is one `flow` knows; otherwise the problem names it and lists the known ones.
- name, port and parameter-key rules as version one; port names unique across inlets and outlets;
  every port has a contract whose name fits ankka's rule and whose fingerprint, if present, is
  `sha256:` and 64 lowercase hex.
- `flink` present with a non-empty `entry_class` and a `version` the operator can map (`2.2`)
  when the engine is Flink; absent otherwise.

## Blueprint (`blueprint` module)

Unchanged: streamlets by reference to a descriptor, topics with producers and consumers.

New on a topic: `ankka: Option[AnkkaTopicRef(project, name)]`, parsed from `"<project>/<name>"`.
Derived: Kafka name `<project>.<name>`; `managed = false`; `cluster` defaults to the overrides'
`flow.ankka.cluster`, else `ankka`.

New problems: `AnkkaTopicManaged`, `AnkkaTopicNamesKafka` (with `topic.name` or
`bootstrap.servers`), `InvalidAnkkaTopic` (not `project/name`), `AnkkaTopicNotDeclared(project,
name, declared)`, `AnkkaTopicWithoutContract`, `AnkkaContractMismatch(port, portContract,
topicContract)`, `UnknownEngine`, `DescriptorWithoutEngine`, `EngineSettingNotAllowed(streamlet,
key, engine)`. Changed: `IncompatibleSchema` becomes `IncompatibleContract` and compares by R4's
rule; `UnmanagedTopicHasProducers` does not apply to an ankka topic. Removed: `UnsupportedFormat`,
`UnknownBuiltin`, `BuiltinHasImage`.

`AnkkaDeclarations(project, topics: Map[name, Option[Contract]])`, read from the exported file.

Overrides: `flow.streamlets.<name>` gains `parallelism` and `flink { checkpoint-interval,
job-manager { cpu, memory }, task-manager { cpu, memory } }`; `flow.ankka.cluster`.
`BatchSettings` and `consumer-config.flow.batch` are removed.

## Resource (`crd` module)

```scala
final case class AnkkaFlowSpec(pipeline, version, descriptorVersion, onDelete, streamlets, topics)
final case class StreamletSpec(
    name, image, engine: String,
    replicas: Int = 1,                       // Pekko Streams: pods. Flink: 1 runs, 0 suspends.
    parallelism: Option[Int],                // Flink only
    flink: Option[FlinkSpec],                // Flink only
    config: Map[String, JsonNode], inlets: Map[String, String], outlets: Map[String, String],
    descriptor: JsonNode)
final case class FlinkSpec(entryClass, version, checkpointInterval: String = "10s",
    jobManager: Resources = Resources("1", "1024m"), taskManager: Resources = Resources("1", "1024m"))
final case class TopicSpec(id, name, managed, ankka: Option[AnkkaRef(project, name)], cluster,
    bootstrapServers, partitions, replicas, connectionConfig, producerConfig, consumerConfig, topicConfig)

final case class AnkkaFlowStatus(observedGeneration, phase, detail, lastTransitionTime,
    conditions: List[Condition], streamlets: List[StreamletStatus], topics: List[TopicStatus])
final case class Condition(`type`, status: String /* True|False */, reason, message, lastTransitionTime)
final case class StreamletStatus(name, engine, desired: Int, ready: Int, detail, inlets: List[InletStatus])
final case class InletStatus(name, topic, group, lag: Option[Long])
```

Removed: `builtin`, `BatchSpec`, `protocolVersion`. Annotations: `reset-offsets`,
`reset-offsets-done` (unchanged), `state-epochs` (JSON object, streamlet to epoch, absent means
1 for every streamlet).

### Phase

| Phase | When |
|---|---|
| `Failed` | the resource is refused: a descriptor problem, an unknown cluster, `FlinkOperatorMissing`, `StateStoreMissing` |
| `Degraded` | a topic is missing or unreachable; `TopicNotGranted`; `MachineCredentialRefused`; a streamlet failed (`StateUnreadable` among the reasons) |
| `Pending` | a streamlet is rolling out, or a Flink job is not yet `STABLE` and `RUNNING` |
| `Ready` | every streamlet is running and none of the above |

### A Flink streamlet's lifecycle, as the operator reads it

| `FlinkDeployment` status | Streamlet |
|---|---|
| absent, or `lifecycleState` `CREATED`/`UPGRADING`/`DEPLOYED`, or JM `DEPLOYING`/`DEPLOYED_NOT_READY` | rolling out |
| `STABLE` and job `RUNNING` | running (`ready = 1`) |
| `SUSPENDED` | stopped (`desired = 0`) |
| `FAILED`, or JM `ERROR` | failed; detail from `status.error`; `StateUnreadable` when R17's text matches |

## Operator

`KafkaCluster` gains `machine: Option[Machine(organization, name)]`, with
`groupPrefix = "ankka.machine.<organization>.<name>."`.

`Names.group(pipeline, streamlet, inlet, cluster)`: `<pipeline>.<streamlet>.<inlet>`, prefixed
with the machine's group prefix on a machine's cluster.

`StateStore(dir, flinkConfig: Map, env: Map)` from the Secret `flink-state`.

`Observed` gains: `flinkCrd: Boolean`, `stateStore: Option[StateStore]`, `flinkDeployments:
Map[streamlet, FlinkState]`, `lag: Map[(streamlet, inlet), Option[Long]]`, `grants:
Map[topicId, GrantState]` (`Granted | Lacks(right) | CredentialRefused | Unknown`),
`stateEpochs: Map[streamlet, Int]`.

New actions: `ApplyFlinkDeployment(resource)`, `DeleteFlinkDeployment(ns, name)`,
`AdvanceStateEpoch(streamlet)`. Removed with built-ins: nothing else changes in `Action`.

New settings: `FLOW_FLINK_SERVICE_ACCOUNT` (default `flink`), `FLOW_OPERATOR_OBSERVE_SECONDS`
(30). Removed: `FLOW_SIDECAR_IMAGE`.

## SDK types

| Pekko Streams (`…flow.pekko`) | Flink (`…flow.flink`) | Meaning |
|---|---|---|
| `PekkoStreamlet` | `FlinkStreamlet` | the declaration and the logic |
| `Inlet[A]`, `Outlet[A]` | `Inlet<A>`, `Outlet<A>` | a typed port: name, contract, codec |
| `Contract` | `Contract` | name, optional schema resource |
| `Codec[A]` (`json`, `bytes`, `string`) | `Codec<A>` (`json(Class)`, `bytes()`, `string()`) | bytes to a value and back |
| `Record[A]` (key, value, headers) | `Record<A>` (key, value, headers, timestamp) | what a port carries |
| `Emit` | none (Flink's stream) | one record for one outlet |
| `Parameter[A]` | `Parameter<A>` | a typed setting |
| `Context` | `FlinkContext` | parameters; for Flink, `read` and `write` |
| `PekkoStreamlets.run` | `FlinkStreamlets.run` | the main: descriptor, or run |
| `PekkoHarness` | `FlinkHarness` (testkit) | run with no broker |
| `Settings` | `Settings` | `streamlet.properties`, parsed |

A codec that cannot decode a record fails the stream (Pekko Streams) or the job (Flink): never
skip. An author who wants to drop bad records uses `Codec.bytes` and decodes in the logic.
