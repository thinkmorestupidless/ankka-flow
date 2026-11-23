package com.thinkmorestupidless.ankka.flow.crd

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import io.fabric8.kubernetes.api.model.Namespaced
import io.fabric8.kubernetes.client.CustomResource
import io.fabric8.kubernetes.model.annotation.{Group, Kind, Plural, ShortNames, Version}

/**
 * A pipeline as the CLI emits it and the operator runs it (contracts/resource-and-operator.md).
 *
 * Written by `flow generate`, never by the operator. It says everything that will run except what
 * only the cluster knows: Kafka cluster secrets and the state store. Every field has a default so
 * an older operator reading a newer resource sees a missing field as its default.
 */
@JsonInclude(JsonInclude.Include.NON_ABSENT)
final case class AnkkaFlowSpec(
    pipeline: String = "",
    version: String = "",
    /** The descriptor version the streamlets' descriptors were written against, "MAJOR.MINOR". */
    descriptorVersion: String = "",
    onDelete: OnDelete = OnDelete(),
    streamlets: List[StreamletSpec] = Nil,
    topics: List[TopicSpec] = Nil
)

/**
 * Whether deleting the pipeline deletes the topics it created. Unmanaged topics are never touched.
 */
@JsonInclude(JsonInclude.Include.NON_ABSENT)
final case class OnDelete(managedTopics: String = OnDelete.Keep)

object OnDelete:
  val Keep   = "Keep"
  val Delete = "Delete"

@JsonInclude(JsonInclude.Include.NON_ABSENT)
final case class StreamletSpec(
    name: String = "",
    image: String = "",
    /** The engine the descriptor names: `pekko-streams` or `flink`. */
    engine: String = "",
    /** Pekko Streams: the pods. Flink: 1 runs the job, 0 suspends it. */
    replicas: Int = 1,
    /** Flink only: the job's parallelism; the job's own when absent. */
    @JsonDeserialize(contentAs = classOf[java.lang.Integer])
    parallelism: Option[Int] = None,
    /** Flink only. */
    flink: Option[FlinkSpec] = None,
    /** Every declared parameter, resolved and typed. */
    config: Map[String, JsonNode] = Map.empty,
    /** Port name to topic id. */
    inlets: Map[String, String] = Map.empty,
    outlets: Map[String, String] = Map.empty,
    /** The descriptor's `streamlet` object, verbatim, with its snake_case keys (DESCRIPTOR.md). */
    descriptor: JsonNode = null
)

/** A Flink streamlet's job: from its descriptor, and from the deploy-time configuration. */
@JsonInclude(JsonInclude.Include.NON_ABSENT)
final case class FlinkSpec(
    entryClass: String = "",
    /** The Flink line the job was built against, "MAJOR.MINOR". */
    version: String = "",
    checkpointInterval: String = FlinkSpec.DefaultCheckpointInterval,
    jobManager: Resources = Resources(),
    taskManager: Resources = Resources()
)

object FlinkSpec:
  val DefaultCheckpointInterval = "10s"

/** A job manager's or task manager's resources, as Kubernetes quantities. */
@JsonInclude(JsonInclude.Include.NON_ABSENT)
final case class Resources(cpu: String = "1", memory: String = "1024m")

@JsonInclude(JsonInclude.Include.NON_ABSENT)
final case class TopicSpec(
    id: String = "",
    /** The Kafka topic's name. */
    name: String = "",
    managed: Boolean = true,
    /** The ankka topic this is, `<project>/<name>` in the blueprint. Never managed. */
    ankka: Option[AnkkaRef] = None,
    cluster: Option[String] = None,
    bootstrapServers: Option[String] = None,
    // `contentAs` because Scala's Option[Int] erases to Option[Object]: without it Jackson decodes
    // the number as whatever it likes and the first `.get + 1` fails at run time.
    @JsonDeserialize(contentAs = classOf[java.lang.Integer])
    partitions: Option[Int] = None,
    @JsonDeserialize(contentAs = classOf[java.lang.Integer])
    replicas: Option[Int] = None,
    connectionConfig: Map[String, String] = Map.empty,
    producerConfig: Map[String, String] = Map.empty,
    consumerConfig: Map[String, String] = Map.empty,
    topicConfig: Map[String, String] = Map.empty
)

/** An ankka project's topic, by the names ankka gives it. */
@JsonInclude(JsonInclude.Include.NON_ABSENT)
final case class AnkkaRef(project: String = "", name: String = "")

@JsonInclude(JsonInclude.Include.NON_ABSENT)
final case class AnkkaFlowStatus(
    observedGeneration: Long = 0L,
    /** Pending | Ready | Degraded | Failed. */
    phase: String = "",
    detail: String = "",
    lastTransitionTime: String = "",
    conditions: List[Condition] = Nil,
    streamlets: List[StreamletStatus] = Nil,
    topics: List[TopicStatus] = Nil
):
  /** Equal but for the transition times: an unchanged report is not written again. */
  def sameReport(other: AnkkaFlowStatus): Boolean =
    def untimed(s: AnkkaFlowStatus) =
      s.copy(
        lastTransitionTime = "",
        conditions = s.conditions.map(_.copy(lastTransitionTime = ""))
      )
    untimed(this) == untimed(other)

object AnkkaFlowStatus:
  val Pending  = "Pending"
  val Ready    = "Ready"
  val Degraded = "Degraded"
  val Failed   = "Failed"

/** A condition of the pipeline, as Kubernetes conditions are: `status` is `True` or `False`. */
@JsonInclude(JsonInclude.Include.NON_ABSENT)
final case class Condition(
    `type`: String = "",
    status: String = "",
    reason: String = "",
    message: String = "",
    lastTransitionTime: String = ""
)

object Condition:
  val Ready                    = "Ready"
  val FlinkOperatorMissing     = "FlinkOperatorMissing"
  val StateStoreMissing        = "StateStoreMissing"
  val TopicNotGranted          = "TopicNotGranted"
  val MachineCredentialRefused = "MachineCredentialRefused"
  val StateUnreadable          = "StateUnreadable"

  val True  = "True"
  val False = "False"

@JsonInclude(JsonInclude.Include.NON_ABSENT)
final case class StreamletStatus(
    name: String = "",
    engine: String = "",
    desired: Int = 0,
    ready: Int = 0,
    detail: String = "",
    inlets: List[InletStatus] = Nil
)

/** An inlet's consumer group and how far behind it is; `lag` absent when it is not known. */
@JsonInclude(JsonInclude.Include.NON_ABSENT)
final case class InletStatus(
    name: String = "",
    topic: String = "",
    group: String = "",
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    lag: Option[Long] = None
)

@JsonInclude(JsonInclude.Include.NON_ABSENT)
final case class TopicStatus(id: String = "", exists: Boolean = false, detail: String = "")

/**
 * The custom resource. A plain class with the inherited no-arg constructor, as ankka's: fabric8
 * instantiates it reflectively and populates it through the setters.
 */
@Group("flow.ankka.thinkmorestupidless.com")
@Version("v1alpha1")
@Kind("AnkkaFlow")
@Plural("ankkaflows")
@ShortNames(Array("aflow"))
class AnkkaFlow extends CustomResource[AnkkaFlowSpec, AnkkaFlowStatus] with Namespaced:
  override protected def initSpec(): AnkkaFlowSpec = AnkkaFlowSpec()

  /** Null, deliberately: a resource nothing has reported on is distinguishable from defaults. */
  override protected def initStatus(): AnkkaFlowStatus = null

object AnkkaFlow:
  def apply(namespace: String, name: String, spec: AnkkaFlowSpec): AnkkaFlow =
    val resource = new AnkkaFlow
    resource.setMetadata(
      new io.fabric8.kubernetes.api.model.ObjectMetaBuilder()
        .withNamespace(namespace)
        .withName(name)
        .build()
    )
    resource.setSpec(spec)
    resource
