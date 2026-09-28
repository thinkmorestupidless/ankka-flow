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
 * only the cluster knows: the sidecar image (the operator's own setting) and Kafka cluster secrets.
 * Every field has a default so an older operator reading a newer resource sees a missing field as
 * its default.
 */
@JsonInclude(JsonInclude.Include.NON_ABSENT)
final case class AnkkaFlowSpec(
    pipeline: String = "",
    version: String = "",
    /** The protocol version the descriptors were written against, "MAJOR.MINOR". */
    protocolVersion: String = "",
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
    replicas: Int = 1,
    /** Every declared parameter, resolved and typed. */
    config: Map[String, JsonNode] = Map.empty,
    /** Port name to topic id. */
    inlets: Map[String, String] = Map.empty,
    outlets: Map[String, String] = Map.empty,
    /** The descriptor's `streamlet` object, verbatim, with its snake_case keys (DESCRIPTOR.md). */
    descriptor: JsonNode = null
)

@JsonInclude(JsonInclude.Include.NON_ABSENT)
final case class TopicSpec(
    id: String = "",
    /** The Kafka topic's name. */
    name: String = "",
    managed: Boolean = true,
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
    topicConfig: Map[String, String] = Map.empty,
    /** Batching for inlets reading this topic (research R10). */
    batch: BatchSpec = BatchSpec()
)

@JsonInclude(JsonInclude.Include.NON_ABSENT)
final case class BatchSpec(maxRecords: Int = 100, maxBytes: Long = 1024L * 1024)

@JsonInclude(JsonInclude.Include.NON_ABSENT)
final case class AnkkaFlowStatus(
    observedGeneration: Long = 0L,
    /** Pending | Ready | Degraded | Failed. */
    phase: String = "",
    detail: String = "",
    lastTransitionTime: String = "",
    streamlets: List[StreamletStatus] = Nil,
    topics: List[TopicStatus] = Nil
):
  /** Equal but for the transition time: an unchanged report is not written again. */
  def sameReport(other: AnkkaFlowStatus): Boolean =
    copy(lastTransitionTime = "") == other.copy(lastTransitionTime = "")

object AnkkaFlowStatus:
  val Pending  = "Pending"
  val Ready    = "Ready"
  val Degraded = "Degraded"
  val Failed   = "Failed"

@JsonInclude(JsonInclude.Include.NON_ABSENT)
final case class StreamletStatus(
    name: String = "",
    desired: Int = 0,
    ready: Int = 0,
    detail: String = ""
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
