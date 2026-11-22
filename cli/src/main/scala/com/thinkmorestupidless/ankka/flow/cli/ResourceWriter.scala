package com.thinkmorestupidless.ankka.flow.cli

import com.fasterxml.jackson.databind.JsonNode
import com.thinkmorestupidless.ankka.flow.blueprint.{
  FlinkOverrides,
  ResourceOverrides,
  TopicSettings,
  VerifiedStreamlet,
  VerifiedTopic
}
import com.thinkmorestupidless.ankka.flow.crd.*
import com.thinkmorestupidless.ankka.flow.descriptor.{Descriptor, DescriptorJson, Json}

/**
 * The only place a resource is built: a verified blueprint, the deploy-time overrides and the
 * images, as the `AnkkaFlow` the operator runs. Applying it needs nothing else (S2.5).
 */
object ResourceWriter:

  private val mapper = FlowSerialization.mapper()

  private def node(json: Json): JsonNode = mapper.readTree(Json.compact(json))

  def write(
      verified: Verify.Verified,
      images: Map[String, String],
      pipeline: String,
      version: String,
      namespace: Option[String],
      onDelete: OnDelete = OnDelete()
  ): AnkkaFlow =
    val v = verified.blueprint
    def portsOf(streamlet: String, outlets: Boolean) =
      v.topics.flatMap { t =>
        (if outlets then t.producers else t.consumers)
          .filter(_.streamlet.name == streamlet)
          .map(p => p.portName -> t.id)
      }.toMap
    val streamlets = v.streamlets.toList.map { s =>
      StreamletSpec(
        name = s.name,
        image = images(s.name),
        engine = s.descriptor.engine.id,
        replicas = verified.replicas(s.name),
        parallelism = verified.parallelism(s.name),
        flink = flink(s, verified.flink(s.name)),
        config = verified.parameters(s.name).map((k, j) => k -> node(j)).toMap,
        inlets = portsOf(s.name, outlets = false),
        outlets = portsOf(s.name, outlets = true),
        descriptor = node(DescriptorJson.streamletToJson(s.descriptor.descriptor.streamlet))
      )
    }
    val topics = v.topics.toList.map(t => topic(t, verified, pipeline))
    val resource = AnkkaFlow(
      namespace.orNull,
      pipeline,
      AnkkaFlowSpec(
        pipeline = pipeline,
        version = version,
        descriptorVersion = Descriptor.Version,
        onDelete = onDelete,
        streamlets = streamlets,
        topics = topics
      )
    )
    resource.setApiVersion(AnkkaFlowDefinition.apiVersion)
    resource.setKind(AnkkaFlowDefinition.kind)
    // A sorted map, not `Map.of`: the JDK's immutable maps iterate in an order salted per JVM
    // run, so the labels came out in either order and two generations of one blueprint could
    // differ byte for byte. A resource is written the same way every time.
    val labels = new java.util.TreeMap[String, String]
    labels.put("app.kubernetes.io/managed-by", "ankka-flow")
    labels.put(s"${AnkkaFlowDefinition.domain}/pipeline", pipeline)
    resource.getMetadata.setLabels(labels)
    resource

  /** A Flink streamlet's job: the descriptor's entry class and version, the deploy's settings. */
  private def flink(s: VerifiedStreamlet, o: Option[FlinkOverrides]): Option[FlinkSpec] =
    s.descriptor.descriptor.streamlet.flink.map { job =>
      def resources(r: Option[ResourceOverrides]) =
        val d = Resources()
        Resources(r.flatMap(_.cpu).getOrElse(d.cpu), r.flatMap(_.memory).getOrElse(d.memory))
      FlinkSpec(
        entryClass = job.entryClass,
        version = job.version,
        checkpointInterval =
          o.flatMap(_.checkpointInterval).getOrElse(FlinkSpec.DefaultCheckpointInterval),
        jobManager = resources(o.map(_.jobManager)),
        taskManager = resources(o.map(_.taskManager))
      )
    }

  private def topic(t: VerifiedTopic, verified: Verify.Verified, pipeline: String): TopicSpec =
    val s = TopicSettings.fromConfig(verified.overrides.topicConfig(t))
    TopicSpec(
      id = t.id,
      name = t.kafkaName(pipeline),
      managed = t.managed,
      cluster = verified.overrides.cluster(t),
      bootstrapServers = s.bootstrapServers,
      partitions = s.partitions,
      replicas = s.replicas,
      connectionConfig = s.connectionConfig,
      producerConfig = s.producerConfig,
      consumerConfig = s.consumerConfig,
      topicConfig = s.topicConfig
    )
