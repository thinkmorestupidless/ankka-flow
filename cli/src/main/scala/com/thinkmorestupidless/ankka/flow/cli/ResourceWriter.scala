package com.thinkmorestupidless.ankka.flow.cli

import com.fasterxml.jackson.databind.JsonNode
import com.thinkmorestupidless.ankka.flow.blueprint.{DeltaTopics, TopicSettings, VerifiedTopic}
import com.thinkmorestupidless.ankka.flow.crd.*
import com.thinkmorestupidless.ankka.flow.protocol.{DescriptorJson, Json, ProtocolVersion}

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
        image = if s.descriptor.builtin then "" else images(s.name),
        replicas = verified.replicas(s.name),
        config = verified.parameters(s.name).map((k, j) => k -> node(j)).toMap,
        inlets = portsOf(s.name, outlets = false),
        outlets = portsOf(s.name, outlets = true),
        descriptor = node(DescriptorJson.streamletToJson(s.descriptor.proto)),
        builtin = s.descriptor.builtin
      )
    }
    val topics = v.topics.toList.map(t => topic(t, verified, pipeline))
    val resource = AnkkaFlow(
      namespace.orNull,
      pipeline,
      AnkkaFlowSpec(
        pipeline = pipeline,
        version = version,
        protocolVersion = ProtocolVersion.Current.toString,
        onDelete = onDelete,
        streamlets = streamlets,
        topics = topics
      )
    )
    resource.setApiVersion(AnkkaFlowDefinition.apiVersion)
    resource.setKind(AnkkaFlowDefinition.kind)
    resource.getMetadata.setLabels(
      java.util.Map.of(
        "app.kubernetes.io/managed-by",
        "ankka-flow",
        s"${AnkkaFlowDefinition.domain}/pipeline",
        pipeline
      )
    )
    resource

  private def topic(t: VerifiedTopic, verified: Verify.Verified, pipeline: String): TopicSpec =
    val s = TopicSettings.fromConfig(verified.overrides.topicConfig(t))
    // A delta topic the pipeline owns is compacted unless the blueprint or --conf chose a policy:
    // written here, so the resource says what runs and the operator adds nothing.
    val topicConfig = DeltaTopics.decide(t, s.topicConfig) match
      case Some(DeltaTopics.Decision.Compacted) =>
        s.topicConfig + (DeltaTopics.CleanupPolicy -> DeltaTopics.Compact)
      case _ => s.topicConfig
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
      topicConfig = topicConfig,
      batch = BatchSpec(s.batch.maxRecords, s.batch.maxBytes)
    )
