package com.thinkmorestupidless.ankka.flow.blueprint

import scala.jdk.CollectionConverters.*

import com.typesafe.config.{Config, ConfigFactory, ConfigValueType}

/**
 * Batching per inlet (research R10): `consumer-config.flow.batch.*` in a blueprint topic. A batch
 * is what arrived while the previous one was in flight, capped by these.
 */
final case class BatchSettings(maxRecords: Int, maxBytes: Long)

object BatchSettings:
  val Default: BatchSettings = BatchSettings(100, 1024L * 1024)

/**
 * A topic's settings as the blueprint (and deploy-time overrides) give them. Anything left unset is
 * filled by the operator from the named or default Kafka cluster (FR-021, research R12).
 */
final case class TopicSettings(
    partitions: Option[Int] = None,
    replicas: Option[Int] = None,
    bootstrapServers: Option[String] = None,
    connectionConfig: Map[String, String] = Map.empty,
    producerConfig: Map[String, String] = Map.empty,
    consumerConfig: Map[String, String] = Map.empty,
    topicConfig: Map[String, String] = Map.empty,
    batch: BatchSettings = BatchSettings.Default
)

object TopicSettings:

  def fromConfig(c: Config): TopicSettings =
    def opt[A](key: String)(get: String => A): Option[A] =
      if c.hasPath(key) then Some(get(key)) else None
    val consumer = props(c, Blueprint.ConsumerConfigKey)
    TopicSettings(
      partitions = opt(Blueprint.PartitionsKey)(c.getInt),
      replicas = opt(Blueprint.ReplicasKey)(c.getInt),
      bootstrapServers = opt(Blueprint.BootstrapServersKey)(c.getString),
      connectionConfig = props(c, Blueprint.ConnectionConfigKey),
      producerConfig = props(c, Blueprint.ProducerConfigKey),
      consumerConfig = consumer.filterNot(_._1.startsWith("flow.")),
      topicConfig = props(c, Blueprint.TopicConfigKey) - "name",
      batch = batch(c)
    )

  /** A HOCON block as flat Kafka properties (`security.protocol`, `retention.ms`, …). */
  def props(c: Config, key: String): Map[String, String] =
    if !c.hasPath(key) then Map.empty
    else
      c.getConfig(key)
        .entrySet
        .asScala
        .iterator
        .map { e =>
          val v = e.getValue
          e.getKey.replace("\"", "") -> (if v.valueType == ConfigValueType.STRING then
                                           v.unwrapped.toString
                                         else v.render)
        }
        .toMap

  private def batch(c: Config): BatchSettings =
    val key = s"${Blueprint.ConsumerConfigKey}.flow.batch"
    val b   = if c.hasPath(key) then c.getConfig(key) else ConfigFactory.empty()
    val d   = BatchSettings.Default
    BatchSettings(
      maxRecords = if b.hasPath("max-records") then b.getInt("max-records") else d.maxRecords,
      maxBytes = if b.hasPath("max-bytes") then b.getMemorySize("max-bytes").toBytes else d.maxBytes
    )
