package com.thinkmorestupidless.ankka.flow.blueprint

import scala.jdk.CollectionConverters.*

import com.typesafe.config.{Config, ConfigValueType}

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
    topicConfig: Map[String, String] = Map.empty
)

object TopicSettings:

  def fromConfig(c: Config): TopicSettings =
    def opt[A](key: String)(get: String => A): Option[A] =
      if c.hasPath(key) then Some(get(key)) else None
    TopicSettings(
      partitions = opt(Blueprint.PartitionsKey)(c.getInt),
      replicas = opt(Blueprint.ReplicasKey)(c.getInt),
      bootstrapServers = opt(Blueprint.BootstrapServersKey)(c.getString),
      connectionConfig = props(c, Blueprint.ConnectionConfigKey),
      producerConfig = props(c, Blueprint.ProducerConfigKey),
      consumerConfig = props(c, Blueprint.ConsumerConfigKey),
      topicConfig = props(c, Blueprint.TopicConfigKey) - "name"
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
