/*
 * Copyright (C) 2016-2026 Lightbend Inc. <https://www.lightbend.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.thinkmorestupidless.ankka.flow.operator

import java.io.StringReader
import java.util.Properties

import scala.jdk.CollectionConverters.*

import com.thinkmorestupidless.ankka.flow.crd.TopicSpec

/** A Kafka cluster: a `kafka-cluster-<name>` Secret in the operator's clusters namespace (R12). */
final case class KafkaCluster(
    name: String,
    bootstrapServers: String,
    connectionConfig: Map[String, String] = Map.empty,
    producerConfig: Map[String, String] = Map.empty,
    consumerConfig: Map[String, String] = Map.empty,
    partitions: Option[Int] = None,
    replicas: Option[Int] = None,
    /** The ankka machine this cluster's credentials are, when the Secret names one. */
    machine: Option[Machine] = None
)

/** An ankka machine, `<organization>/<name>`: ankka admits its groups under its own prefix only. */
final case class Machine(organization: String, name: String):
  def groupPrefix: String       = s"ankka.machine.$organization.$name."
  override def toString: String = s"$organization/$name"

object Machine:
  private val Part    = "[a-z0-9](?:[a-z0-9-]*[a-z0-9])?"
  private val Pattern = s"($Part)/($Part)".r

  def parse(value: String): Option[Machine] = value.trim match
    case Pattern(org, name) => Some(Machine(org, name))
    case _                  => None

object KafkaCluster:
  /**
   * From a Secret's decoded data. `bootstrap.servers` is required; the configs are properties text.
   */
  def fromData(name: String, data: Map[String, String]): Either[String, KafkaCluster] =
    def props(key: String): Map[String, String] =
      data.get(key).fold(Map.empty) { text =>
        val p = new Properties()
        p.load(new StringReader(text))
        p.asScala.toMap
      }
    val machine = data
      .get("machine")
      .map(m =>
        Machine
          .parse(m)
          .toRight(
            s"Secret '${Names.kafkaClusterSecret(name)}' has machine '$m', which is not <organization>/<name>"
          )
      )
    data.get("bootstrap.servers").filter(_.trim.nonEmpty) match
      case None                          => Left(s"Kafka cluster '$name' has no bootstrap.servers")
      case _ if machine.exists(_.isLeft) => Left(machine.get.left.toOption.get)
      case Some(bootstrap) =>
        Right(
          KafkaCluster(
            name,
            bootstrap.trim,
            props("connection-config"),
            props("producer-config"),
            props("consumer-config"),
            data.get("partitions").flatMap(_.trim.toIntOption),
            data.get("replicas").flatMap(_.trim.toIntOption),
            machine.flatMap(_.toOption)
          )
        )

/** A topic with every setting resolved: the resource's, then its cluster's (FR-021). */
final case class ResolvedTopic(
    id: String,
    name: String,
    managed: Boolean,
    bootstrapServers: String,
    connectionConfig: Map[String, String],
    producerConfig: Map[String, String],
    consumerConfig: Map[String, String],
    partitions: Option[Int],
    replicas: Option[Int],
    topicConfig: Map[String, String],
    /** The ankka machine the topic's cluster is, if any. */
    machine: Option[Machine] = None
):
  /** The prefix every group reading this topic has: its machine's, if any. */
  def groupPrefix: Option[String] = machine.map(_.groupPrefix)

  /** The key the Admin client cache uses: same brokers and credentials, same client. */
  def connectionKey: (String, Map[String, String]) = bootstrapServers -> connectionConfig

/**
 * Resolving a topic's connection and sizing. The create-managed and skip-unmanaged rules are
 * carried from Cloudflow's `TopicActions`
 * (core/cloudflow-operator/src/main/scala/cloudflow/operator/action/TopicActions.scala): a managed
 * topic needs partitions and replicas from somewhere, and one that has none is an error pointing at
 * the cluster's defaults; an unmanaged topic is never sized, created or altered.
 */
object TopicResolution:

  def resolve(
      topic: TopicSpec,
      clusters: Map[String, KafkaCluster]
  ): Either[String, ResolvedTopic] =
    // An ankka topic is ankka's: never the pipeline's to create, size or delete.
    val managed = topic.managed && topic.ankka.isEmpty
    val clusterName =
      topic.cluster.orElse(
        Option.when(topic.bootstrapServers.isEmpty)(
          if topic.ankka.isDefined then Names.AnkkaCluster else Names.DefaultCluster
        )
      )
    val cluster = clusterName match
      case None => Right(None)
      case Some(n) =>
        clusters
          .get(n)
          .map(Some(_))
          .toRight(
            s"topic '${topic.id}' uses Kafka cluster '$n', but there is no Secret '${Names.kafkaClusterSecret(n)}'"
          )
    cluster.flatMap { c =>
      val bootstrap  = topic.bootstrapServers.orElse(c.map(_.bootstrapServers))
      val partitions = topic.partitions.orElse(c.flatMap(_.partitions))
      val replicas   = topic.replicas.orElse(c.flatMap(_.replicas))
      bootstrap match
        case None => Left(s"topic '${topic.id}' names no bootstrap.servers and no Kafka cluster")
        case Some(_) if managed && (partitions.isEmpty || replicas.isEmpty) =>
          Left(
            s"managed topic '${topic.id}' has no ${
                if partitions.isEmpty then "partitions" else "replicas"
              }: set it in the blueprint, or as a default in Secret '${Names.kafkaClusterSecret(clusterName.getOrElse(Names.DefaultCluster))}'"
          )
        case Some(b) =>
          Right(
            ResolvedTopic(
              id = topic.id,
              name = topic.name,
              managed = managed,
              bootstrapServers = b,
              connectionConfig =
                c.fold(Map.empty[String, String])(_.connectionConfig) ++ topic.connectionConfig,
              producerConfig =
                c.fold(Map.empty[String, String])(_.producerConfig) ++ topic.producerConfig,
              consumerConfig =
                c.fold(Map.empty[String, String])(_.consumerConfig) ++ topic.consumerConfig,
              partitions = if managed then partitions else None,
              replicas = if managed then replicas else None,
              topicConfig = topic.topicConfig,
              machine = c.flatMap(_.machine)
            )
          )
    }
