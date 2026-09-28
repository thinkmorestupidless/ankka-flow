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

package com.thinkmorestupidless.ankka.flow.blueprint

import scala.collection.immutable
import scala.jdk.CollectionConverters.*

import com.typesafe.config.*

/**
 * The blueprint: which streamlets a pipeline uses and the topics that connect them. Carried from
 * Cloudflow's `cloudflow.blueprint.Blueprint`
 * (core/cloudflow-blueprint/src/main/scala/cloudflow/blueprint/Blueprint.scala), with its keys and
 * its verification, except that a streamlet names a descriptor instead of a class, the pipeline may
 * be named (`blueprint.name`), and an unconnected outlet is a note, not a problem (spec S2.2).
 */
object Blueprint:
  val NameKey                          = "blueprint.name"
  val StreamletsSectionKey             = "blueprint.streamlets"
  val TopicsSectionKey                 = "blueprint.topics"
  val UnsupportedConnectionsSectionKey = "blueprint.connections"
  val TopicKey                         = "topic.name"
  val ManagedKey                       = "managed"
  val ProducersKey                     = "producers"
  val ConsumersKey                     = "consumers"
  val ClusterKey                       = "cluster"

  // kafka config items
  val BootstrapServersKey = "bootstrap.servers"
  val ConnectionConfigKey = "connection-config"
  val ProducerConfigKey   = "producer-config"
  val ConsumerConfigKey   = "consumer-config"
  val PartitionsKey       = "partitions"
  val ReplicasKey         = "replicas"
  val TopicConfigKey      = "topic"

  /** Parses the blueprint from a String. */
  def parseString(
      blueprintString: String,
      streamletDescriptors: Vector[StreamletDescriptor]
  ): Blueprint =
    try parseConfig(ConfigFactory.parseString(blueprintString).resolve(), streamletDescriptors)
    catch
      case e: ConfigException =>
        Blueprint(globalProblems = Vector(BlueprintFormatError(e.getMessage)))

  /** Parses the blueprint from a Config. */
  def parseConfig(config: Config, streamletDescriptors: Vector[StreamletDescriptor]): Blueprint =
    if !config.hasPath(StreamletsSectionKey) then
      Blueprint(globalProblems = Vector(MissingStreamletsSection))
    else
      try
        val streamletRefs = getKeys(config, StreamletsSectionKey).map { key =>
          StreamletRef(
            name = key,
            descriptorName = config.getString(s"$StreamletsSectionKey.\"$key\"")
          )
        }

        val topics =
          if config.hasPath(TopicsSectionKey) then
            getKeys(config, TopicsSectionKey).map { key =>
              val base      = s"$TopicsSectionKey.\"$key\""
              val producers = getStringListOrEmpty(config, s"$base.$ProducersKey")
              val consumers = getStringListOrEmpty(config, s"$base.$ConsumersKey")
              val cluster   = getStringOrEmpty(config, s"$base.$ClusterKey")
              val kafkaConfig = getConfigOrEmpty(config, base)
                .withoutPath(ProducersKey)
                .withoutPath(ConsumersKey)
                .withoutPath(ClusterKey)
              // validate at least that the config sections are objects and the sizes are numbers.
              if kafkaConfig.hasPath(ConnectionConfigKey) then
                kafkaConfig.getObject(ConnectionConfigKey): Unit
              if kafkaConfig.hasPath(ProducerConfigKey) then
                kafkaConfig.getObject(ProducerConfigKey): Unit
              if kafkaConfig.hasPath(ConsumerConfigKey) then
                kafkaConfig.getObject(ConsumerConfigKey): Unit
              if kafkaConfig.hasPath(PartitionsKey) then kafkaConfig.getInt(PartitionsKey): Unit
              if kafkaConfig.hasPath(ReplicasKey) then kafkaConfig.getInt(ReplicasKey): Unit
              if kafkaConfig.hasPath(ManagedKey) then kafkaConfig.getBoolean(ManagedKey): Unit
              Topic(key, producers, consumers, cluster, kafkaConfig)
            }
          else Vector.empty[Topic]

        // not supporting the pre-topics format
        if config.hasPath(UnsupportedConnectionsSectionKey) then
          throw new ConfigException.BadPath(
            UnsupportedConnectionsSectionKey,
            s"Please specify '$TopicsSectionKey' section instead of '$UnsupportedConnectionsSectionKey'."
          )

        Blueprint(
          name = getStringOrEmpty(config, NameKey),
          streamlets = streamletRefs,
          topics = topics,
          streamletDescriptors = streamletDescriptors
        ).verify
      catch
        case e: ConfigException =>
          Blueprint(globalProblems = Vector(BlueprintFormatError(e.getMessage)))

  private def getKeys(config: Config, key: String): Vector[String] =
    config.getConfig(key).root().keySet().asScala.toVector.sorted

  private def getConfigOrEmpty(config: Config, key: String): Config =
    if config.hasPath(key) then config.getConfig(key) else ConfigFactory.empty()
  private def getStringListOrEmpty(config: Config, key: String): Vector[String] =
    if config.hasPath(key) then config.getStringList(key).asScala.toVector else Vector.empty[String]
  private def getStringOrEmpty(config: Config, key: String): Option[String] =
    if config.hasPath(key) then Option(config.getString(key)) else None

final case class Blueprint(
    name: Option[String] = None,
    streamlets: Vector[StreamletRef] = Vector.empty[StreamletRef],
    topics: Vector[Topic] = Vector.empty[Topic],
    streamletDescriptors: Vector[StreamletDescriptor] = Vector.empty,
    globalProblems: Vector[BlueprintProblem] = Vector.empty[BlueprintProblem],
    notes: Vector[BlueprintProblem] = Vector.empty[BlueprintProblem]
):
  val problems: Vector[BlueprintProblem] =
    globalProblems ++ streamlets.flatMap(_.problems) ++ topics.flatMap(_.problems)

  val isValid: Boolean = problems.isEmpty

  def verify: Blueprint =
    val emptyStreamletsProblem = if streamlets.isEmpty then Some(EmptyStreamlets) else None
    val emptyStreamletDescriptorsProblem =
      if streamletDescriptors.isEmpty then Some(EmptyStreamletDescriptors) else None

    val newStreamlets      = streamlets.map(_.verify(streamletDescriptors))
    val verifiedStreamlets = newStreamlets.flatMap(_.verified)

    val newTopics      = topics.map(_.verify(verifiedStreamlets))
    val verifiedTopics = newTopics.flatMap(_.verified)

    val duplicatesProblem = verifyNoDuplicateStreamletNames(newStreamlets).left.toOption

    val (unconnectedOutlets, unconnectedInlets) =
      verifyPortsConnected(verifiedStreamlets, verifiedTopics).partition(
        _.isInstanceOf[UnconnectedOutlets]
      )
    val portsBoundToManyTopics = verifyPortsBoundToManyTopics(verifiedTopics)
    val globalProblems =
      Vector(emptyStreamletsProblem, emptyStreamletDescriptorsProblem, duplicatesProblem).flatten ++
        unconnectedInlets ++ portsBoundToManyTopics

    copy(
      streamlets = newStreamlets,
      topics = newTopics,
      globalProblems = globalProblems,
      notes = unconnectedOutlets
    )

  def verified: Either[Vector[BlueprintProblem], VerifiedBlueprint] =
    val v = verify
    if v.problems.isEmpty then
      Right(
        VerifiedBlueprint(v.name, v.streamlets.flatMap(_.verified), v.topics.flatMap(_.verified))
      )
    else Left(v.problems)

  private def verifyNoDuplicateStreamletNames(
      streamlets: Vector[StreamletRef]
  ): Either[DuplicateStreamletNamesFound, Vector[StreamletRef]] =
    val dups = streamlets.groupBy(_.name.trim()).values.filter(_.size > 1).flatten.toVector
    if dups.isEmpty then Right(streamlets) else Left(DuplicateStreamletNamesFound(dups))

  private def verifyPortsConnected(
      verifiedStreamlets: Vector[VerifiedStreamlet],
      verifiedTopics: Vector[VerifiedTopic]
  ): Vector[UnconnectedPorts] =
    var problems = Vector.empty[UnconnectedPorts]
    val (outlets, inlets) = verifiedStreamlets
      .flatMap { streamlet =>
        def unconnected(portDescriptors: immutable.IndexedSeq[PortDescriptor]) =
          portDescriptors
            .filterNot { port =>
              verifiedTopics.exists(topic =>
                topic.connections.exists(verifiedPort =>
                  verifiedPort.streamlet == streamlet && verifiedPort.portName == port.name
                )
              )
            }
            .map(port => UnconnectedPort(streamlet.name, port))
        unconnected(streamlet.descriptor.inlets) ++ unconnected(streamlet.descriptor.outlets)
      }
      .partition(_.port.isOutlet)
    if outlets.nonEmpty then problems = problems :+ UnconnectedOutlets(outlets)
    if inlets.nonEmpty then problems = problems :+ UnconnectedInlets(inlets)
    problems

  private def verifyPortsBoundToManyTopics(
      verifiedTopics: Vector[VerifiedTopic]
  ): Vector[PortBoundToManyTopics] =
    verifiedTopics
      .flatMap(verifiedTopic => verifiedTopic.connections.map(_ -> verifiedTopic.id))
      .groupBy { case (verifiedPort, _) => verifiedPort }
      .flatMap { case (verifiedPort, groupedTopicIds) =>
        val topicIds = groupedTopicIds.map { case (_, topic) => topic }
        if topicIds.size > 1 then
          Some(PortBoundToManyTopics(verifiedPort.portPath.toString, topicIds))
        else None
      }
      .toVector
      .sortBy(_.path)
