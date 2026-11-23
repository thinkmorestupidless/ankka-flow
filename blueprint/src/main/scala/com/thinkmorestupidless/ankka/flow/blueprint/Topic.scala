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

import scala.util.Try

import com.thinkmorestupidless.ankka.flow.descriptor.Contract
import com.typesafe.config.*

object Topic:
  val LegalTopicChars   = "[a-zA-Z0-9\\._\\-]"
  val LegalTopicPattern = s"$LegalTopicChars+".r
  val MaxLength         = 249

/**
 * A topic and the streamlet inlets and outlets that connect to it. Carried from Cloudflow's
 * `cloudflow.blueprint.Topic`
 * (core/cloudflow-blueprint/src/main/scala/cloudflow/blueprint/Topic.scala). Two ports connect when
 * their contracts do (`Contract.connects`): the same name and, where both state one, the same
 * fingerprint. The Avro and Protobuf branches are not carried. Unmanaged topics are checked here
 * too: consumers only, and brokers or a cluster named (FR-005).
 */
final case class Topic(
    id: String,
    producers: Vector[String] = Vector.empty[String],
    consumers: Vector[String] = Vector.empty[String],
    cluster: Option[String] = None,
    kafkaConfig: Config = ConfigFactory.empty(),
    problems: Vector[BlueprintProblem] = Vector.empty[BlueprintProblem],
    verified: Option[VerifiedTopic] = None,
    ankka: Option[String] = None
):
  import Topic.*

  private val ankkaRef = ankka.flatMap(AnkkaTopicRef.parse)

  def name: String =
    ankkaRef
      .map(_.kafkaName)
      .getOrElse(Try(kafkaConfig.getString(Blueprint.TopicKey)).getOrElse(id))

  def managed: Boolean =
    ankka.isEmpty && Try(kafkaConfig.getBoolean(Blueprint.ManagedKey)).getOrElse(true)

  def verify(verifiedStreamlets: Vector[VerifiedStreamlet]): Topic =
    val invalidTopicError = name match
      case LegalTopicPattern() =>
        if name.size > MaxLength then Vector(InvalidTopicName(name))
        else Vector.empty[BlueprintProblem]
      case _ => Vector(InvalidTopicName(name))

    val patternErrors =
      (producers ++ consumers).flatMap(port => VerifiedPortPath(port).left.toOption)
    val verifiedProducerPaths = producers.flatMap(producer => VerifiedPortPath(producer).toOption)
    val verifiedConsumerPaths = consumers.flatMap(consumer => VerifiedPortPath(consumer).toOption)
    val verifiedProducerPortsResult =
      VerifiedPort.collectPorts(verifiedProducerPaths, verifiedStreamlets)
    val verifiedConsumerPortsResult =
      VerifiedPort.collectPorts(verifiedConsumerPaths, verifiedStreamlets)

    val portPathErrors =
      verifiedProducerPortsResult.left.toOption.getOrElse(Vector.empty[PortPathError]) ++
        verifiedConsumerPortsResult.left.toOption.getOrElse(Vector.empty[PortPathError])

    // producers must be outlets, consumers inlets. Computed from the ports that were found only:
    // Cloudflow derived these from the failed lookup too, and reported each unknown path twice.
    val producerErrors = verifiedProducerPortsResult.toOption.toVector.flatten
      .filterNot(_.isOutlet)
      .map(p => InvalidProducerPortPath(name, p.portPath.toString))
    val consumerErrors = verifiedConsumerPortsResult.toOption.toVector.flatten
      .filter(_.isOutlet)
      .map(p => InvalidConsumerPortPath(name, p.portPath.toString))

    val clusterNameError = cluster.flatMap { clusterName =>
      if NameUtils.isDnsLabelCompatible(clusterName) then None
      else Some(InvalidKafkaClusterName(clusterName))
    }.toVector

    // An ankka topic is ankka's: it says neither where it lives nor that the pipeline owns it.
    val ankkaErrors = ankka.toVector.flatMap { value =>
      Vector(
        Option.when(ankkaRef.isEmpty)(InvalidAnkkaTopic(id, value)),
        Option.when(Try(kafkaConfig.getBoolean(Blueprint.ManagedKey)).getOrElse(false))(
          AnkkaTopicManaged(id)
        )
      ).flatten ++
        Vector(Blueprint.TopicKey, Blueprint.BootstrapServersKey)
          .filter(kafkaConfig.hasPath)
          .map(AnkkaTopicNamesKafka(id, _))
    }

    // A topic the platform does not own is only ever read, and must say where it lives.
    val unmanagedErrors =
      if managed || ankka.isDefined then Vector.empty
      else
        Vector(
          Option.when(producers.nonEmpty)(UnmanagedTopicHasProducers(id, producers)),
          Option.when(cluster.isEmpty && !kafkaConfig.hasPath(Blueprint.BootstrapServersKey))(
            UnmanagedTopicWithoutBrokers(id)
          )
        ).flatten

    val verifiedPorts =
      verifiedProducerPortsResult.getOrElse(
        Vector.empty[VerifiedPort]
      ) ++ verifiedConsumerPortsResult.getOrElse(
        Vector.empty[VerifiedPort]
      )
    // Batching was version one's; nothing reads it, so it is refused, not ignored.
    val removedSettings =
      Option
        .when(kafkaConfig.hasPath(s"${Blueprint.ConsumerConfigKey}.flow.batch"))(
          BatchSettingRemoved(id)
        )
        .toVector
    val contractErrors = verifyContracts(verifiedPorts)
    copy(
      problems =
        invalidTopicError ++ patternErrors ++ portPathErrors ++ producerErrors ++ consumerErrors ++ clusterNameError ++
          ankkaErrors ++ unmanagedErrors ++ removedSettings ++ contractErrors,
      verified =
        if verifiedPorts.nonEmpty then
          Some(
            VerifiedTopic(
              id,
              verifiedPorts.distinct.sortBy(_.portPath.toString),
              cluster,
              kafkaConfig,
              ankkaRef
            )
          )
        else None
    )

  val connections = producers ++ consumers

  private def verifyContracts(verifiedPorts: Vector[VerifiedPort]): Vector[BlueprintProblem] =
    verifiedPorts
      .flatMap(port => verifiedPorts.flatMap(otherPort => checkCompatibility(port, otherPort)))
      .map { e =>
        // One problem per pair, whichever way round it was found.
        if e.path.toString <= e.otherPath.toString then e
        else IncompatibleContract(e.otherPath, e.path, e.otherContract, e.contract)
      }
      .distinct

  private def checkCompatibility(
      port: VerifiedPort,
      otherPort: VerifiedPort
  ): Option[IncompatibleContract] =
    if otherPort.portPath != port.portPath && !Contract.connects(port.contract, otherPort.contract)
    then
      Some(
        IncompatibleContract(port.portPath, otherPort.portPath, port.contract, otherPort.contract)
      )
    else None
