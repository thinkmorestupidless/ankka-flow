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

import com.thinkmorestupidless.ankka.flow.protocol.Fingerprint
import com.typesafe.config.*

object Topic:
  val LegalTopicChars   = "[a-zA-Z0-9\\._\\-]"
  val LegalTopicPattern = s"$LegalTopicChars+".r
  val MaxLength         = 249

/**
 * A topic and the streamlet inlets and outlets that connect to it. Carried from Cloudflow's
 * `cloudflow.blueprint.Topic`
 * (core/cloudflow-blueprint/src/main/scala/cloudflow/blueprint/Topic.scala). Compatibility is equal
 * format and equal fingerprint and nothing else: the Avro and Protobuf branches are not carried
 * (research R4). Unmanaged topics are checked here too: consumers only, and brokers or a cluster
 * named (FR-005).
 */
final case class Topic(
    id: String,
    producers: Vector[String] = Vector.empty[String],
    consumers: Vector[String] = Vector.empty[String],
    cluster: Option[String] = None,
    kafkaConfig: Config = ConfigFactory.empty(),
    problems: Vector[BlueprintProblem] = Vector.empty[BlueprintProblem],
    verified: Option[VerifiedTopic] = None
):
  import Topic.*

  def name: String = Try(kafkaConfig.getString(Blueprint.TopicKey)).getOrElse(id)

  def managed: Boolean = Try(kafkaConfig.getBoolean(Blueprint.ManagedKey)).getOrElse(true)

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

    // A topic the platform does not own is only ever read, and must say where it lives.
    val unmanagedErrors =
      if managed then Vector.empty
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
    val schemaErrors = verifySchema(verifiedPorts)
    copy(
      problems =
        invalidTopicError ++ patternErrors ++ portPathErrors ++ producerErrors ++ consumerErrors ++ clusterNameError ++
          unmanagedErrors ++ schemaErrors,
      verified =
        if verifiedPorts.nonEmpty then
          Some(
            VerifiedTopic(
              id,
              verifiedPorts.distinct.sortBy(_.portPath.toString),
              cluster,
              kafkaConfig
            )
          )
        else None
    )

  val connections = producers ++ consumers

  private def verifySchema(verifiedPorts: Vector[VerifiedPort]): Vector[BlueprintProblem] =
    val unsupported = verifiedPorts
      .filterNot(_.schemaDescriptor.format == Fingerprint.Format)
      .map(p => UnsupportedFormat(p.portPath, p.schemaDescriptor.format))
    val incompatible = verifiedPorts
      .flatMap(port => verifiedPorts.flatMap(otherPort => checkCompatibility(port, otherPort)))
      .map { e =>
        // One problem per pair, whichever way round it was found.
        if e.path.toString <= e.otherPath.toString then e
        else IncompatibleSchema(e.otherPath, e.path, e.otherSchema, e.schema)
      }
      .distinct
    unsupported.distinct ++ incompatible

  private def checkCompatibility(
      port: VerifiedPort,
      otherPort: VerifiedPort
  ): Option[IncompatibleSchema] =
    if otherPort.portPath != port.portPath then
      val schema      = port.schemaDescriptor
      val otherSchema = otherPort.schemaDescriptor
      if otherSchema.format == schema.format && otherSchema.fingerprint == schema.fingerprint then
        None
      else Some(IncompatibleSchema(port.portPath, otherPort.portPath, schema, otherSchema))
    else None
