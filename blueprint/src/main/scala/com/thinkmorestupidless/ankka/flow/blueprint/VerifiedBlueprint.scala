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

import com.typesafe.config.Config

/** Carried from Cloudflow's `cloudflow.blueprint.VerifiedBlueprint` (core/cloudflow-blueprint). */
final case class VerifiedBlueprint(
    name: Option[String],
    streamlets: Vector[VerifiedStreamlet],
    topics: Vector[VerifiedTopic]
)

object VerifiedPortPath:
  def apply(portPath: String): Either[PortPathError, VerifiedPortPath] =
    val trimmed = portPath.trim()
    val parts   = trimmed.split("\\.").filterNot(_.isEmpty).toVector
    if trimmed.startsWith(".") then Left(InvalidPortPath(portPath))
    else if parts.size >= 2 then
      val portName          = parts.last
      val streamletNamePart = parts.init
      val streamletRef      = streamletNamePart.mkString(".")
      if streamletRef.isEmpty then Left(InvalidPortPath(portPath))
      else if portName.isEmpty then Left(InvalidPortPath(portPath))
      else Right(VerifiedPortPath(streamletRef, portName))
    else Left(InvalidPortPath(portPath))

final case class VerifiedPortPath(streamletRef: String, portName: String):
  override def toString = s"$streamletRef.$portName"

final case class VerifiedStreamlet(name: String, descriptor: StreamletDescriptor)

/**
 * A verified topic. `managed` and `kafkaName` are new: a managed topic's Kafka name defaults to
 * `<pipeline>.<id>` so two pipelines' `valid-carts` never collide (research R6).
 */
final case class VerifiedTopic(
    id: String,
    connections: Vector[VerifiedPort],
    cluster: Option[String],
    kafkaConfig: Config
):
  def managed: Boolean =
    !kafkaConfig.hasPath(Blueprint.ManagedKey) || kafkaConfig.getBoolean(Blueprint.ManagedKey)
  def producers: Vector[VerifiedPort] = connections.filter(_.isOutlet)
  def consumers: Vector[VerifiedPort] = connections.filterNot(_.isOutlet)
  def kafkaName(pipeline: String): String =
    if kafkaConfig.hasPath(Blueprint.TopicKey) then kafkaConfig.getString(Blueprint.TopicKey)
    else if managed then s"$pipeline.$id"
    else id

sealed trait VerifiedPort:
  def streamlet: VerifiedStreamlet
  def portName: String
  def schemaDescriptor: SchemaDescriptor
  def portPath: VerifiedPortPath
  def isOutlet: Boolean

object VerifiedPort:
  def findPort(
      verifiedPortPath: VerifiedPortPath,
      verifiedStreamlets: Vector[VerifiedStreamlet]
  ): Either[PortPathError, VerifiedPort] =
    val portPath = verifiedPortPath.toString
    verifiedStreamlets
      .find(_.name == verifiedPortPath.streamletRef)
      .toRight(PortPathNotFound(portPath))
      .flatMap { verifiedStreamlet =>
        val ports = verifiedStreamlet.descriptor.outlets ++ verifiedStreamlet.descriptor.inlets
        ports
          .find(port => port.name == verifiedPortPath.portName)
          .map { portDescriptor =>
            if portDescriptor.isOutlet then
              VerifiedOutlet(verifiedStreamlet, portDescriptor.name, portDescriptor.schema)
            else VerifiedInlet(verifiedStreamlet, portDescriptor.name, portDescriptor.schema)
          }
          .toRight(
            PortPathNotFound(
              portPath,
              ports.map(p => VerifiedPortPath(verifiedStreamlet.name, p.name)).sortBy(_.toString)
            )
          )
      }

  def collectPorts(
      verifiedPortPaths: Vector[VerifiedPortPath],
      verifiedStreamlets: Vector[VerifiedStreamlet]
  ): Either[Vector[PortPathError], Vector[VerifiedPort]] =
    val results = verifiedPortPaths.map(p => VerifiedPort.findPort(p, verifiedStreamlets))
    val errors  = results.collect { case Left(e) => e }
    if errors.isEmpty then Right(results.collect { case Right(p) => p }) else Left(errors)

final case class VerifiedInlet(
    streamlet: VerifiedStreamlet,
    portName: String,
    schemaDescriptor: SchemaDescriptor
) extends VerifiedPort:
  def portPath = VerifiedPortPath(streamlet.name, portName)
  def isOutlet = false

final case class VerifiedOutlet(
    streamlet: VerifiedStreamlet,
    portName: String,
    schemaDescriptor: SchemaDescriptor
) extends VerifiedPort:
  def portPath = VerifiedPortPath(streamlet.name, portName)
  def isOutlet = true
