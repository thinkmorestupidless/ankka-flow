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

/**
 * Everything verification can find wrong, carried from Cloudflow's
 * `cloudflow.blueprint.BlueprintProblem`
 * (core/cloudflow-blueprint/src/main/scala/cloudflow/blueprint/BlueprintProblem.scala). Class
 * names, volume mounts and config-parameter validation are gone: a streamlet is named by its
 * descriptor, has no volumes, and its parameters are validated with the descriptor (protocol
 * `DescriptorValidation`). New: formats this version does not support, unmanaged topics with
 * producers or without brokers, streamlets without an image, and unconnected outlets reported as
 * notes, not refusals.
 */
sealed trait BlueprintProblem

object BlueprintProblem:
  def toMessage(problem: BlueprintProblem): String =
    problem match
      case BlueprintFormatError(reason) =>
        s"The blueprint file has an invalid format:\n $reason"
      case MissingStreamletsSection =>
        s"The blueprint.streamlets section is missing in the blueprint file."
      case DuplicateStreamletNamesFound(streamlets) =>
        val duplicates =
          streamlets.map(s => s"(name: ${s.name}, descriptor: ${s.descriptorName})").mkString(", ")
        s"Duplicate streamlet names detected: $duplicates."
      case EmptyStreamletDescriptors =>
        s"The streamlet descriptor list is empty."
      case EmptyStreamlets =>
        s"The application blueprint is empty."
      case IncompatibleSchema(path, otherPath, schema, otherSchema) =>
        s"'$path' ($schema) is not compatible with '$otherPath' ($otherSchema)."
      case UnsupportedFormat(path, format) =>
        s"'$path' uses format '$format', which this version does not support; the only contract format is json."
      case InvalidPortPath(path) =>
        s"'$path' is not a valid path to an outlet or an inlet."
      case InvalidTopicName(topicName) =>
        s"'$topicName' is not a valid topic name, must match '${Topic.LegalTopicChars}', max ${Topic.MaxLength} characters."
      case InvalidProducerPortPath(topic, path) =>
        s"'$path' is not a valid producer for topic '$topic', must be an outlet."
      case InvalidConsumerPortPath(topic, path) =>
        s"'$path' is not a valid consumer for topic '$topic', must be an inlet."
      case InvalidStreamletName(streamletRef) =>
        s"Invalid streamlet name '$streamletRef'. Names must consist of lower case alphanumeric characters and may contain '-' except for at the start or end."
      case PortBoundToManyTopics(path, topics) =>
        s"'$path' is bound to more than one topic: ${topics.mkString(",")}."
      case PortPathNotFound(path, suggestions) =>
        val end =
          if suggestions.nonEmpty then
            s""", please try ${suggestions.map(_.toString).mkString(" or ")}."""
          else "."
        s"'$path' does not point to a known streamlet inlet or outlet$end"
      case InvalidKafkaClusterName(name) =>
        s"Invalid Kafka cluster name '$name'. Names must consist of lower case alphanumeric characters and may contain '-' except for at the start or end."
      case StreamletDescriptorNotFound(streamletRef, descriptorName) =>
        s"Streamlet '$streamletRef' names descriptor '$descriptorName', which no descriptor declares."
      case UnmanagedTopicHasProducers(topic, paths) =>
        s"Topic '$topic' is not managed but has producers ${paths.mkString(", ")}; the platform only reads topics it does not own."
      case UnmanagedTopicWithoutBrokers(topic) =>
        s"Topic '$topic' is not managed and names no bootstrap.servers or cluster."
      case MissingImage(streamlet) =>
        s"Streamlet '$streamlet' has no image."
      case UnknownBuiltin(streamletRef, descriptorName, known) =>
        val list = if known.isEmpty then "none" else known.mkString(", ")
        s"Streamlet '$streamletRef' names built-in descriptor '$descriptorName', which this version does not have; the built-ins are: $list."
      case BuiltinHasImage(streamlet) =>
        s"Streamlet '$streamlet' is built in and takes no image."
      case UnconnectedInlets(unconnectedInlets) =>
        val list = unconnectedInlets.map(ui => s"${ui.streamletRef}.${ui.port.name}").mkString(",")
        if unconnectedInlets.size > 1 then s"Inlets ($list) are not connected."
        else s"Inlet $list is not connected."
      case UnconnectedOutlets(unconnectedOutlets) =>
        val list = unconnectedOutlets.map(ui => s"${ui.streamletRef}.${ui.port.name}").mkString(",")
        if unconnectedOutlets.size > 1 then s"Outlets ($list) are not connected."
        else s"Outlet $list is not connected."

final case class DuplicateStreamletNamesFound(streamlets: immutable.IndexedSeq[StreamletRef])
    extends BlueprintProblem
case object EmptyStreamlets                           extends BlueprintProblem
case object EmptyStreamletDescriptors                 extends BlueprintProblem
case object MissingStreamletsSection                  extends BlueprintProblem
final case class BlueprintFormatError(reason: String) extends BlueprintProblem

sealed trait PortProblem extends BlueprintProblem:
  def path: VerifiedPortPath

final case class IncompatibleSchema(
    path: VerifiedPortPath,
    otherPath: VerifiedPortPath,
    schema: SchemaDescriptor,
    otherSchema: SchemaDescriptor
) extends PortProblem
final case class UnsupportedFormat(path: VerifiedPortPath, format: String) extends PortProblem

final case class InvalidTopicName(topicName: String) extends BlueprintProblem

sealed trait PortPathError                     extends BlueprintProblem
final case class InvalidPortPath(path: String) extends BlueprintProblem with PortPathError
final case class InvalidProducerPortPath(topic: String, path: String)
    extends BlueprintProblem
    with PortPathError
final case class InvalidConsumerPortPath(topic: String, path: String)
    extends BlueprintProblem
    with PortPathError
final case class PortPathNotFound(
    path: String,
    suggestions: immutable.IndexedSeq[VerifiedPortPath] = immutable.IndexedSeq.empty
) extends PortPathError
final case class PortBoundToManyTopics(path: String, topics: immutable.IndexedSeq[String])
    extends PortPathError

final case class InvalidKafkaClusterName(name: String) extends BlueprintProblem

final case class InvalidStreamletName(streamletRef: String) extends BlueprintProblem

final case class StreamletDescriptorNotFound(streamletRef: String, descriptorName: String)
    extends BlueprintProblem

final case class UnmanagedTopicHasProducers(topic: String, paths: immutable.IndexedSeq[String])
    extends BlueprintProblem
final case class UnmanagedTopicWithoutBrokers(topic: String) extends BlueprintProblem
final case class MissingImage(streamlet: String)             extends BlueprintProblem

/** A `builtin/<name>` this version does not ship; `known` lists the built-ins that exist. */
final case class UnknownBuiltin(
    streamletRef: String,
    descriptorName: String,
    known: immutable.IndexedSeq[String]
) extends BlueprintProblem

/** An image given for a streamlet whose descriptor is built in: its pod has only the sidecar. */
final case class BuiltinHasImage(streamlet: String) extends BlueprintProblem

sealed trait UnconnectedPorts extends BlueprintProblem:
  def nonEmpty: Boolean
final case class UnconnectedInlets(unconnectedInlets: immutable.IndexedSeq[UnconnectedPort])
    extends UnconnectedPorts:
  def nonEmpty: Boolean = unconnectedInlets.nonEmpty
final case class UnconnectedPort(streamletRef: String, port: PortDescriptor)
final case class UnconnectedOutlets(unconnectedOutlets: immutable.IndexedSeq[UnconnectedPort])
    extends UnconnectedPorts:
  def nonEmpty: Boolean = unconnectedOutlets.nonEmpty
