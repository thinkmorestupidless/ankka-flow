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

package com.thinkmorestupidless.ankka.flow.crd

import scala.jdk.CollectionConverters.*
import scala.util.Try

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * A request to reset the consumer groups of a pipeline's streamlets to the earliest offsets of the
 * topics they read, so the pipeline reprocesses its inputs from the start. Carried from Cloudflow's
 * `cloudflow.crd.ResetOffsets`
 * (core/cloudflow-crd/src/main/scala/cloudflow/crd/ResetOffsets.scala).
 *
 * The CLI records a request as an annotation on the `AnkkaFlow`; the operator, which already holds
 * the Kafka connection each streamlet uses, carries it out and records the request's id as done on
 * a second annotation. So a request is carried out once, not again when the operator restarts, and
 * a new request is a new id. Both ends share this definition so they cannot disagree on the format.
 */
object ResetRequest:
  val RequestAnnotation = s"${AnkkaFlowDefinition.domain}/reset-offsets"
  val DoneAnnotation    = s"${AnkkaFlowDefinition.domain}/reset-offsets-done"

  /** @param streamlets the streamlets whose inlets to reset; empty means every one that reads. */
  final case class Request(id: String, streamlets: List[String] = Nil):
    def includes(streamlet: String): Boolean = streamlets.isEmpty || streamlets.contains(streamlet)

  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def toJson(request: Request): String = mapper.writeValueAsString(request)

  def fromJson(json: String): Try[Request] = Try(mapper.readValue(json, classOf[Request]))

  /** The request recorded on the pipeline, if any and if it parses. */
  def request(flow: AnkkaFlow): Option[Request] =
    annotation(flow, RequestAnnotation).flatMap(fromJson(_).toOption)

  /** The id of the last request the operator carried out, if any. */
  def done(flow: AnkkaFlow): Option[String] = annotation(flow, DoneAnnotation)

  /** The recorded request, unless it has already been carried out. */
  def pending(flow: AnkkaFlow): Option[Request] =
    request(flow).filterNot(r => done(flow).contains(r.id))

  /**
   * The consumer group an inlet reads with: `<pipeline>.<streamlet>.<inlet>` (FR-015), after the
   * prefix an ankka machine's cluster gives its groups.
   */
  def groupId(
      pipeline: String,
      streamlet: String,
      inlet: String,
      prefix: Option[String] = None
  ): String =
    s"${prefix.getOrElse("")}$pipeline.$streamlet.$inlet"

  /**
   * Each Flink streamlet's state epoch: a JSON object of streamlet to epoch. A streamlet the
   * annotation does not name, or an annotation that does not parse, is at epoch 1. A reset of a
   * Flink streamlet advances its epoch, so its job starts with state no earlier run wrote.
   */
  val StateEpochsAnnotation = s"${AnkkaFlowDefinition.domain}/state-epochs"

  def stateEpochs(flow: AnkkaFlow): Map[String, Int] =
    annotation(flow, StateEpochsAnnotation)
      .flatMap { text =>
        Try {
          val node = mapper.readTree(text)
          node.properties.asScala.map(e => e.getKey -> e.getValue.asInt(1)).toMap
        }.toOption
      }
      .getOrElse(Map.empty)

  def stateEpoch(flow: AnkkaFlow, streamlet: String): Int =
    stateEpochs(flow).getOrElse(streamlet, 1)

  def withEpochAdvanced(flow: AnkkaFlow, streamlet: String): AnkkaFlow =
    val epochs = stateEpochs(flow)
    val next   = epochs.updated(streamlet, epochs.getOrElse(streamlet, 1) + 1)
    val sorted = scala.collection.immutable.TreeMap.from(next)
    annotate(flow, StateEpochsAnnotation, mapper.writeValueAsString(sorted.asJava))

  def withRequest(flow: AnkkaFlow, request: Request): AnkkaFlow =
    annotate(flow, RequestAnnotation, toJson(request))

  private def annotate(flow: AnkkaFlow, key: String, value: String): AnkkaFlow =
    val annotations = new java.util.HashMap[String, String]()
    Option(flow.getMetadata.getAnnotations).foreach(a => annotations.putAll(a))
    annotations.put(key, value)
    flow.getMetadata.setAnnotations(annotations)
    flow

  private def annotation(flow: AnkkaFlow, key: String): Option[String] =
    Option(flow.getMetadata).flatMap(m => Option(m.getAnnotations)).flatMap(a => Option(a.get(key)))
