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

package com.thinkmorestupidless.ankka.flow.cli

import java.util.UUID

import com.thinkmorestupidless.ankka.flow.crd.{AnkkaFlow, ResetRequest}

/**
 * Who may be reset, and when. Carried from Cloudflow's `ResetOffsetsExecution.targets` and
 * `stopped`
 * (core/cloudflow-cli/src/main/scala/cloudflow/cli/execution/ResetOffsetsExecution.scala): only a
 * streamlet that reads has consumer groups to reset, and every target must be stopped, scaled to 0
 * with no pods left, so the refusal is immediate and names what to do. Kafka refuses, too, to reset
 * a group that still has members, which is what holds if this view is stale.
 */
object ResetGuards:

  /**
   * The streamlets a request would reset: the named ones, each of which must exist and read; or
   * every one that reads.
   */
  def targets(flow: AnkkaFlow, named: List[String]): Either[Vector[String], List[String]] =
    val spec                = flow.getSpec
    def reads(name: String) = spec.streamlets.find(_.name == name).exists(_.inlets.nonEmpty)
    if named.isEmpty then
      val all = spec.streamlets.filter(_.inlets.nonEmpty).map(_.name)
      if all.isEmpty then
        Left(Vector(s"pipeline ${spec.pipeline} has no streamlets with inlets to reset"))
      else Right(all)
    else
      val problems = named.flatMap { name =>
        if !spec.streamlets.exists(_.name == name) then Some(s"no streamlet [$name]")
        else if !reads(name) then
          Some(s"streamlet [$name] has no inlets, so no consumer groups to reset")
        else None
      }
      if problems.isEmpty then Right(named)
      else Left(Vector(s"cannot reset offsets: ${problems.mkString("; ")}"))

  /** Every target must be scaled to 0 and have no pods left. */
  def stopped(
      flow: AnkkaFlow,
      targets: List[String],
      pods: Map[String, Int]
  ): Either[Vector[String], Unit] =
    val running = targets.flatMap { name =>
      val replicas = flow.getSpec.streamlets.find(_.name == name).map(_.replicas)
      val count    = pods.getOrElse(name, 0)
      if !replicas.contains(0) then Some(s"[$name] is not scaled to 0")
      else if count > 0 then Some(s"[$name] still has $count pod(s)")
      else None
    }
    if running.isEmpty then Right(())
    else
      Left(
        Vector(
          s"cannot reset offsets while streamlets are running: ${running.mkString("; ")}. Stop them first: set " +
            s"replicas: 0 for ${targets.mkString(", ")} (flow generate --conf with flow.streamlets.<name>.replicas = 0) and apply, " +
            "then wait for their pods to go."
        )
      )

  def check(
      flow: AnkkaFlow,
      named: List[String],
      pods: Map[String, Int]
  ): Either[Vector[String], ResetRequest.Request] =
    for
      ts <- targets(flow, named)
      _  <- stopped(flow, ts, pods)
    yield ResetRequest.Request(UUID.randomUUID().toString, named)
