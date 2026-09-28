package com.thinkmorestupidless.ankka.flow.operator

import com.thinkmorestupidless.ankka.flow.crd.{AnkkaFlow, ResetRequest}

/**
 * A pending reset request (FR-023, US4): refused with an event while any target runs; otherwise one
 * group reset per target inlet, over the target's own resolved connection, then the done marker, so
 * a restart never repeats it. The guards are Cloudflow's, applied again here because the CLI's view
 * of the pods may be stale.
 */
object Reset:

  def actions(
      resource: AnkkaFlow,
      observed: Observed,
      topics: Map[String, ResolvedTopic]
  ): Vector[Action] =
    ResetRequest.pending(resource).toVector.flatMap { request =>
      val spec     = resource.getSpec
      val pipeline = spec.pipeline
      val reading  = spec.streamlets.filter(s => s.inlets.nonEmpty)
      val targets  = reading.filter(s => request.includes(s.name))
      val unknown  = request.streamlets.filterNot(n => spec.streamlets.exists(_.name == n))
      val running = targets.flatMap { s =>
        val pods = observed.pods.getOrElse(s.name, 0)
        if s.replicas != 0 then Some(s"${s.name} is not scaled to 0")
        else if pods > 0 then Some(s"${s.name} still has $pods pod(s)")
        else None
      }
      if unknown.nonEmpty then
        Vector(
          Action.RecordEvent(
            "ResetRefused",
            Events.Warning,
            s"reset ${request.id} names unknown streamlet(s) ${unknown.mkString(", ")}"
          ),
          Action.MarkResetDone(request.id)
        )
      else if running.nonEmpty then
        Vector(
          Action.RecordEvent(
            "ResetRefused",
            Events.Warning,
            s"reset ${request.id} waits: ${running.mkString("; ")}"
          )
        )
      else
        val resets = targets.toVector.flatMap { s =>
          s.inlets.toVector.sortBy(_._1).flatMap { (inlet, topicId) =>
            topics
              .get(topicId)
              .map(t =>
                Action.ResetGroup(
                  ResetTarget(s.name, inlet, ResetRequest.groupId(pipeline, s.name, inlet), t)
                )
              )
          }
        }
        resets :+ Action.MarkResetDone(request.id)
    }
