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
        // A Flink job is stopped once its FlinkDeployment reports it suspended, or is gone.
        val job = observed.flinkDeployments.get(s.name).filter(_ => s.engine == "flink")
        if s.replicas != 0 then Some(s"${s.name} is not scaled to 0")
        else if job.exists(!_.lifecycleState.contains("SUSPENDED")) then
          Some(s"${s.name}'s Flink job is not yet suspended")
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
        val namespace = resource.getMetadata.getNamespace
        val resets = targets.toVector.flatMap { s =>
          val groups = s.inlets.toVector.sortBy(_._1).flatMap { (inlet, topicId) =>
            topics
              .get(topicId)
              .map(t =>
                Action.ResetGroup(
                  ResetTarget(s.name, inlet, ResetRequest.groupId(pipeline, s.name, inlet), t)
                )
              )
          }
          if s.engine != "flink" then groups
          else
            // Deleting the FlinkDeployment removes its high-availability metadata; the next epoch
            // is in every state path, so the job that starts next finds no state. The previous
            // epoch's files stay for the store's own lifecycle rule (R12).
            val epoch = observed.epoch(s.name)
            val left = observed.stateStore
              .map(
                _.paths(namespace, pipeline, s.name, epoch).checkpoints.stripSuffix("/checkpoints")
              )
              .getOrElse("the state store")
            Vector(
              Action.DeleteFlinkDeployment(namespace, Names.deployment(pipeline, s.name)),
              Action.AdvanceStateEpoch(s.name)
            ) ++ groups :+
              Action.RecordEvent(
                "StateDiscarded",
                Events.Normal,
                s"streamlet '${s.name}' starts at state epoch ${epoch + 1}; the state of epoch $epoch is left in $left"
              )
        }
        resets :+ Action.MarkResetDone(request.id)
    }
