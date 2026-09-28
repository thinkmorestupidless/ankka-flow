package com.thinkmorestupidless.ankka.flow.operator

import com.thinkmorestupidless.ankka.flow.crd.*

/**
 * The pipeline's phase from what was rendered and observed (data-model.md, *Pipeline phase*): Ready
 * iff every streamlet is ready at its desired count and every topic exists; Degraded when an
 * unmanaged topic is missing or a settled streamlet is short; Pending while anything rolls out.
 */
object LifecycleRules:

  def status(
      resource: AnkkaFlow,
      desiredHashes: Map[String, String],
      topics: Vector[ResolvedTopic],
      observed: Observed,
      now: String
  ): AnkkaFlowStatus =
    val spec = resource.getSpec
    val streamlets = spec.streamlets.map { s =>
      val d     = observed.deployments.get(s.name)
      val ready = d.fold(0)(_.readyReplicas)
      val settled = d.exists(dep =>
        dep.configHash == desiredHashes.getOrElse(
          s.name,
          ""
        ) && dep.image == s.image && dep.rolledOut
      )
      val detail =
        if !settled then "rolling out"
        else if ready < s.replicas then s"$ready of ${s.replicas} ready"
        else ""
      StreamletStatus(s.name, s.replicas, ready, detail) -> settled
    }
    val topicStatuses = topics.toList.map { t =>
      observed.topics.get(t.name) match
        case Some(TopicState.Exists(_, _, _)) => TopicStatus(t.id, exists = true)
        case _ if t.managed => TopicStatus(t.id, exists = true, "created by the operator")
        case Some(TopicState.Unreachable(r)) =>
          TopicStatus(t.id, exists = false, s"Kafka unreachable: $r")
        case _ => TopicStatus(t.id, exists = false, s"topic '${t.name}' does not exist")
    }
    val missing  = topicStatuses.filterNot(_.exists)
    val allReady = streamlets.forall { case (st, settled) => settled && st.ready >= st.desired }
    val rolling  = streamlets.exists { case (_, settled) => !settled }
    val phase =
      if missing.nonEmpty then AnkkaFlowStatus.Degraded
      else if allReady then AnkkaFlowStatus.Ready
      else if rolling then AnkkaFlowStatus.Pending
      else AnkkaFlowStatus.Degraded
    val detail = (missing.map(_.detail) ++ streamlets.collect {
      case (st, _) if st.detail.nonEmpty => s"${st.name}: ${st.detail}"
    }).mkString("; ")
    AnkkaFlowStatus(
      observedGeneration =
        Option(resource.getMetadata.getGeneration).map(_.longValue).getOrElse(0L),
      phase = phase,
      detail = detail,
      lastTransitionTime = now,
      streamlets = streamlets.map(_._1),
      topics = topicStatuses
    )

  def failed(resource: AnkkaFlow, problems: Vector[String], now: String): AnkkaFlowStatus =
    AnkkaFlowStatus(
      observedGeneration =
        Option(resource.getMetadata.getGeneration).map(_.longValue).getOrElse(0L),
      phase = AnkkaFlowStatus.Failed,
      detail = problems.mkString("; "),
      lastTransitionTime = now
    )
