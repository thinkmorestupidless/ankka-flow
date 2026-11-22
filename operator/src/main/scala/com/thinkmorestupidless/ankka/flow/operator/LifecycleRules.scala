package com.thinkmorestupidless.ankka.flow.operator

import com.thinkmorestupidless.ankka.flow.crd.*

/**
 * The pipeline's phase and conditions from what was rendered and observed (data-model.md, *Phase*
 * and *A Flink streamlet's lifecycle*): Ready iff every streamlet is running and every topic
 * exists; Degraded when an unmanaged topic is missing, a settled streamlet is short, or a streamlet
 * failed; Pending while anything rolls out.
 */
object LifecycleRules:

  /**
   * What `status.error` says when a job cannot restore its state (research R17). V6 pins the texts
   * to what a FlinkDeployment shows.
   */
  val StateRestoreFailures: Vector[String] = Vector(
    "StateMigrationException",
    "Failed to rollback to checkpoint/savepoint",
    "Cannot map checkpoint/savepoint state",
    "InvalidClassException"
  )

  def stateUnreadableMessage(streamlet: String): String =
    s"streamlet '$streamlet' cannot read its state with its new image: deploy the previous image, or reset it"

  /** One streamlet as the status reports it, and whether it is settled at its spec. */
  private final case class Seen(
      status: StreamletStatus,
      settled: Boolean,
      failed: Boolean = false,
      stateUnreadable: Boolean = false
  ):
    def running: Boolean = settled && !failed && status.ready >= status.desired

  def status(
      resource: AnkkaFlow,
      desiredHashes: Map[String, String],
      topics: Vector[ResolvedTopic],
      observed: Observed,
      now: String
  ): AnkkaFlowStatus =
    val spec = resource.getSpec
    val streamlets = spec.streamlets.map { s =>
      val hash = desiredHashes.getOrElse(s.name, "")
      if s.engine == "flink" then flink(s, hash, observed.flinkDeployments.get(s.name))
      else pekko(s, hash, observed.deployments.get(s.name))
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
    val allReady = streamlets.forall(_.running)
    val failed   = streamlets.exists(_.failed)
    val rolling  = streamlets.exists(!_.settled)
    val phase =
      if missing.nonEmpty || failed then AnkkaFlowStatus.Degraded
      else if allReady then AnkkaFlowStatus.Ready
      else if rolling then AnkkaFlowStatus.Pending
      else AnkkaFlowStatus.Degraded
    // The pipeline's detail names what is not running; each streamlet's says how it stands.
    val detail = (missing.map(_.detail) ++ streamlets.collect {
      case st if !st.running && st.status.detail.nonEmpty =>
        s"${st.status.name}: ${st.status.detail}"
    }).mkString("; ")
    val unreadable = streamlets.collect {
      case st if st.stateUnreadable =>
        Conditions.holds(
          Condition.StateUnreadable,
          "RestoreFailed",
          stateUnreadableMessage(st.status.name),
          now
        )
    }
    val ready = Conditions.ready(phase == AnkkaFlowStatus.Ready, phase, detail, now)
    AnkkaFlowStatus(
      observedGeneration =
        Option(resource.getMetadata.getGeneration).map(_.longValue).getOrElse(0L),
      phase = phase,
      detail = detail,
      lastTransitionTime = now,
      conditions = Conditions.merge(Conditions.previous(resource), unreadable :+ ready),
      streamlets = streamlets.map(_.status),
      topics = topicStatuses
    )

  private def pekko(s: StreamletSpec, hash: String, d: Option[DeploymentState]): Seen =
    val ready   = d.fold(0)(_.readyReplicas)
    val settled = d.exists(dep => dep.configHash == hash && dep.image == s.image && dep.rolledOut)
    val detail  = if !settled then "rolling out" else s"$ready of ${s.replicas} ready"
    Seen(StreamletStatus(s.name, s.engine, s.replicas, ready, detail), settled)

  /** The rows of data-model's Flink lifecycle table. */
  private def flink(s: StreamletSpec, hash: String, f: Option[FlinkState]): Seen =
    def st(ready: Int, detail: String) =
      StreamletStatus(s.name, s.engine, s.replicas, ready, detail)
    f match
      case None => Seen(st(0, "rolling out"), settled = false)
      case Some(job) =>
        val failed =
          job.lifecycleState.contains("FAILED") || job.jobManagerStatus.contains("ERROR")
        val current = job.configHash == hash && job.image == s.image
        if failed then
          val error        = job.error.getOrElse("the job failed")
          val imageChanged = job.lastStableImage.exists(_ != job.image)
          val unreadable =
            imageChanged && StateRestoreFailures.exists(t => job.error.exists(_.contains(t)))
          val detail = if unreadable then stateUnreadableMessage(s.name) else error
          Seen(st(0, detail), settled = current, failed = true, stateUnreadable = unreadable)
        else if !current then Seen(st(0, "rolling out"), settled = false)
        else if job.lifecycleState.contains("SUSPENDED") then
          Seen(st(0, "suspended"), settled = s.replicas == 0)
        else if job.lifecycleState.contains("STABLE") && job.jobState.contains("RUNNING") then
          Seen(st(1, "running"), settled = s.replicas == 1)
        else Seen(st(0, "rolling out"), settled = false)

  def failed(resource: AnkkaFlow, problems: Vector[String], now: String): AnkkaFlowStatus =
    AnkkaFlowStatus(
      observedGeneration =
        Option(resource.getMetadata.getGeneration).map(_.longValue).getOrElse(0L),
      phase = AnkkaFlowStatus.Failed,
      detail = problems.mkString("; "),
      lastTransitionTime = now,
      conditions = Conditions.merge(
        Conditions.previous(resource),
        List(Conditions.ready(false, AnkkaFlowStatus.Failed, problems.mkString("; "), now))
      )
    )
