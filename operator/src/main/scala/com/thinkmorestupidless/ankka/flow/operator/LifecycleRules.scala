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
    // What a JobException event's message begins with when an operator's state cannot be restored:
    // the event keeps only the top of the trace (V6).
    "Exception while creating StreamOperatorStateContext",
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
    val spec     = resource.getSpec
    val names    = topics.map(t => t.id -> t.name).toMap
    val prefixes = topics.map(t => t.id -> t.groupPrefix).toMap
    val streamlets = spec.streamlets.map { s =>
      val hash = desiredHashes.getOrElse(s.name, "")
      val seen =
        if s.engine == "flink" then flink(s, hash, observed.flinkDeployments.get(s.name))
        else pekko(s, hash, observed.deployments.get(s.name))
      // Each inlet's group and lag, the same for every engine.
      val inlets = s.inlets.toList.sortBy(_._1).map { (inlet, topic) =>
        val group = Names.group(spec.pipeline, s.name, inlet, prefixes.get(topic).flatten)
        InletStatus(inlet, names.getOrElse(topic, topic), group, observed.lag.get(group))
      }
      seen.copy(status = seen.status.copy(inlets = inlets))
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
    val grants   = grantConditions(resource, topics, observed, now)
    val allReady = streamlets.forall(_.running)
    val failed   = streamlets.exists(_.failed)
    val rolling  = streamlets.exists(!_.settled)
    val phase =
      if missing.nonEmpty || failed || grants.nonEmpty then AnkkaFlowStatus.Degraded
      else if allReady then AnkkaFlowStatus.Ready
      else if rolling then AnkkaFlowStatus.Pending
      else AnkkaFlowStatus.Degraded
    // The pipeline's detail names what is not running; each streamlet's says how it stands.
    val detail = (missing.map(_.detail) ++ grants.map(_.message) ++ streamlets.collect {
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
      conditions = Conditions.merge(Conditions.previous(resource), grants ++ unreadable :+ ready),
      streamlets = streamlets.map(_.status),
      topics = topicStatuses
    )

  /**
   * `TopicNotGranted` for the topics whose machine lacks a right, and `MachineCredentialRefused`
   * when a machine's credentials are refused (R16). Each is one condition, its message naming every
   * topic it holds for; a topic the broker could not be asked about is neither.
   */
  private def grantConditions(
      resource: AnkkaFlow,
      topics: Vector[ResolvedTopic],
      observed: Observed,
      now: String
  ): List[Condition] =
    val specs = resource.getSpec.topics.map(t => t.id -> t).toMap
    val found = topics.sortBy(_.id).flatMap(t => observed.grants.get(t.id).map(t -> _))
    val lacking = found.collect { case (t, Grant.Lacks(right)) =>
      right -> notGranted(t, right, specs.get(t.id).flatMap(_.ankka))
    }
    val refused = found.collect { case (t, Grant.CredentialRefused(reason)) =>
      s"ankka refuses the credentials of machine ${t.machine.fold("?")(_.toString)}: $reason"
    }.distinct
    Option
      .when(lacking.nonEmpty)(
        Conditions.holds(
          Condition.TopicNotGranted,
          lacking.head._1.toString,
          lacking.map(_._2).mkString("; "),
          now
        )
      )
      .toList ++
      Option
        .when(refused.nonEmpty)(
          Conditions.holds(
            Condition.MachineCredentialRefused,
            "CredentialRefused",
            refused.mkString("; "),
            now
          )
        )
        .toList

  def notGranted(t: ResolvedTopic, right: Grant.Right, ankka: Option[AnkkaRef]): String =
    val machine = t.machine.fold("?")(_.toString)
    val verb    = if right == Grant.Right.Read then "read" else "write"
    val remedy = ankka.fold("")(a =>
      s": an owner of project ${a.project} runs `ankka projects grants make machine:$machine topic ${a.name} ${right.grantWord} -p ${a.project}`"
    )
    s"machine $machine holds no grant to $verb topic ${t.name}$remedy"

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
        val current      = job.configHash == hash && job.image == s.image
        val imageChanged = job.lastStableImage.exists(_ != job.image)
        def restoreFailure(text: Option[String]) =
          text.exists(t => StateRestoreFailures.exists(t.contains))
        // A job that cannot restore its state restarts without end, the deployment DEPLOYED: the
        // JobException events of the current job say so (V6).
        val restoring = imageChanged && restoreFailure(job.jobException)
        val failed =
          job.lifecycleState.contains("FAILED") || job.jobManagerStatus.contains(
            "ERROR"
          ) || restoring
        if failed then
          val error = job.error.orElse(job.jobException).getOrElse("the job failed")
          val unreadable =
            imageChanged && (restoreFailure(job.error) || restoreFailure(job.jobException))
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
