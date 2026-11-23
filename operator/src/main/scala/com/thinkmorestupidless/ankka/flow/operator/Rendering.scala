package com.thinkmorestupidless.ankka.flow.operator

import scala.jdk.CollectionConverters.*

import com.thinkmorestupidless.ankka.flow.crd.*
import com.thinkmorestupidless.ankka.flow.descriptor.{DescriptorJson, DescriptorValidation, Json}
import io.fabric8.kubernetes.api.model.*
import io.fabric8.kubernetes.api.model.apps.{Deployment, DeploymentBuilder}
import io.fabric8.kubernetes.api.model.apps.{
  DeploymentSpecBuilder,
  DeploymentStrategyBuilder,
  RollingUpdateDeploymentBuilder
}
import io.fabric8.kubernetes.api.model.rbac.{
  PolicyRuleBuilder,
  RoleBindingBuilder,
  RoleBuilder,
  RoleRefBuilder,
  SubjectBuilder
}

/**
 * From a resource and what was observed, the actions that make the cluster match it. Pure: no
 * client, no clock (the time comes in), no Kafka (contracts/resource-and-operator.md, *Reconcile*).
 */
object Rendering:

  /** Mode of the settings Secret's files: 0440, readable by the pod's group only. */
  val SecretFileMode: Int = Integer.parseInt("440", 8)

  val ConfigDir = "/etc/flow/config"
  val StateDir  = "/tmp/flow"

  /** The engines this operator runs. */
  private val Runs = Set("pekko-streams", "flink")

  private def isFlink(s: StreamletSpec): Boolean = s.engine == "flink"

  final case class Rendered(
      actions: Vector[Action],
      resolved: Vector[ResolvedTopic],
      hashes: Map[String, String]
  )

  def render(
      resource: AnkkaFlow,
      settings: Settings,
      observed: Observed,
      now: String
  ): Rendered =
    val spec      = resource.getSpec
    val pipeline  = spec.pipeline
    val namespace = resource.getMetadata.getNamespace

    // ── Deletion: the finalizer is only there when managed topics are to be deleted ──────────
    if Option(resource.getMetadata.getDeletionTimestamp).exists(_.nonEmpty) then
      val resolved =
        spec.topics.flatMap(t => TopicResolution.resolve(t, observed.clusters).toOption).toVector
      val deletes =
        if spec.onDelete.managedTopics == OnDelete.Delete then
          resolved.filter(_.managed).map(Action.DeleteTopic(_))
        else Vector.empty
      return Rendered(deletes :+ Action.RemoveFinalizer, resolved, Map.empty)

    // ── 1. Refusals: nothing is applied when any of these is true ────────────────────────────
    val topicsResolved = spec.topics.map(t => t.id -> TopicResolution.resolve(t, observed.clusters))
    val refusals: Vector[String] =
      topicsResolved.toVector.collect { case (_, Left(e)) => e } ++
        spec.topics.flatMap(t => t.cluster.flatMap(observed.clusterProblems.get)).distinct ++
        spec.streamlets.flatMap(s => descriptorProblems(s))
    // What Flink needs of the cluster: each a condition, and the whole resource refused (R11).
    val missing =
      Conditions.prerequisites(spec.streamlets.exists(isFlink), observed, settings, now)
    if missing.nonEmpty then
      val conditions = Conditions.merge(
        Conditions.previous(resource),
        missing :+ Conditions.ready(false, missing.head.`type`, missing.head.message, now)
      )
      return Rendered(
        missing.toVector.map(c => Action.RecordEvent(c.`type`, Events.Warning, c.message)) ++
          refusals.map(p => Action.RecordEvent("Refused", Events.Warning, p)) :+
          Action.SetStatus(
            LifecycleRules
              .failed(resource, missing.map(_.message).toVector ++ refusals, now)
              .copy(conditions = conditions)
          ),
        Vector.empty,
        Map.empty
      )
    if refusals.nonEmpty then
      return Rendered(
        refusals.map(p => Action.RecordEvent("Refused", Events.Warning, p)) :+
          Action.SetStatus(LifecycleRules.failed(resource, refusals, now)),
        Vector.empty,
        Map.empty
      )
    val topics = topicsResolved.collect { case (_, Right(t)) => t }.toVector
    val byId   = topics.map(t => t.id -> t).toMap

    // ── 2. Topics ─────────────────────────────────────────────────────────────────────────────
    val topicActions = topics.flatMap { t =>
      (t.managed, observed.topics.get(t.name)) match
        case (true, None | Some(TopicState.Missing)) =>
          Vector(
            Action.EnsureTopic(t),
            Action.RecordEvent(
              "TopicCreated",
              Events.Normal,
              s"created topic '${t.name}' with ${t.partitions.get} partitions"
            )
          )
        case (true, Some(TopicState.Exists(partitions, replication, configs))) =>
          val differs = Option.when(
            t.partitions.exists(_ != partitions) || t.replicas.exists(_ != replication)
          )(
            Action.RecordEvent(
              "TopicDiffers",
              Events.Warning,
              s"topic '${t.name}' exists with $partitions partitions and replication $replication; the resource declares ${t.partitions.get} and ${t.replicas.get}. Left as it is."
            )
          )
          val changed = t.topicConfig
            .collect { case (k, v) if configs.get(k).exists(_ != v) => k }
            .toVector
            .sorted
          val ignored = Option.when(changed.nonEmpty)(
            Action.RecordEvent(
              "TopicSettingsIgnored",
              Events.Warning,
              s"topic '${t.name}' exists; changed settings ${changed.mkString(", ")} were not applied"
            )
          )
          differs.toVector ++ ignored
        case (false, Some(TopicState.Missing)) =>
          Vector(
            Action.RecordEvent(
              "TopicMissing",
              Events.Warning,
              s"topic '${t.name}' is not managed by the pipeline and does not exist; its consumers will not become ready"
            )
          )
        case _ => Vector.empty
    }

    // ── 3. Per pipeline: its identity ────────────────────────────────────────────────────────
    val owner = ownerReference(resource)
    def meta(name: String) =
      new ObjectMetaBuilder()
        .withName(name)
        .withNamespace(namespace)
        .withLabels(Labels.pipeline(pipeline).asJava)
        .withOwnerReferences(owner)
        .build()
    val account = new ServiceAccountBuilder()
      .withMetadata(meta(Names.serviceAccount(pipeline)))
      .withAutomountServiceAccountToken(false)
      .build()
    val role = new RoleBuilder()
      .withMetadata(meta(Names.serviceAccount(pipeline)))
      .withRules(
        new PolicyRuleBuilder()
          .withApiGroups("events.k8s.io")
          .withResources("events")
          .withVerbs("create")
          .build()
      )
      .build()
    val binding = new RoleBindingBuilder()
      .withMetadata(meta(Names.serviceAccount(pipeline)))
      .withRoleRef(
        new RoleRefBuilder()
          .withApiGroup("rbac.authorization.k8s.io")
          .withKind("Role")
          .withName(Names.serviceAccount(pipeline))
          .build()
      )
      .withSubjects(
        new SubjectBuilder()
          .withKind("ServiceAccount")
          .withName(Names.serviceAccount(pipeline))
          .withNamespace(namespace)
          .build()
      )
      .build()

    // ── 4. Per streamlet: the Secret with its settings, and the Deployment ────────────────────
    val files = spec.streamlets
      .map(s =>
        s.name -> StreamletFiles.render(pipeline, namespace, s, byId, observed.epoch(s.name))
      )
      .toMap
    val fileProblems = files.values.collect { case Left(e) => e }.toVector
    if fileProblems.nonEmpty then
      return Rendered(
        fileProblems.map(p => Action.RecordEvent("Refused", Events.Warning, p)) :+ Action.SetStatus(
          LifecycleRules.failed(resource, fileProblems, now)
        ),
        topics,
        Map.empty
      )
    val hashes = files.collect { case (n, Right(f)) => n -> f.hash }
    def rolledEvent(s: StreamletSpec) = Action.RecordEvent(
      "StreamletRolled",
      Events.Normal,
      s"streamlet '${s.name}' rolls out: its image, descriptor or configuration changed"
    )
    // The state store's env, once per pipeline that has a Flink streamlet (R11).
    val flinkEnv = observed.stateStore
      .filter(_ => spec.streamlets.exists(isFlink))
      .map(store => Action.EnsureSecret(FlinkRendering.envSecret(resource, store, owner)))
      .toVector
    val streamletActions = flinkEnv ++ spec.streamlets.toVector.flatMap { s =>
      val f      = files(s.name).toOption.get
      val secret = secretFor(resource, s, f, owner)
      if isFlink(s) then
        val deployment = FlinkRendering.deploymentFor(
          resource,
          s,
          f.hash,
          settings,
          observed.stateStore.get,
          observed.epoch(s.name),
          FlinkRendering.tokenEndpoints(s, byId),
          owner
        )
        val rolled = observed.flinkDeployments
          .get(s.name)
          .filter(d => d.configHash != f.hash || d.image != s.image)
          .map(_ => rolledEvent(s))
        Vector(Action.EnsureSecret(secret), Action.ApplyFlinkDeployment(deployment)) ++ rolled
      else
        val deployment = deploymentFor(resource, s, f.hash, owner)
        val rolled = observed.deployments
          .get(s.name)
          .filter(d => d.configHash != f.hash || d.image != s.image)
          .map(_ => rolledEvent(s))
        Vector(Action.EnsureSecret(secret), Action.ApplyDeployment(deployment)) ++ rolled
    }
    val inSpec = spec.streamlets.map(_.name).toSet
    val removedFlink =
      (observed.flinkDeployments.keySet -- inSpec).toVector.sorted.flatMap { name =>
        Vector(
          Action.DeleteFlinkDeployment(namespace, Names.deployment(pipeline, name)),
          Action.DeleteSecret(namespace, Names.secret(pipeline, name)),
          Action.RecordEvent(
            "StreamletRemoved",
            Events.Normal,
            s"streamlet '$name' is no longer in the pipeline"
          )
        )
      }
    val removed = removedFlink ++
      (observed.labelledStreamlets -- inSpec).toVector.sorted.flatMap { name =>
        Vector(
          Action.DeleteDeployment(namespace, Names.deployment(pipeline, name)),
          Action.DeleteSecret(namespace, Names.secret(pipeline, name)),
          Action.RecordEvent(
            "StreamletRemoved",
            Events.Normal,
            s"streamlet '$name' is no longer in the pipeline"
          )
        )
      }

    // ── 5. A reset request (FR-023) ───────────────────────────────────────────────────────────
    val reset = Reset.actions(resource, observed, byId)
    // A FlinkDeployment a reset deletes is applied again by the next reconcile, at its new epoch.
    val discarded = reset.collect { case Action.DeleteFlinkDeployment(_, name) => name }.toSet

    // ── 6. Finalizer, when managed topics are to go with the pipeline ────────────────────────
    val finalizer =
      Option.when(spec.onDelete.managedTopics == OnDelete.Delete)(Action.EnsureFinalizer).toVector

    val status = LifecycleRules.status(resource, hashes, topics, observed, now)
    // A condition that begins to hold is recorded once as a Warning, with its message.
    val began = status.conditions
      .filter(c =>
        c.`type` != com.thinkmorestupidless.ankka.flow.crd.Condition.Ready &&
          c.status == com.thinkmorestupidless.ankka.flow.crd.Condition.True
      )
      .filterNot(c =>
        Conditions
          .previous(resource)
          .exists(p => p.`type` == c.`type` && p.status == c.status && p.message == c.message)
      )
      .map(c => Action.RecordEvent(c.`type`, Events.Warning, c.message))
    Rendered(
      finalizer ++ topicActions ++
        Vector(
          Action.EnsureServiceAccount(account),
          Action.EnsureRole(role),
          Action.EnsureRoleBinding(binding)
        ) ++
        streamletActions.filterNot {
          case Action.ApplyFlinkDeployment(d) => discarded.contains(d.getMetadata.getName)
          case _                              => false
        } ++ removed ++ reset ++ began :+ Action.SetStatus(status),
      topics,
      hashes
    )

  private def descriptorProblems(s: StreamletSpec): Vector[String] =
    Option(s.descriptor) match
      case None => Vector(s"streamlet '${s.name}' has no descriptor")
      case Some(node) =>
        Json.parse(node.toString).flatMap(DescriptorJson.streamletFromJson) match
          // A descriptor naming no engine, or one flow does not know, says so as it is.
          case Left(e) if e == DescriptorJson.NoEngine || e.startsWith("engine '") =>
            Vector(s"streamlet '${s.name}': $e")
          case Left(e) => Vector(s"streamlet '${s.name}': its descriptor does not parse: $e")
          case Right(d) =>
            val own =
              DescriptorValidation.validateStreamlet(d).map(p => s"streamlet '${s.name}': $p") ++
                Option
                  .when(s.engine == d.engine.id && !Runs.contains(s.engine))(
                    s"streamlet '${s.name}': engine ${s.engine} is not yet run by this operator"
                  )
                  .toVector ++
                Option
                  .when(s.engine != d.engine.id)(
                    s"streamlet '${s.name}': the resource says engine '${s.engine}' and its descriptor '${d.engine.id}'"
                  )
                  .toVector
            // Every inlet is bound exactly; an outlet may be left unbound (connected to nothing is
            // allowed, S2.2); binding a port the descriptor does not declare never is.
            val inlets  = d.inlets.map(_.name).toSet
            val outlets = d.outlets.map(_.name).toSet
            own ++
              (inlets -- s.inlets.keySet).toVector.sorted.map(p =>
                s"streamlet '${s.name}': inlet '$p' is not bound to a topic"
              ) ++
              (s.inlets.keySet -- inlets).toVector.sorted.map(p =>
                s"streamlet '${s.name}': binds inlet '$p', which the descriptor does not declare"
              ) ++
              (s.outlets.keySet -- outlets).toVector.sorted.map(p =>
                s"streamlet '${s.name}': binds outlet '$p', which the descriptor does not declare"
              )

  def ownerReference(resource: AnkkaFlow): OwnerReference =
    new OwnerReferenceBuilder()
      .withApiVersion(AnkkaFlowDefinition.apiVersion)
      .withKind(AnkkaFlowDefinition.kind)
      .withName(resource.getMetadata.getName)
      .withUid(resource.getMetadata.getUid)
      .withController(true)
      .withBlockOwnerDeletion(true)
      .build()

  def secretFor(
      resource: AnkkaFlow,
      s: StreamletSpec,
      f: StreamletFiles,
      owner: OwnerReference
  ): Secret =
    val pipeline = resource.getSpec.pipeline
    new SecretBuilder()
      .withMetadata(
        new ObjectMetaBuilder()
          .withName(Names.secret(pipeline, s.name))
          .withNamespace(resource.getMetadata.getNamespace)
          .withLabels(Labels.streamlet(pipeline, s.name).asJava)
          .withOwnerReferences(owner)
          .build()
      )
      .withType("Opaque")
      .withStringData(Map(StreamletFiles.Key -> f.properties).asJava)
      .build()

  /**
   * A Pekko Streams streamlet's Deployment: one container `streamlet`, its image, its settings
   * mounted read-only where `FLOW_CONFIG_DIR` names, the runner's two file probes, and no service
   * account token.
   */
  def deploymentFor(
      resource: AnkkaFlow,
      s: StreamletSpec,
      hash: String,
      owner: OwnerReference
  ): Deployment =
    val pipeline               = resource.getSpec.pipeline
    val labels                 = Labels.streamlet(pipeline, s.name)
    def exec(command: String*) = new ExecActionBuilder().withCommand(command*).build()
    val streamlet = new ContainerBuilder()
      .withName("streamlet")
      .withImage(s.image)
      .withImagePullPolicy("IfNotPresent")
      .withEnv(new EnvVarBuilder().withName("FLOW_CONFIG_DIR").withValue(ConfigDir).build())
      .withVolumeMounts(
        new VolumeMountBuilder()
          .withName("config")
          .withMountPath(ConfigDir)
          .withReadOnly(true)
          .build()
      )
      .withReadinessProbe(
        new ProbeBuilder()
          .withExec(exec("test", "-f", s"$StateDir/ready"))
          .withPeriodSeconds(5)
          .build()
      )
      .withLivenessProbe(
        new ProbeBuilder()
          .withExec(
            exec(
              "sh",
              "-c",
              s"""test $$(( $$(date +%s) - $$(stat -c %Y $StateDir/alive) )) -lt 15"""
            )
          )
          .withInitialDelaySeconds(20)
          .withPeriodSeconds(10)
          .build()
      )
      .withLifecycle(
        new LifecycleBuilder()
          .withPreStop(new LifecycleHandlerBuilder().withExec(exec("sleep", "2")).build())
          .build()
      )
      .build()

    val config = new VolumeBuilder()
      .withName("config")
      .withSecret(
        new SecretVolumeSourceBuilder()
          .withSecretName(Names.secret(pipeline, s.name))
          .withDefaultMode(SecretFileMode)
          .build()
      )
      .build()

    val template = new PodTemplateSpecBuilder()
      .withMetadata(
        new ObjectMetaBuilder()
          .withLabels(labels.asJava)
          .withAnnotations(Map(Labels.ConfigHash -> hash).asJava)
          .build()
      )
      .withSpec(
        new PodSpecBuilder()
          .withServiceAccountName(Names.serviceAccount(pipeline))
          .withAutomountServiceAccountToken(false)
          .withTerminationGracePeriodSeconds(30L)
          .withContainers(streamlet)
          .withVolumes(config)
          .build()
      )
      .build()

    new DeploymentBuilder()
      .withMetadata(
        new ObjectMetaBuilder()
          .withName(Names.deployment(pipeline, s.name))
          .withNamespace(resource.getMetadata.getNamespace)
          .withLabels(labels.asJava)
          .withOwnerReferences(owner)
          .build()
      )
      .withSpec(
        new DeploymentSpecBuilder()
          .withReplicas(s.replicas)
          .withSelector(
            new LabelSelectorBuilder()
              .withMatchLabels(Labels.selector(pipeline, s.name).asJava)
              .build()
          )
          .withStrategy(
            new DeploymentStrategyBuilder()
              .withType("RollingUpdate")
              .withRollingUpdate(
                new RollingUpdateDeploymentBuilder()
                  .withMaxSurge(new IntOrString(1))
                  .withMaxUnavailable(new IntOrString(0))
                  .build()
              )
              .build()
          )
          .withTemplate(template)
          .build()
      )
      .build()
