package com.thinkmorestupidless.ankka.flow.operator

import scala.jdk.CollectionConverters.*

import com.thinkmorestupidless.ankka.flow.crd.*
import com.thinkmorestupidless.ankka.flow.protocol.{DescriptorJson, DescriptorValidation, Json}
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

  /** Mode of the stage Secret's files: 0440, readable by the sidecar's group (0) only. */
  val SecretFileMode: Int = Integer.parseInt("440", 8)

  val ProcessPort = 9010
  val MetricsPort = 2050
  val ConfigDir   = "/etc/flow/config"
  val StateDir    = "/tmp/flow"
  val TokenDir    = "/var/run/secrets/kubernetes.io/serviceaccount"

  final case class Rendered(
      actions: Vector[Action],
      resolved: Vector[ResolvedTopic],
      hashes: Map[String, String]
  )

  def render(resource: AnkkaFlow, settings: Settings, observed: Observed, now: String): Rendered =
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
      settings.sidecarImage.fold(
        Vector("no sidecar image is configured: set FLOW_SIDECAR_IMAGE on the operator")
      )(_ => Vector.empty) ++
        topicsResolved.collect { case (_, Left(e)) => e } ++
        spec.topics.flatMap(t => t.cluster.flatMap(observed.clusterProblems.get)).distinct ++
        spec.streamlets.flatMap(s => descriptorProblems(s)) ++
        spec.streamlets.flatMap(s => BuiltinStages.problems(s, namespace, observed))
    if refusals.nonEmpty then
      val reason = if settings.sidecarImage.isEmpty then "SidecarImageMissing" else "Refused"
      return Rendered(
        refusals.map(p => Action.RecordEvent(reason, Events.Warning, p)) :+
          Action.SetStatus(LifecycleRules.failed(resource, refusals, now)),
        Vector.empty,
        Map.empty
      )
    val sidecarImage = settings.sidecarImage.get
    val topics       = topicsResolved.collect { case (_, Right(t)) => t }.toVector
    val byId         = topics.map(t => t.id -> t).toMap

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

    // ── 3. Per pipeline: the identity the sidecars post their stall warnings with ────────────
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

    // ── 4. Per streamlet: the Secret with its two files, and the Deployment ───────────────────
    def secretVersion(s: StreamletSpec) =
      Option
        .when(s.builtin)(BuiltinStages.secretName(s).flatMap(observed.secrets.get))
        .flatten
        .map(_.resourceVersion)
    val files = spec.streamlets
      .map(s => s.name -> StreamletFiles.render(pipeline, s, byId, secretVersion(s)))
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
    val streamletActions = spec.streamlets.toVector.flatMap { s =>
      val f          = files(s.name).toOption.get
      val secret     = secretFor(resource, s, f, owner)
      val deployment = deploymentFor(resource, s, f.hash, sidecarImage, owner)
      val rolled = observed.deployments
        .get(s.name)
        .filter(d => d.configHash != f.hash || d.image != s.image)
        .map { _ =>
          Action.RecordEvent(
            "StreamletRolled",
            Events.Normal,
            s"streamlet '${s.name}' rolls out: its image, descriptor or configuration changed"
          )
        }
      Vector(Action.EnsureSecret(secret), Action.ApplyDeployment(deployment)) ++ rolled
    }
    val removed =
      (observed.labelledStreamlets -- spec.streamlets.map(_.name)).toVector.sorted.flatMap { name =>
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

    // ── 6. Finalizer, when managed topics are to go with the pipeline ────────────────────────
    val finalizer =
      Option.when(spec.onDelete.managedTopics == OnDelete.Delete)(Action.EnsureFinalizer).toVector

    val status = LifecycleRules.status(resource, hashes, topics, observed, now)
    Rendered(
      finalizer ++ topicActions ++
        Vector(
          Action.EnsureServiceAccount(account),
          Action.EnsureRole(role),
          Action.EnsureRoleBinding(binding)
        ) ++
        streamletActions ++ removed ++ reset :+ Action.SetStatus(status),
      topics,
      hashes
    )

  private def descriptorProblems(s: StreamletSpec): Vector[String] =
    Option(s.descriptor) match
      case None => Vector(s"streamlet '${s.name}' has no descriptor")
      case Some(node) =>
        Json.parse(node.toString).flatMap(DescriptorJson.streamletFromJson) match
          case Left(e) => Vector(s"streamlet '${s.name}': its descriptor does not parse: $e")
          case Right(d) =>
            val own =
              DescriptorValidation.validateStreamlet(d).map(p => s"streamlet '${s.name}': $p")
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
      .withStringData(
        Map("descriptor.json" -> f.descriptorJson, "streamlet.conf" -> f.streamletConf).asJava
      )
      .build()

  def deploymentFor(
      resource: AnkkaFlow,
      s: StreamletSpec,
      hash: String,
      sidecarImage: String,
      owner: OwnerReference
  ): Deployment =
    val pipeline = resource.getSpec.pipeline
    val labels   = Labels.streamlet(pipeline, s.name)
    def env(name: String, value: String) =
      new EnvVarBuilder().withName(name).withValue(value).build()
    def fieldEnv(name: String, path: String) =
      new EnvVarBuilder()
        .withName(name)
        .withValueFrom(
          new EnvVarSourceBuilder()
            .withFieldRef(new ObjectFieldSelectorBuilder().withFieldPath(path).build())
            .build()
        )
        .build()
    def exec(command: String*) = new ExecActionBuilder().withCommand(command*).build()
    // A built-in streamlet's stage reads its connection from this Secret, in the pod's namespace.
    val stageSecret = Option.when(s.builtin)(BuiltinStages.secretName(s)).flatten

    val sidecar = new ContainerBuilder()
      .withName("sidecar")
      .withImage(sidecarImage)
      .withImagePullPolicy("IfNotPresent")
      .withEnv(
        (Option.unless(s.builtin)(env("FLOW_PROCESS_ADDRESS", s"127.0.0.1:$ProcessPort")).toSeq ++
          Seq(
            env("FLOW_CONFIG_DIR", ConfigDir),
            env("FLOW_STATE_DIR", StateDir),
            env("FLOW_METRICS_PORT", MetricsPort.toString),
            fieldEnv("FLOW_POD_NAME", "metadata.name"),
            fieldEnv("FLOW_POD_NAMESPACE", "metadata.namespace")
          ))*
      )
      .withPorts(
        new ContainerPortBuilder().withName("metrics").withContainerPort(MetricsPort).build()
      )
      .withVolumeMounts(
        (Seq(
          new VolumeMountBuilder()
            .withName("config")
            .withMountPath(ConfigDir)
            .withReadOnly(true)
            .build(),
          new VolumeMountBuilder()
            .withName("api-token")
            .withMountPath(TokenDir)
            .withReadOnly(true)
            .build()
        ) ++ stageSecret.map(_ =>
          new VolumeMountBuilder()
            .withName("neo4j")
            .withMountPath(BuiltinStages.CredentialsDir)
            .withReadOnly(true)
            .build()
        ))*
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

    // The developer's container: its port, and nothing else. No probes, no mounts, no secrets,
    // no API token (FR-021): its liveness is the sidecar's opinion.
    val process = new ContainerBuilder()
      .withName("process")
      .withImage(s.image)
      .withImagePullPolicy("IfNotPresent")
      .withEnv(env("FLOW_PROCESS_PORT", ProcessPort.toString))
      .build()

    // The pod's token is not automounted, because that would mount it into every container; the
    // sidecar alone gets a projected token for posting its stall warnings.
    val token = new VolumeBuilder()
      .withName("api-token")
      .withProjected(
        new ProjectedVolumeSourceBuilder()
          .withSources(
            new VolumeProjectionBuilder()
              .withServiceAccountToken(
                new ServiceAccountTokenProjectionBuilder()
                  .withPath("token")
                  .withExpirationSeconds(3607L)
                  .build()
              )
              .build(),
            new VolumeProjectionBuilder()
              .withConfigMap(
                new ConfigMapProjectionBuilder()
                  .withName("kube-root-ca.crt")
                  .withItems(new KeyToPathBuilder().withKey("ca.crt").withPath("ca.crt").build())
                  .build()
              )
              .build(),
            new VolumeProjectionBuilder()
              .withDownwardAPI(
                new DownwardAPIProjectionBuilder()
                  .withItems(
                    new DownwardAPIVolumeFileBuilder()
                      .withPath("namespace")
                      .withFieldRef(
                        new ObjectFieldSelectorBuilder().withFieldPath("metadata.namespace").build()
                      )
                      .build()
                  )
                  .build()
              )
              .build()
          )
          .build()
      )
      .build()
    val config = new VolumeBuilder()
      .withName("config")
      .withSecret(
        new SecretVolumeSourceBuilder().withSecretName(Names.secret(pipeline, s.name)).build()
      )
      .build()

    // 0440: the sidecar image runs as uid 1001 in group 0, and the kubelet writes Secret files as
    // root:root, so group-readable is what lets the sidecar read them and no one else.
    val stageVolume = stageSecret.map(name =>
      new VolumeBuilder()
        .withName("neo4j")
        .withSecret(
          new SecretVolumeSourceBuilder()
            .withSecretName(name)
            .withDefaultMode(SecretFileMode)
            .build()
        )
        .build()
    )

    val template = new PodTemplateSpecBuilder()
      .withMetadata(
        new ObjectMetaBuilder()
          .withLabels(labels.asJava)
          .withAnnotations(
            Map(
              Labels.ConfigHash      -> hash,
              "prometheus.io/scrape" -> "true",
              "prometheus.io/port"   -> MetricsPort.toString
            ).asJava
          )
          .build()
      )
      .withSpec(
        new PodSpecBuilder()
          .withServiceAccountName(Names.serviceAccount(pipeline))
          .withAutomountServiceAccountToken(false)
          .withTerminationGracePeriodSeconds(30L)
          // A built-in streamlet has no process: the sidecar runs its stage.
          .withContainers((if s.builtin then Seq(sidecar) else Seq(sidecar, process))*)
          .withVolumes((Seq(config, token) ++ stageVolume)*)
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
