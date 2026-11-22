package com.thinkmorestupidless.ankka.flow.operator

import java.util.{LinkedHashMap as JMap, List as JList, TreeMap}

import scala.jdk.CollectionConverters.*

import com.thinkmorestupidless.ankka.flow.crd.{AnkkaFlow, FlinkSpec, Resources, StreamletSpec}
import io.fabric8.kubernetes.api.model.{
  GenericKubernetesResource,
  GenericKubernetesResourceBuilder,
  ObjectMetaBuilder,
  OwnerReference,
  Secret,
  SecretBuilder
}

/**
 * A Flink streamlet as the Flink Kubernetes Operator runs it (research R10): one `FlinkDeployment`
 * in application mode, its job the streamlet's jar, its state under the state store's directory at
 * the streamlet's epoch, upgraded from the last state.
 */
object FlinkRendering:

  val ApiVersion = "flink.apache.org/v1beta1"
  val Kind       = "FlinkDeployment"
  val Crd        = "flinkdeployments.flink.apache.org"

  val JarUri = "local:///opt/flink/usrlib/streamlet.jar"

  /** The container of Flink's pods that the pod template's settings go to. */
  val MainContainer = "flink-main-container"

  /** The `flink` user and group of Flink's image, so the settings Secret's 0440 files are read. */
  val FlinkGroup: Long = 9999L

  /** `2.2` to the Flink operator's `v2_2`. */
  def flinkVersion(version: String): String = "v" + version.replace('.', '_')

  /** The Flink configuration flow sets; the state store's own keys are merged under it. */
  def configuration(
      flink: FlinkSpec,
      paths: StateStore.Paths,
      store: StateStore,
      tokenEndpoints: Vector[String]
  ): Map[String, String] =
    val own = Map(
      "execution.checkpointing.interval"      -> flink.checkpointInterval,
      "execution.checkpointing.mode"          -> "EXACTLY_ONCE",
      "execution.checkpointing.dir"           -> paths.checkpoints,
      "execution.checkpointing.savepoint-dir" -> paths.savepoints,
      "high-availability.type"                -> "kubernetes",
      "high-availability.storageDir"          -> paths.ha,
      "restart-strategy.type"                 -> "exponential-delay"
    ) ++
      // A task manager never runs the main, so kafka-clients 4's allowed token endpoints are
      // given to its JVM here (research V2); the job manager's main sets them itself.
      Option
        .when(tokenEndpoints.nonEmpty)(
          "env.java.opts.taskmanager" ->
            s"-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=${tokenEndpoints.mkString(",")}"
        )
        .toMap
    store.flinkConfig ++ own

  /** `flow-<pipeline>-flink-state`: the state store's `env`, given to Flink's pods. */
  def envSecret(resource: AnkkaFlow, store: StateStore, owner: OwnerReference): Secret =
    val pipeline = resource.getSpec.pipeline
    new SecretBuilder()
      .withMetadata(
        new ObjectMetaBuilder()
          .withName(StateStore.envSecretName(pipeline))
          .withNamespace(resource.getMetadata.getNamespace)
          .withLabels(Labels.pipeline(pipeline).asJava)
          .withOwnerReferences(owner)
          .build()
      )
      .withType("Opaque")
      .withStringData(store.env.asJava)
      .build()

  def deploymentFor(
      resource: AnkkaFlow,
      s: StreamletSpec,
      hash: String,
      settings: Settings,
      store: StateStore,
      epoch: Int,
      tokenEndpoints: Vector[String],
      owner: OwnerReference
  ): GenericKubernetesResource =
    val pipeline  = resource.getSpec.pipeline
    val namespace = resource.getMetadata.getNamespace
    val flink     = s.flink.getOrElse(FlinkSpec())
    val paths     = store.paths(namespace, pipeline, s.name, epoch)

    val container = obj(
      "name" -> MainContainer,
      "env"  -> list(obj("name" -> "FLOW_CONFIG_DIR", "value" -> Rendering.ConfigDir)),
      "envFrom" -> list(
        obj("secretRef" -> obj("name" -> StateStore.envSecretName(pipeline)))
      ),
      "volumeMounts" -> list(
        obj("name" -> "config", "mountPath" -> Rendering.ConfigDir, "readOnly" -> true)
      )
    )
    val podTemplate = obj(
      "apiVersion" -> "v1",
      "kind"       -> "Pod",
      "metadata" -> obj(
        "labels"      -> strings(Labels.streamlet(pipeline, s.name)),
        "annotations" -> strings(Map(Labels.ConfigHash -> hash))
      ),
      "spec" -> obj(
        "securityContext" -> obj("fsGroup" -> FlinkGroup),
        "containers"      -> list(container),
        "volumes" -> list(
          obj(
            "name" -> "config",
            "secret" -> obj(
              "secretName"  -> Names.secret(pipeline, s.name),
              "defaultMode" -> Rendering.SecretFileMode
            )
          )
        )
      )
    )
    val spec = obj(
      "image"              -> s.image,
      "imagePullPolicy"    -> "IfNotPresent",
      "flinkVersion"       -> flinkVersion(flink.version),
      "serviceAccount"     -> settings.flinkServiceAccount,
      "flinkConfiguration" -> sorted(configuration(flink, paths, store, tokenEndpoints)),
      "jobManager"         -> obj("replicas" -> 1, "resource" -> resources(flink.jobManager)),
      "taskManager"        -> obj("resource" -> resources(flink.taskManager)),
      "podTemplate"        -> podTemplate,
      "job" -> obj(
        "jarURI"      -> JarUri,
        "entryClass"  -> flink.entryClass,
        "parallelism" -> s.parallelism.getOrElse(1),
        "upgradeMode" -> "last-state",
        "state"       -> (if s.replicas == 0 then "suspended" else "running")
      )
    )
    new GenericKubernetesResourceBuilder()
      .withApiVersion(ApiVersion)
      .withKind(Kind)
      .withMetadata(
        new ObjectMetaBuilder()
          .withName(Names.deployment(pipeline, s.name))
          .withNamespace(namespace)
          .withLabels(Labels.streamlet(pipeline, s.name).asJava)
          .withOwnerReferences(owner)
          .build()
      )
      .withAdditionalProperties(Map[String, AnyRef]("spec" -> spec).asJava)
      .build()

  private def resources(r: Resources): JMap[String, AnyRef] =
    obj("cpu" -> java.lang.Double.valueOf(r.cpu.toDouble), "memory" -> r.memory)

  private def obj(fields: (String, Any)*): JMap[String, AnyRef] =
    val m = new JMap[String, AnyRef]()
    fields.foreach((k, v) => m.put(k, box(v)))
    m

  private def box(v: Any): AnyRef = v match
    case i: Int     => java.lang.Integer.valueOf(i)
    case l: Long    => java.lang.Long.valueOf(l)
    case b: Boolean => java.lang.Boolean.valueOf(b)
    case d: Double  => java.lang.Double.valueOf(d)
    case other      => other.asInstanceOf[AnyRef]

  private def list(items: AnyRef*): JList[AnyRef] = items.toList.asJava

  private def strings(m: Map[String, String]): JMap[String, AnyRef] = obj(m.toSeq.sortBy(_._1)*)

  private def sorted(m: Map[String, String]): TreeMap[String, AnyRef] =
    val t = new TreeMap[String, AnyRef]()
    m.foreach((k, v) => t.put(k, v))
    t

  /** Every token endpoint the streamlet's topics name, in order and once. */
  def tokenEndpoints(s: StreamletSpec, topics: Map[String, ResolvedTopic]): Vector[String] =
    val key = "sasl.oauthbearer.token.endpoint.url"
    (s.inlets.toVector.sortBy(_._1) ++ s.outlets.toVector.sortBy(_._1))
      .flatMap((_, id) => topics.get(id))
      .flatMap(t => (t.connectionConfig ++ t.consumerConfig ++ t.producerConfig).get(key))
      .distinct
