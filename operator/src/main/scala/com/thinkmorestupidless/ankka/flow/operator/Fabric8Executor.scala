package com.thinkmorestupidless.ankka.flow.operator

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64

import scala.jdk.CollectionConverters.*
import scala.util.Try

import com.fasterxml.jackson.databind.ObjectMapper
import com.thinkmorestupidless.ankka.flow.crd.AnkkaFlow
import io.fabric8.kubernetes.api.model.{GenericKubernetesResource, Secret}
import io.fabric8.kubernetes.client.dsl.base.ResourceDefinitionContext
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientException}
import org.slf4j.LoggerFactory

/**
 * The only code that writes to Kubernetes. Server-side apply with one field manager, forcing
 * conflicts: the resource is the source of truth for every field the operator owns (ankka's rule).
 */
final class Fabric8Executor(client: KubernetesClient, settings: Settings):

  private val log          = LoggerFactory.getLogger(classOf[Fabric8Executor])
  private val FieldManager = "ankka-flow-operator"

  private def flows(namespace: String) = client.resources(classOf[AnkkaFlow]).inNamespace(namespace)

  private def op(f: AnkkaFlow => AnkkaFlow): java.util.function.UnaryOperator[AnkkaFlow] = a => f(a)

  private def editMeta(
      resource: AnkkaFlow
  )(f: io.fabric8.kubernetes.api.model.ObjectMeta => Unit): Unit =
    flows(resource.getMetadata.getNamespace)
      .withName(resource.getMetadata.getName)
      .edit(op { current =>
        f(current.getMetadata)
        current
      }): Unit

  private def flinkDeployments(namespace: String) =
    client.genericKubernetesResources(Fabric8Executor.FlinkDeploymentContext).inNamespace(namespace)

  /** Whether the Flink operator's CRD is installed. */
  def flinkCrdInstalled(): Boolean =
    Try(
      client.apiextensions.v1.customResourceDefinitions.withName(FlinkRendering.Crd).get()
    ).toOption.exists(_ != null)

  private def stateStore(): Option[StateStore] =
    Try(
      Option(
        client.secrets.inNamespace(settings.clustersNamespace).withName(StateStore.SecretName).get()
      )
    ).toOption.flatten
      .flatMap { secret =>
        StateStore.fromData(decode(secret)) match
          case Right(store) => store
          case Left(problem) =>
            log.warn("{}", problem)
            None
      }

  /** Deployments, FlinkDeployments, pods, Kafka clusters and the state store. */
  def observe(resource: AnkkaFlow): Observed =
    val namespace = resource.getMetadata.getNamespace
    val pipeline  = resource.getSpec.pipeline
    val deployments = client.apps.deployments
      .inNamespace(namespace)
      .withLabel(Labels.PipelineKey, pipeline)
      .withLabel(Labels.ManagedByKey, Labels.ManagedBy)
      .list()
      .getItems
      .asScala
      .toVector
      .filter(d =>
        Option(d.getMetadata.getOwnerReferences)
          .exists(_.asScala.exists(_.getUid == resource.getMetadata.getUid))
      )
    val states = deployments.flatMap { d =>
      Option(d.getMetadata.getLabels).flatMap(l => Option(l.get(Labels.StreamletKey))).map {
        streamlet =>
          val template = d.getSpec.getTemplate
          val status   = Option(d.getStatus)
          streamlet -> DeploymentState(
            configHash = Option(template.getMetadata.getAnnotations)
              .flatMap(a => Option(a.get(Labels.ConfigHash)))
              .getOrElse(""),
            image = template.getSpec.getContainers.asScala
              .find(_.getName == "streamlet")
              .fold("")(_.getImage),
            replicas = Option(d.getSpec.getReplicas).fold(1)(_.intValue),
            readyReplicas = status.flatMap(s => Option(s.getReadyReplicas)).fold(0)(_.intValue),
            updatedReplicas = status.flatMap(s => Option(s.getUpdatedReplicas)).fold(0)(_.intValue),
            generation = Option(d.getMetadata.getGeneration).fold(0L)(_.longValue),
            observedGeneration =
              status.flatMap(s => Option(s.getObservedGeneration)).fold(0L)(_.longValue)
          )
      }
    }
    val pods = client.pods
      .inNamespace(namespace)
      .withLabel(Labels.PipelineKey, pipeline)
      .list()
      .getItems
      .asScala
      .toVector
      .flatMap(p =>
        Option(p.getMetadata.getLabels).flatMap(l => Option(l.get(Labels.StreamletKey)))
      )
      .groupBy(identity)
      .view
      .mapValues(_.size)
      .toMap
    val (clusters, clusterProblems) = kafkaClusters()
    val crd                         = flinkCrdInstalled()
    val flink =
      if !crd then Map.empty[String, FlinkState]
      else
        flinkDeployments(namespace)
          .withLabel(Labels.PipelineKey, pipeline)
          .withLabel(Labels.ManagedByKey, Labels.ManagedBy)
          .list()
          .getItems
          .asScala
          .toVector
          .filter(d =>
            Option(d.getMetadata.getOwnerReferences)
              .exists(_.asScala.exists(_.getUid == resource.getMetadata.getUid))
          )
          .flatMap(d =>
            Option(d.getMetadata.getLabels)
              .flatMap(l => Option(l.get(Labels.StreamletKey)))
              .map(_ -> Fabric8Executor.flinkState(d))
          )
          .toMap
    Observed(
      deployments = states.toMap,
      labelledStreamlets = states.map(_._1).toSet,
      pods = pods,
      clusters = clusters,
      clusterProblems = clusterProblems,
      flinkCrd = crd,
      stateStore = stateStore(),
      flinkDeployments = flink,
      stateEpochs = Fabric8Executor.stateEpochs(resource)
    )

  private def kafkaClusters(): (Map[String, KafkaCluster], Map[String, String]) =
    val secrets = Try(
      client.secrets.inNamespace(settings.clustersNamespace).list().getItems.asScala.toVector
    ).getOrElse(Vector.empty).filter(_.getMetadata.getName.startsWith("kafka-cluster-"))
    val parsed = secrets.map { s =>
      val name = s.getMetadata.getName.stripPrefix("kafka-cluster-")
      name -> KafkaCluster.fromData(name, decode(s))
    }
    (
      parsed.collect { case (n, Right(c)) => n -> c }.toMap,
      parsed.collect { case (n, Left(e)) => n -> e }.toMap
    )

  private def decode(s: Secret): Map[String, String] =
    val data = Option(s.getData)
      .map(_.asScala.toMap)
      .getOrElse(Map.empty)
      .view
      .mapValues(v => new String(Base64.getDecoder.decode(v), UTF_8))
      .toMap
    data ++ Option(s.getStringData).map(_.asScala.toMap).getOrElse(Map.empty)

  def execute(resource: AnkkaFlow, action: Action): Unit =
    val namespace = resource.getMetadata.getNamespace
    action match
      case Action.EnsureSecret(secret) =>
        client.resource(secret).fieldManager(FieldManager).forceConflicts().serverSideApply(): Unit
      case Action.EnsureServiceAccount(account) =>
        client.resource(account).fieldManager(FieldManager).forceConflicts().serverSideApply(): Unit
      case Action.EnsureRole(role) =>
        client.resource(role).fieldManager(FieldManager).forceConflicts().serverSideApply(): Unit
      case Action.EnsureRoleBinding(binding) =>
        client.resource(binding).fieldManager(FieldManager).forceConflicts().serverSideApply(): Unit
      case Action.ApplyDeployment(deployment) =>
        client
          .resource(deployment)
          .fieldManager(FieldManager)
          .forceConflicts()
          .serverSideApply(): Unit
      case Action.DeleteDeployment(ns, name) =>
        client.apps.deployments.inNamespace(ns).withName(name).delete(): Unit
      case Action.ApplyFlinkDeployment(deployment) =>
        flinkDeployments(deployment.getMetadata.getNamespace)
          .resource(deployment)
          .fieldManager(FieldManager)
          .forceConflicts()
          .serverSideApply(): Unit
      case Action.DeleteFlinkDeployment(ns, name) =>
        flinkDeployments(ns).withName(name).delete(): Unit
      case Action.AdvanceStateEpoch(streamlet) =>
        editMeta(resource) { m =>
          val annotations = Option(m.getAnnotations)
            .map(a => new java.util.HashMap[String, String](a))
            .getOrElse(new java.util.HashMap[String, String]())
          val epochs = Fabric8Executor.parseEpochs(Option(annotations.get(Labels.StateEpochs)))
          val next   = epochs.updated(streamlet, epochs.getOrElse(streamlet, 1) + 1)
          annotations.put(Labels.StateEpochs, Fabric8Executor.writeEpochs(next))
          m.setAnnotations(annotations)
        }
      case Action.DeleteSecret(ns, name) =>
        client.secrets.inNamespace(ns).withName(name).delete(): Unit
      case Action.RecordEvent(reason, eventType, note) =>
        val event = Events.build(resource, reason, eventType, note, settings.reportingInstance)
        try client.resource(event).create(): Unit
        catch
          // The same reason and note were already recorded: one event per distinct occurrence.
          case e: KubernetesClientException if e.getCode == 409 => ()
        if eventType == Events.Warning then
          log.warn("{}/{}: {}: {}", namespace, resource.getMetadata.getName, reason, note)
        else log.info("{}/{}: {}: {}", namespace, resource.getMetadata.getName, reason, note)
      case Action.SetStatus(next) =>
        val current  = flows(namespace).withName(resource.getMetadata.getName).get()
        val previous = Option(current).flatMap(c => Option(c.getStatus))
        val status =
          previous
            .filter(_.phase == next.phase)
            .fold(next)(p => next.copy(lastTransitionTime = p.lastTransitionTime))
        // An unchanged report is not written again: a status write is itself an update event.
        if current != null && !previous.exists(_.sameReport(status)) then
          flows(namespace)
            .withName(resource.getMetadata.getName)
            .editStatus(op { c =>
              c.setStatus(status)
              c
            }): Unit
      case Action.MarkResetDone(id) =>
        editMeta(resource) { m =>
          val annotations = Option(m.getAnnotations)
            .map(a => new java.util.HashMap[String, String](a))
            .getOrElse(new java.util.HashMap[String, String]())
          annotations.put(Labels.ResetDone, id)
          m.setAnnotations(annotations)
        }
      case Action.EnsureFinalizer =>
        if !Option(resource.getMetadata.getFinalizers).exists(_.contains(Labels.TopicFinalizer))
        then
          editMeta(resource) { m =>
            val fs = new java.util.ArrayList[String](
              Option(m.getFinalizers).getOrElse(java.util.List.of[String]())
            )
            fs.add(Labels.TopicFinalizer)
            m.setFinalizers(fs)
          }
      case Action.RemoveFinalizer =>
        if Option(resource.getMetadata.getFinalizers).exists(_.contains(Labels.TopicFinalizer)) then
          editMeta(resource) { m =>
            val fs = new java.util.ArrayList[String](
              Option(m.getFinalizers).getOrElse(java.util.List.of[String]())
            )
            fs.remove(Labels.TopicFinalizer)
            m.setFinalizers(fs)
          }
      case other =>
        throw new IllegalArgumentException(s"not a Kubernetes action: ${other.describe}")

object Fabric8Executor:

  /** `flinkdeployments.flink.apache.org`, named so no discovery call is needed. */
  val FlinkDeploymentContext: ResourceDefinitionContext = new ResourceDefinitionContext.Builder()
    .withGroup("flink.apache.org")
    .withVersion("v1beta1")
    .withKind(FlinkRendering.Kind)
    .withPlural("flinkdeployments")
    .withNamespaced(true)
    .build()

  private val json = new ObjectMapper()

  // Read with the build's mapper, whose Scala module gives an untyped object as a Scala map; an
  // object built in this JVM is a Java one.
  private def at(root: Any, path: String*): Option[Any] =
    path.foldLeft(Option(root)) {
      case (Some(m: java.util.Map[?, ?]), key) => Option(m.get(key))
      case (Some(m: scala.collection.Map[?, ?]), key) =>
        m.asInstanceOf[scala.collection.Map[String, Any]].get(key)
      case _ => None
    }

  private def text(root: Any, path: String*): Option[String] =
    at(root, path*).collect { case s: String => s }.filter(_.nonEmpty)

  /** What the operator reads of a `FlinkDeployment`: what it applied, and the status. */
  def flinkState(d: GenericKubernetesResource): FlinkState =
    val props  = d.getAdditionalProperties
    val spec   = props.get("spec")
    val status = props.get("status")
    // `lastStableSpec` is the spec, as JSON text, that last ran healthy.
    val lastStableImage = text(status, "reconciliationStatus", "lastStableSpec").flatMap(t =>
      Try(json.readTree(t)).toOption.flatMap(n => Option(n.get("image"))).map(_.asText)
    )
    FlinkState(
      image = text(spec, "image").getOrElse(""),
      configHash =
        text(spec, "podTemplate", "metadata", "annotations", Labels.ConfigHash).getOrElse(""),
      desiredState = text(spec, "job", "state").getOrElse("running"),
      lifecycleState = text(status, "lifecycleState"),
      jobState = text(status, "jobStatus", "state"),
      jobManagerStatus = text(status, "jobManagerDeploymentStatus"),
      error = text(status, "error"),
      lastStableImage = lastStableImage
    )

  def parseEpochs(text: Option[String]): Map[String, Int] =
    text
      .flatMap(t => Try(json.readTree(t)).toOption)
      .filter(_.isObject)
      .map(
        _.properties.asScala
          .collect { case e if e.getValue.canConvertToInt => e.getKey -> e.getValue.asInt }
          .toMap
      )
      .getOrElse(Map.empty)

  def writeEpochs(epochs: Map[String, Int]): String =
    val node = json.createObjectNode()
    epochs.toVector.sortBy(_._1).foreach((k, v) => node.put(k, v))
    json.writeValueAsString(node)

  def stateEpochs(resource: AnkkaFlow): Map[String, Int] =
    parseEpochs(
      Option(resource.getMetadata.getAnnotations).flatMap(a => Option(a.get(Labels.StateEpochs)))
    )
