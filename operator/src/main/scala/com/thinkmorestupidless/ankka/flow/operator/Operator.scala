package com.thinkmorestupidless.ankka.flow.operator

import scala.jdk.CollectionConverters.*

import com.thinkmorestupidless.ankka.flow.crd.AnkkaFlow
import io.fabric8.kubernetes.api.model.apps.Deployment
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.informers.{ResourceEventHandler, SharedIndexInformer}
import org.slf4j.LoggerFactory

/** Informers on pipelines and on the Deployments they own; each change enqueues its pipeline. */
final class Operator(client: KubernetesClient, settings: Settings, reconciler: Reconciler)
    extends AutoCloseable:

  private val log   = LoggerFactory.getLogger(classOf[Operator])
  private val queue = new WorkQueue(settings, reconciler.reconcile)

  private var informers: Vector[SharedIndexInformer[?]] = Vector.empty

  private def refOf(resource: AnkkaFlow): Option[PipelineRef] =
    Option(resource.getMetadata).map(m => PipelineRef(m.getNamespace, m.getName))

  private def refOf(deployment: Deployment): Option[PipelineRef] =
    for
      meta  <- Option(deployment.getMetadata)
      owner <- Option(meta.getOwnerReferences).flatMap(_.asScala.find(_.getKind == "AnkkaFlow"))
    yield PipelineRef(meta.getNamespace, owner.getName)

  private def handler[T](toRef: T => Option[PipelineRef]): ResourceEventHandler[T] =
    new ResourceEventHandler[T]:
      def onAdd(obj: T): Unit                = toRef(obj).foreach(queue.enqueue)
      def onUpdate(old: T, updated: T): Unit = toRef(updated).foreach(queue.enqueue)
      def onDelete(obj: T, deletedFinalStateUnknown: Boolean): Unit =
        toRef(obj).foreach(queue.enqueue)

  def start(): Unit =
    val resync = settings.resyncInterval.toMillis
    val flows = client
      .resources(classOf[AnkkaFlow])
      .inAnyNamespace()
      .inform(handler[AnkkaFlow](refOf), resync)
    val deployments = client.apps.deployments
      .inAnyNamespace()
      .withLabel(Labels.ManagedByKey, Labels.ManagedBy)
      .inform(handler[Deployment](refOf), resync)
    informers = Vector(flows, deployments)
    queue.start()
    log.info(
      "ankka-flow operator {} watching pipelines in every namespace; sidecar image {}",
      BuildInfo.version,
      settings.sidecarImage.getOrElse("(not configured: every pipeline will be refused)")
    )

  def awaitTermination(): Unit =
    try Thread.currentThread().join()
    catch case _: InterruptedException => Thread.currentThread().interrupt()

  def close(): Unit =
    informers.foreach(_.close())
    informers = Vector.empty
    queue.stop()
