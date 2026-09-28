package com.thinkmorestupidless.ankka.flow.cli

import scala.jdk.CollectionConverters.*
import scala.util.Try

import com.thinkmorestupidless.ankka.flow.crd.{AnkkaFlow, FlowSerialization, ResetRequest}
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientBuilder}

/** Records a reset request on a pipeline, for the operator to carry out (FR-023). */
trait Reset:
  def request(
      pipeline: String,
      streamlets: List[String],
      namespace: Option[String]
  ): Either[Vector[String], String]

object Reset:
  val kubernetes: Reset = new KubernetesReset(() =>
    new KubernetesClientBuilder().withKubernetesSerialization(FlowSerialization()).build()
  )

final class KubernetesReset(newClient: () => KubernetesClient) extends Reset:

  def request(
      pipeline: String,
      streamlets: List[String],
      namespace: Option[String]
  ): Either[Vector[String], String] =
    Try(newClient()).toEither.left
      .map(e => Vector(s"cannot reach Kubernetes: ${e.getMessage}"))
      .flatMap { client =>
        try
          val ns    = namespace.getOrElse(client.getNamespace)
          val flows = client.resources(classOf[AnkkaFlow]).inNamespace(ns)
          Option(flows.withName(pipeline).get()) match
            case None => Left(Vector(s"no pipeline '$pipeline' in namespace '$ns'"))
            case Some(flow) =>
              val pods = client.pods
                .inNamespace(ns)
                .withLabel(s"flow.ankka.thinkmorestupidless.com/pipeline", flow.getSpec.pipeline)
                .list()
                .getItems
                .asScala
                .toVector
                .flatMap(p =>
                  Option(p.getMetadata.getLabels).flatMap(l =>
                    Option(l.get("flow.ankka.thinkmorestupidless.com/streamlet"))
                  )
                )
                .groupBy(identity)
                .view
                .mapValues(_.size)
                .toMap
              ResetGuards.check(flow, streamlets, pods).map { request =>
                flows
                  .withName(pipeline)
                  .edit((current: AnkkaFlow) => ResetRequest.withRequest(current, request)): Unit
                request.id
              }
        catch case e: Exception => Left(Vector(s"reset failed: ${e.getMessage}"))
        finally client.close()
      }
