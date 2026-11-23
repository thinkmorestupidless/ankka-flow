package com.thinkmorestupidless.ankka.flow.operator

import com.thinkmorestupidless.ankka.flow.crd.{AnkkaFlowDefinition, ResetRequest}

/** Labels and annotations, under `flow.ankka.thinkmorestupidless.com/`. */
object Labels:
  private val d = AnkkaFlowDefinition.domain

  val ManagedByKey   = "app.kubernetes.io/managed-by"
  val ManagedBy      = "ankka-flow"
  val NameKey        = "app.kubernetes.io/name"
  val PipelineKey    = s"$d/pipeline"
  val StreamletKey   = s"$d/streamlet"
  val KafkaCluster   = s"$d/kafka-cluster"
  val ConfigHash     = s"$d/config-hash"
  val ResetRequest   = s"$d/reset-offsets"
  val ResetDone      = s"$d/reset-offsets-done"
  val TopicFinalizer = s"$d/managed-topics"

  /** A JSON object, streamlet name to its state epoch; absent, every streamlet is at 1 (R12). */
  val StateEpochs = s"$d/state-epochs"

  def pipeline(p: String): Map[String, String] = Map(ManagedByKey -> ManagedBy, PipelineKey -> p)

  def streamlet(p: String, s: String): Map[String, String] =
    pipeline(p) ++ Map(StreamletKey -> s, NameKey -> s"$p-$s")

  /** What a Deployment's selector matches on. Never changes for a streamlet. */
  def selector(p: String, s: String): Map[String, String] = Map(PipelineKey -> p, StreamletKey -> s)

/** Names of everything the operator renders, and the Kafka identities (FR-015). */
object Names:
  def deployment(pipeline: String, streamlet: String) = s"flow-$pipeline-$streamlet"
  def secret(pipeline: String, streamlet: String)     = s"flow-$pipeline-$streamlet"
  def serviceAccount(pipeline: String)                = s"flow-$pipeline"
  def kafkaClusterSecret(cluster: String)             = s"kafka-cluster-$cluster"
  def group(pipeline: String, streamlet: String, inlet: String, prefix: Option[String] = None) =
    ResetRequest.groupId(pipeline, streamlet, inlet, prefix)
  def clientId(pipeline: String, streamlet: String, port: String) = s"$pipeline.$streamlet.$port"

  val DefaultCluster = "default"

  /** The cluster an ankka topic that names none is reached through. */
  val AnkkaCluster = "ankka"
