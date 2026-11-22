package com.thinkmorestupidless.ankka.flow.operator

/** What a reconcile saw before deciding: the input `Rendering` reads, never writes. */
final case class Observed(
    /** Streamlet name to its Deployment's state. */
    deployments: Map[String, DeploymentState] = Map.empty,
    /**
     * Deployments labelled with this pipeline, by streamlet label, including ones no longer in the
     * spec.
     */
    labelledStreamlets: Set[String] = Set.empty,
    /** Streamlet name to the number of pods that exist for it. */
    pods: Map[String, Int] = Map.empty,
    clusters: Map[String, KafkaCluster] = Map.empty,
    /** Problems reading cluster Secrets, reported as refusals if a topic needs that cluster. */
    clusterProblems: Map[String, String] = Map.empty,
    /** Kafka topic name to what Kafka says about it; absent when not yet described. */
    topics: Map[String, TopicState] = Map.empty,
    /** The CRD `flinkdeployments.flink.apache.org` exists: the Flink operator is installed. */
    flinkCrd: Boolean = false,
    /** The Secret `flink-state`, when it exists with a `dir`. */
    stateStore: Option[StateStore] = None,
    /** Streamlet name to its `FlinkDeployment`'s state, for those labelled with this pipeline. */
    flinkDeployments: Map[String, FlinkState] = Map.empty,
    /** The `state-epochs` annotation: streamlet name to its state epoch; absent means 1. */
    stateEpochs: Map[String, Int] = Map.empty
):
  def epoch(streamlet: String): Int = stateEpochs.getOrElse(streamlet, 1)

final case class DeploymentState(
    configHash: String,
    image: String,
    replicas: Int,
    readyReplicas: Int,
    updatedReplicas: Int,
    generation: Long,
    observedGeneration: Long
):
  def rolledOut: Boolean = observedGeneration >= generation && updatedReplicas >= replicas

/**
 * A `FlinkDeployment` as the Flink operator reports it: what was applied (image, config hash, the
 * job's desired state) and `status.lifecycleState`, `status.jobStatus.state`,
 * `status.jobManagerDeploymentStatus` and `status.error`.
 */
final case class FlinkState(
    image: String,
    configHash: String,
    desiredState: String = "running",
    lifecycleState: Option[String] = None,
    jobState: Option[String] = None,
    jobManagerStatus: Option[String] = None,
    error: Option[String] = None,
    /** The image of `status.reconciliationStatus.lastStableSpec`: what last ran healthy. */
    lastStableImage: Option[String] = None
)

/** A topic as Kafka describes it. `configs` holds only the keys the resource sets. */
enum TopicState:
  case Missing
  case Exists(partitions: Int, replicationFactor: Int, configs: Map[String, String])
  case Unreachable(reason: String)
