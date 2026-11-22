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
    topics: Map[String, TopicState] = Map.empty
)

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

/** A topic as Kafka describes it. `configs` holds only the keys the resource sets. */
enum TopicState:
  case Missing
  case Exists(partitions: Int, replicationFactor: Int, configs: Map[String, String])
  case Unreachable(reason: String)
