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
    /**
     * The Secrets built-in streamlets name in their `secret` parameter, by name, from the
     * resource's namespace; absent when the Secret does not exist.
     */
    secrets: Map[String, SecretState] = Map.empty,
    /** Problems reading those Secrets, by Secret name, reported as refusals. */
    secretProblems: Map[String, String] = Map.empty
)

/** What rendering may know of a stage's Secret: its version and its keys, never its values. */
final case class SecretState(resourceVersion: String, keys: Set[String])

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
