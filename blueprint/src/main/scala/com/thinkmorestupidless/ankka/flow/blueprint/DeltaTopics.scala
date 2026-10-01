package com.thinkmorestupidless.ankka.flow.blueprint

import com.thinkmorestupidless.ankka.flow.protocol.Builtins

/**
 * Delta topics (feature 003): a topic with any port, producing or consuming, of the graph delta
 * contract. Its latest record per element is the graph, so a pipeline's own delta topic is
 * compacted unless the blueprint or the deploy-time configuration says otherwise. The decision is
 * made here, from a verified topic and its merged settings, so the CLI can say it in a note and
 * write it into the resource: the resource says what runs.
 */
object DeltaTopics:

  /** The Kafka topic setting that decides what a topic keeps. */
  val CleanupPolicy = "cleanup.policy"

  val Compact = "compact"

  enum Decision:
    /** Managed, with no cleanup policy of its own: the CLI sets `cleanup.policy = compact`. */
    case Compacted

    /** Managed, with a cleanup policy set by the blueprint or `--conf`: kept as set. */
    case Kept(policy: String)

    /** Not managed: never created or altered; whether it is compacted is its owner's business. */
    case NotOurs

  /** The policies a `cleanup.policy` value names: `compact,delete` is both. */
  def policies(policy: String): Set[String] =
    policy.split(',').iterator.map(_.trim).filter(_.nonEmpty).toSet

  /** Whether a `cleanup.policy` value keeps every key's last record. */
  def compacts(policy: String): Boolean = policies(policy).contains(Compact)

  def carriesDeltas(topic: VerifiedTopic): Boolean =
    topic.connections.exists(_.schemaDescriptor.name == Builtins.GraphDeltaSchema)

  /**
   * What becomes of a topic's cleanup policy, or `None` for a topic that carries no graph deltas.
   * `topicConfig` is the topic's Kafka settings after the deploy-time overrides.
   */
  def decide(topic: VerifiedTopic, topicConfig: Map[String, String]): Option[Decision] =
    if !carriesDeltas(topic) then None
    else if !topic.managed then Some(Decision.NotOurs)
    else Some(topicConfig.get(CleanupPolicy).fold(Decision.Compacted)(Decision.Kept(_)))

  /** What `flow verify` and `flow generate` say about a delta topic. */
  def note(topic: VerifiedTopic, decision: Decision): String =
    val subject = s"Topic '${topic.id}' carries graph deltas"
    decision match
      case Decision.NotOurs =>
        s"$subject and is not managed; whether it is compacted is its owner's."
      case Decision.Compacted =>
        s"$subject and is compacted ($CleanupPolicy = $Compact)."
      case Decision.Kept(policy) if policies(policy) == Set(Compact) =>
        s"$subject and is compacted ($CleanupPolicy = $Compact)."
      case Decision.Kept(policy) if compacts(policy) =>
        s"$subject and sets $CleanupPolicy = $policy; records older than its retention are gone from a rebuild."
      case Decision.Kept(policy) =>
        s"$subject and sets $CleanupPolicy = $policy; it will not hold the whole graph and cannot be relied on to rebuild it."
