package com.thinkmorestupidless.ankka.flow.blueprint

import scala.jdk.CollectionConverters.*
import scala.util.Try

import com.thinkmorestupidless.ankka.flow.protocol.{DescriptorValidation, Json}
import com.typesafe.config.{Config, ConfigFactory}

/**
 * Deploy-time configuration (`flow generate --conf`), merged over the blueprint by the CLI so the
 * emitted resource says what will run (FR-021, research R12):
 *
 * {{{
 * flow.topics.valid-carts { partitions = 12, topic { retention.ms = 604800000 } }
 * flow.streamlets.router  { replicas = 3, config { review-threshold = 250 } }
 * }}}
 */
final case class Overrides(config: Config):

  private def section(key: String): Map[String, Config] =
    if !config.hasPath(key) then Map.empty
    else config.getObject(key).keySet.asScala.map(k => k -> config.getConfig(s"$key.\"$k\"")).toMap

  val topics: Map[String, Config]     = section("flow.topics")
  val streamlets: Map[String, Config] = section("flow.streamlets")

  /** Names the overrides use that the blueprint does not declare. */
  def unknownNames(verified: VerifiedBlueprint): Vector[String] =
    val topicIds = verified.topics.map(_.id).toSet
    val names    = verified.streamlets.map(_.name).toSet
    (topics.keySet -- topicIds).toVector.sorted.map(id => s"overrides name unknown topic '$id'") ++
      (streamlets.keySet -- names).toVector.sorted.map(n =>
        s"overrides name unknown streamlet '$n'"
      )

  /** A topic's settings: the overrides over the blueprint's. */
  def topicConfig(topic: VerifiedTopic): Config =
    topics.get(topic.id).fold(topic.kafkaConfig)(_.withFallback(topic.kafkaConfig))

  def cluster(topic: VerifiedTopic): Option[String] =
    topics
      .get(topic.id)
      .filter(_.hasPath(Blueprint.ClusterKey))
      .map(_.getString(Blueprint.ClusterKey))
      .orElse(topic.cluster)

  def replicas(streamlet: String): Either[String, Int] =
    streamlets.get(streamlet).filter(_.hasPath("replicas")) match
      case None => Right(1)
      case Some(c) =>
        Try(c.getInt("replicas")).toEither.left
          .map(_ => s"streamlet '$streamlet': replicas must be a number")
          .filterOrElse(_ >= 0, s"streamlet '$streamlet': replicas must not be negative")

  /**
   * Every declared parameter of a streamlet, typed: the override, else the descriptor's default. A
   * required parameter with neither, or a value that is not its type, is a problem.
   */
  def parameters(streamlet: VerifiedStreamlet): Either[Vector[String], Vector[(String, Json)]] =
    val supplied =
      streamlets.get(streamlet.name).filter(_.hasPath("config")).map(_.getConfig("config"))
    val declared = streamlet.descriptor.proto.configParameters
    val unknown = supplied.toVector
      .flatMap(_.root.keySet.asScala.toVector.sorted)
      .filterNot(k => declared.exists(_.key == k))
      .map(k => s"streamlet '${streamlet.name}': parameter '$k' is not declared")
    val resolved = declared.map { p =>
      val value = supplied.filter(_.hasPath(p.key)).map(_.getValue(p.key).unwrapped.toString)
      value.orElse(Option.when(p.defaultValue.nonEmpty)(p.defaultValue)) match
        case None =>
          Left(s"streamlet '${streamlet.name}': parameter '${p.key}' has no default and no value")
        case Some(v) =>
          DescriptorValidation
            .parseValue(p.`type`, v)
            .left
            .map(_ =>
              s"streamlet '${streamlet.name}': parameter '${p.key}' = $v is not a ${DescriptorValidation.typeName(p.`type`)}"
            )
            .map(p.key -> _)
    }
    val problems = unknown ++ resolved.collect { case Left(e) => e }
    if problems.nonEmpty then Left(problems)
    else Right(resolved.collect { case Right(kv) => kv }.toVector)

object Overrides:
  val empty: Overrides = Overrides(ConfigFactory.empty())

  def parse(text: String): Either[String, Overrides] =
    Try(Overrides(ConfigFactory.parseString(text).resolve())).toEither.left.map(_.getMessage)
