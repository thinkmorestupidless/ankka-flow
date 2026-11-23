package com.thinkmorestupidless.ankka.flow.blueprint

import scala.jdk.CollectionConverters.*
import scala.util.Try

import com.thinkmorestupidless.ankka.flow.descriptor.{DescriptorValidation, Engine, Json}
import com.typesafe.config.{Config, ConfigFactory}

/**
 * Deploy-time configuration (`flow generate --conf`), merged over the blueprint by the CLI so the
 * emitted resource says what will run (FR-021, research R12):
 *
 * {{{
 * flow.topics.valid-carts { partitions = 12, topic { retention.ms = 604800000 } }
 * flow.streamlets.router  { replicas = 3, config { review-threshold = 250 } }
 * flow.streamlets.counter { parallelism = 2, flink { checkpoint-interval = 10s, task-manager { cpu = 1, memory = 2048m } } }
 * flow.ankka.cluster = pipelines
 * }}}
 *
 * `parallelism` and `flink` are a Flink streamlet's alone; a Flink streamlet's `replicas` is 1
 * (running) or 0 (suspended).
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
      .orElse(topic.clusterFor(ankkaCluster))

  def replicas(streamlet: VerifiedStreamlet): Either[String, Int] =
    val name = streamlet.name
    streamlets.get(name).filter(_.hasPath("replicas")) match
      case None => Right(1)
      case Some(c) =>
        Try(c.getInt("replicas")).toEither.left
          .map(_ => s"streamlet '$name': replicas must be a number")
          .filterOrElse(_ >= 0, s"streamlet '$name': replicas must not be negative")
          .filterOrElse(
            r => streamlet.descriptor.engine != Engine.Flink || r <= 1,
            s"streamlet '$name': a flink streamlet's replicas may only be 0 or 1"
          )

  /** The Kafka cluster Secret that holds the machine's credential for ankka topics. */
  val ankkaCluster: String =
    if config.hasPath(Overrides.AnkkaClusterKey) then config.getString(Overrides.AnkkaClusterKey)
    else Overrides.DefaultAnkkaCluster

  /** A streamlet's override of a key only another engine takes, as its refusal. */
  private def notAllowed(streamlet: VerifiedStreamlet, key: String): Option[String] =
    val engine = streamlet.descriptor.engine
    Option.when(
      engine != Engine.Flink && streamlets.get(streamlet.name).exists(_.hasPath(key))
    )(BlueprintProblem.toMessage(EngineSettingNotAllowed(streamlet.name, key, engine)))

  /**
   * What the overrides set that no streamlet or topic of this blueprint takes: an engine's setting
   * on a streamlet of another engine, and the removed `consumer-config.flow.batch`.
   */
  def settingProblems(verified: VerifiedBlueprint): Vector[String] =
    val engine = verified.streamlets.sortBy(_.name).flatMap { s =>
      Overrides.FlinkOnly.flatMap(notAllowed(s, _))
    }
    val batch = topics.toVector.sortBy(_._1).collect {
      case (id, c) if c.hasPath(s"${Blueprint.ConsumerConfigKey}.flow.batch") =>
        BlueprintProblem.toMessage(BatchSettingRemoved(id))
    }
    engine ++ batch

  /** A Flink streamlet's parallelism, when the overrides give one. */
  def parallelism(streamlet: VerifiedStreamlet): Either[String, Option[Int]] =
    notAllowed(streamlet, "parallelism").toLeft(()).flatMap { _ =>
      streamlets.get(streamlet.name).filter(_.hasPath("parallelism")) match
        case None => Right(None)
        case Some(c) =>
          Try(c.getInt("parallelism")).toOption
            .filter(_ >= 1)
            .map(Some(_))
            .toRight(s"streamlet '${streamlet.name}': parallelism must be a number of at least 1")
    }

  /** A Flink streamlet's `flink { ... }` block, when the overrides give one. */
  def flink(streamlet: VerifiedStreamlet): Either[String, Option[FlinkOverrides]] =
    val name = streamlet.name
    notAllowed(streamlet, "flink").toLeft(()).flatMap { _ =>
      streamlets.get(name).filter(_.hasPath("flink")).map(_.getConfig("flink")) match
        case None    => Right(None)
        case Some(f) => FlinkOverrides.read(name, f).map(Some(_))
    }

  /**
   * Every declared parameter of a streamlet, typed: the override, else the descriptor's default. A
   * required parameter with neither, or a value that is not its type, is a problem.
   */
  def parameters(streamlet: VerifiedStreamlet): Either[Vector[String], Vector[(String, Json)]] =
    val supplied =
      streamlets.get(streamlet.name).filter(_.hasPath("config")).map(_.getConfig("config"))
    val declared = streamlet.descriptor.descriptor.streamlet.parameters
    val unknown = supplied.toVector
      .flatMap(_.root.keySet.asScala.toVector.sorted)
      .filterNot(k => declared.exists(_.key == k))
      .map(k => s"streamlet '${streamlet.name}': parameter '$k' is not declared")
    val resolved = declared.map { p =>
      val value = supplied.filter(_.hasPath(p.key)).map(_.getValue(p.key).unwrapped.toString)
      value.orElse(p.default) match
        case None =>
          Left(s"streamlet '${streamlet.name}': parameter '${p.key}' has no default and no value")
        case Some(v) =>
          DescriptorValidation
            .parseValue(p.kind, v)
            .left
            .map(_ =>
              s"streamlet '${streamlet.name}': parameter '${p.key}' = $v is not a ${DescriptorValidation.typeName(p.kind)}"
            )
            .map(p.key -> _)
    }
    val problems = unknown ++ resolved.collect { case Left(e) => e }
    if problems.nonEmpty then Left(problems)
    else Right(resolved.collect { case Right(kv) => kv }.toVector)

object Overrides:
  val AnkkaClusterKey     = "flow.ankka.cluster"
  val DefaultAnkkaCluster = "ankka"

  /** The streamlet settings only a Flink streamlet takes. */
  val FlinkOnly: Vector[String] = Vector("flink", "parallelism")

  val empty: Overrides = Overrides(ConfigFactory.empty())

  def parse(text: String): Either[String, Overrides] =
    Try(Overrides(ConfigFactory.parseString(text).resolve())).toEither.left.map(_.getMessage)

/** A job manager's or task manager's resources, as given: Kubernetes quantities. */
final case class ResourceOverrides(cpu: Option[String], memory: Option[String])

/** `flow.streamlets.<name>.flink { ... }`: what a deploy sets of a Flink streamlet's job. */
final case class FlinkOverrides(
    checkpointInterval: Option[String],
    jobManager: ResourceOverrides,
    taskManager: ResourceOverrides
)

object FlinkOverrides:
  private val Keys         = Set("checkpoint-interval", "job-manager", "task-manager")
  private val ResourceKeys = Set("cpu", "memory")

  def read(streamlet: String, f: Config): Either[String, FlinkOverrides] =
    def problem(what: String) = s"streamlet '$streamlet': flink.$what"
    def unknown(c: Config, allowed: Set[String], at: String) =
      c.root.keySet.asScala.toVector.sorted
        .find(k => !allowed.contains(k))
        .map(k => problem(s"$at$k is not a setting"))
        .toLeft(())
    def resources(key: String): Either[String, ResourceOverrides] =
      if !f.hasPath(key) then Right(ResourceOverrides(None, None))
      else
        val r = f.getConfig(key)
        for
          _ <- unknown(r, ResourceKeys, s"$key.")
          cpu <-
            if !r.hasPath("cpu") then Right(None)
            else
              val text = r.getValue("cpu").unwrapped.toString
              text.toDoubleOption
                .filter(_ > 0)
                .map(_ => Some(text))
                .toRight(problem(s"$key.cpu = $text is not a positive number"))
          memory <-
            if !r.hasPath("memory") then Right(None)
            else
              Try(r.getMemorySize("memory")).toOption
                .map(_ => Some(r.getString("memory")))
                .toRight(
                  problem(s"$key.memory = ${r.getValue("memory").render} is not a memory size")
                )
        yield ResourceOverrides(cpu, memory)
    for
      _ <- unknown(f, Keys, "")
      interval <-
        if !f.hasPath("checkpoint-interval") then Right(None)
        else
          Try(f.getDuration("checkpoint-interval")).toOption
            .map(_ => Some(f.getString("checkpoint-interval")))
            .toRight(
              problem(
                s"checkpoint-interval = ${f.getValue("checkpoint-interval").render} is not a duration"
              )
            )
      jm <- resources("job-manager")
      tm <- resources("task-manager")
    yield FlinkOverrides(interval, jm, tm)
