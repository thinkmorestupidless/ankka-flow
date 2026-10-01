package com.thinkmorestupidless.ankka.flow.sidecar

import java.nio.file.Path

import scala.jdk.CollectionConverters.*
import scala.util.Try

import ankka.flow.v1.discovery.StreamletDescriptor
import com.thinkmorestupidless.ankka.flow.protocol.{Builtins, DescriptorValidation, Json}
import com.typesafe.config.{Config, ConfigFactory, ConfigValueType}

/**
 * Batching per inlet: a batch is what arrived while the previous one was in flight, capped
 * (research R10).
 */
final case class BatchSettings(maxRecords: Int, maxBytes: Long)

object BatchSettings:
  val Default: BatchSettings = BatchSettings(100, 1024L * 1024)

final case class InletConfig(
    name: String,
    topic: String,
    group: String,
    clientId: String,
    bootstrapServers: String,
    connectionConfig: Map[String, String],
    consumerConfig: Map[String, String],
    batch: BatchSettings
)

final case class OutletConfig(
    name: String,
    topic: String,
    clientId: String,
    bootstrapServers: String,
    connectionConfig: Map[String, String],
    producerConfig: Map[String, String]
)

/** The Neo4j settings of the `neo4j-merge-sink` stage: where its credentials are mounted. */
final case class Neo4jStageConfig(credentialsDir: String)

/**
 * `flow.stage`: present when the sidecar runs a built-in stage instead of talking to a process
 * (feature 002). `name` is the built-in descriptor's name.
 */
final case class StageConfig(name: String, neo4j: Option[Neo4jStageConfig])

/**
 * `streamlet.conf`: what one pod does. Rendered by the operator into the streamlet's Secret, or
 * written by hand beside a compose file (contracts/sidecar.md).
 */
final case class StreamletConfig(
    pipeline: String,
    streamlet: String,
    config: Config,
    inlets: Map[String, InletConfig],
    outlets: Map[String, OutletConfig],
    stage: Option[StageConfig] = None
):

  /**
   * Every declared parameter, typed, as the JSON `Start.config_json` carries: the configured value,
   * else the declared default. A required parameter with neither is a problem.
   */
  def configJson(descriptor: StreamletDescriptor): Either[Vector[String], String] =
    val resolved = descriptor.configParameters.map { p =>
      val configured =
        if config.hasPath(p.key) then Some(config.getValue(p.key).unwrapped.toString) else None
      configured.orElse(Option.when(p.defaultValue.nonEmpty)(p.defaultValue)) match
        case None => Left(s"parameter '${p.key}' has no default and no value")
        case Some(v) =>
          DescriptorValidation
            .parseValue(p.`type`, v)
            .left
            .map(e => s"parameter '${p.key}': $e")
            .map(p.key -> _)
    }
    val problems = resolved.collect { case Left(e) => e }.toVector
    if problems.nonEmpty then Left(problems)
    else Right(Json.compact(Json.Obj(resolved.collect { case Right(kv) => kv }.toVector)))

  /** The inlet and outlet names must be exactly the descriptor's ports. */
  def check(descriptor: StreamletDescriptor): Vector[String] =
    def names(kind: String, configured: Set[String], declared: Set[String]) =
      (declared -- configured).toVector.sorted.map(n =>
        s"$kind '$n' is declared but streamlet.conf does not configure it"
      ) ++
        (configured -- declared).toVector.sorted.map(n =>
          s"streamlet.conf configures $kind '$n', which the descriptor does not declare"
        )
    names("inlet", inlets.keySet, descriptor.inlets.map(_.name).toSet) ++
      names("outlet", outlets.keySet, descriptor.outlets.map(_.name).toSet) ++
      configJson(descriptor).left.toSeq.flatten ++
      stage.toVector.flatMap(checkStage)

  private def checkStage(s: StageConfig): Vector[String] =
    Builtins.byName(s.name) match
      case None =>
        Vector(
          s"stage '${s.name}' is not built into this sidecar; it has: ${Builtins.names.mkString(", ")}"
        )
      case Some(_) if s.name == Builtins.neo4jMergeSink.getStreamlet.name && s.neo4j.isEmpty =>
        Vector(s"stage '${s.name}' needs flow.stage.neo4j.credentials-dir")
      case Some(_) => Vector.empty

object StreamletConfig:

  def load(file: Path): Either[Vector[String], StreamletConfig] =
    Try(ConfigFactory.parseFile(file.toFile).resolve()).toEither.left
      .map(e => Vector(s"${file.getFileName}: ${e.getMessage}"))
      .flatMap(c => parse(c).left.map(_.map(p => s"${file.getFileName}: $p")))

  def parseString(text: String): Either[Vector[String], StreamletConfig] =
    Try(ConfigFactory.parseString(text).resolve()).toEither.left
      .map(e => Vector(e.getMessage))
      .flatMap(parse)

  def parse(root: Config): Either[Vector[String], StreamletConfig] =
    Try {
      val c         = root.getConfig("flow")
      val pipeline  = c.getString("pipeline")
      val streamlet = c.getString("streamlet")
      val config    = if c.hasPath("config") then c.getConfig("config") else ConfigFactory.empty()
      def section(key: String) =
        if c.hasPath(key) then
          c.getObject(key).keySet.asScala.toVector.sorted.map(n => n -> c.getConfig(s"$key.\"$n\""))
        else Vector.empty
      val inlets = section("inlets").map { (name, i) =>
        name -> InletConfig(
          name = name,
          topic = i.getString("topic"),
          group = str(i, "group", s"$pipeline.$streamlet.$name"),
          clientId = str(i, "client-id", s"$pipeline.$streamlet.$name"),
          bootstrapServers = i.getString("bootstrap.servers"),
          connectionConfig = props(i, "connection-config"),
          consumerConfig = props(i, "consumer-config"),
          batch = batch(i)
        )
      }.toMap
      val outlets = section("outlets").map { (name, o) =>
        name -> OutletConfig(
          name = name,
          topic = o.getString("topic"),
          clientId = str(o, "client-id", s"$pipeline.$streamlet.$name"),
          bootstrapServers = o.getString("bootstrap.servers"),
          connectionConfig = props(o, "connection-config"),
          producerConfig = props(o, "producer-config")
        )
      }.toMap
      val stage = Option.when(c.hasPath("stage"))(c.getConfig("stage")).map { s =>
        StageConfig(
          name = s.getString("name"),
          neo4j = Option
            .when(s.hasPath("neo4j.credentials-dir"))(s.getString("neo4j.credentials-dir"))
            .map(Neo4jStageConfig(_))
        )
      }
      StreamletConfig(pipeline, streamlet, config, inlets, outlets, stage)
    }.toEither.left.map(e => Vector(e.getMessage))

  private def str(c: Config, key: String, default: String) =
    if c.hasPath(key) then c.getString(key) else default

  /** A HOCON block as flat Kafka properties: `security.protocol`, `sasl.jaas.config`, … */
  def props(c: Config, key: String): Map[String, String] =
    if !c.hasPath(key) then Map.empty
    else
      c.getConfig(key)
        .entrySet
        .asScala
        .iterator
        .filterNot(_.getKey.startsWith("flow."))
        .map { e =>
          val v = e.getValue
          e.getKey.replace("\"", "") -> (if v.valueType == ConfigValueType.STRING then
                                           v.unwrapped.toString
                                         else v.render)
        }
        .toMap

  private def batch(i: Config): BatchSettings =
    val d = BatchSettings.Default
    val b = if i.hasPath("batch") then i.getConfig("batch") else ConfigFactory.empty()
    BatchSettings(
      maxRecords = if b.hasPath("max-records") then b.getInt("max-records") else d.maxRecords,
      maxBytes = if b.hasPath("max-bytes") then b.getMemorySize("max-bytes").toBytes else d.maxBytes
    )
