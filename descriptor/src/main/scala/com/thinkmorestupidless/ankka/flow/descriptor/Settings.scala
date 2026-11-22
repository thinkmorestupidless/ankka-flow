package com.thinkmorestupidless.ankka.flow.descriptor

import java.io.StringReader
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.Properties
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** How an outlet's records are delivered. */
enum Delivery(val wire: String):
  case AtLeastOnce extends Delivery("at-least-once")
  case ExactlyOnce extends Delivery("exactly-once")

object Delivery:
  def fromWire(s: String): Option[Delivery] = values.find(_.wire == s)

/**
 * One inlet's settings: its topic, its consumer group and client id, and its consumer properties.
 */
final case class InletSettings(
    port: String,
    topic: String,
    group: String,
    clientId: String,
    kafka: Map[String, String]
)

/** One outlet's settings: its topic, client id, delivery and producer properties. */
final case class OutletSettings(
    port: String,
    topic: String,
    clientId: String,
    delivery: Delivery,
    transactionalIdPrefix: Option[String],
    kafka: Map[String, String]
)

/**
 * `streamlet.properties`: what a streamlet is given at run time (contracts/streamlet-settings.md).
 *
 * Written in one layout, so the same settings are always the same bytes: the names, then the
 * parameters, then each inlet, then each outlet, a blank line between groups; parameters, ports and
 * Kafka properties sorted, a port's own keys in the contract's order.
 */
final case class Settings(
    pipeline: String,
    streamlet: String,
    namespace: String,
    local: Boolean,
    parameters: Map[String, String],
    inlets: Vector[InletSettings],
    outlets: Vector[OutletSettings]
):
  def inlet(port: String): Option[InletSettings]   = inlets.find(_.port == port)
  def outlet(port: String): Option[OutletSettings] = outlets.find(_.port == port)

object Settings:

  val FileName = "streamlet.properties"

  /** The environment variable naming the directory that holds the file. */
  val DirectoryVariable = "FLOW_CONFIG_DIR"

  def missing(key: String): String = s"$FileName has no key $key"

  def unknown(key: String): String = s"$FileName has a key flow does not know: $key"

  /** Reads `streamlet.properties` from `dir`. */
  def read(dir: Path): Either[String, Settings] =
    val file = dir.resolve(FileName)
    if !Files.isRegularFile(file) then Left(s"$FileName is not in $dir")
    else
      try parse(new String(Files.readAllBytes(file), StandardCharsets.ISO_8859_1))
      catch case NonFatal(e) => Left(s"$file could not be read: ${e.getMessage}")

  /** Parses the text of a `streamlet.properties` file (ISO 8859-1 with `\u` escapes). */
  def parse(text: String): Either[String, Settings] =
    val props = new Properties()
    try props.load(new StringReader(text))
    catch case NonFatal(e) => return Left(s"$FileName is not a properties file: ${e.getMessage}")
    fromMap(props.stringPropertyNames.asScala.map(k => k -> props.getProperty(k)).toMap)

  private val Names = Vector("pipeline", "streamlet", "namespace", "local")

  def fromMap(all: Map[String, String]): Either[String, Settings] =
    def req(key: String): Either[String, String] = all.get(key).toRight(missing(key))
    def under(prefix: String): Map[String, String] =
      all.collect { case (k, v) if k.startsWith(prefix) => k.stripPrefix(prefix) -> v }
    def ports(kind: String): Vector[String] =
      all.keys
        .collect {
          case k if k.startsWith(s"$kind.") => k.stripPrefix(s"$kind.").takeWhile(_ != '.')
        }
        .toVector
        .distinct
        .sorted

    val stray = all.keys.toVector.sorted.find { k =>
      !Names.contains(k) && !k.startsWith("parameter.") && !portKey("inlet", k, InletKeys) &&
      !portKey("outlet", k, OutletKeys)
    }

    def inlet(p: String): Either[String, InletSettings] =
      for
        topic    <- req(s"inlet.$p.topic")
        group    <- req(s"inlet.$p.group")
        clientId <- req(s"inlet.$p.client-id")
      yield InletSettings(p, topic, group, clientId, under(s"inlet.$p.kafka."))

    def outlet(p: String): Either[String, OutletSettings] =
      for
        topic    <- req(s"outlet.$p.topic")
        clientId <- req(s"outlet.$p.client-id")
        wire     <- req(s"outlet.$p.delivery")
        delivery <- Delivery
          .fromWire(wire)
          .toRight(
            s"$FileName: outlet.$p.delivery is '$wire', not one of " +
              Delivery.values.map(_.wire).mkString(", ")
          )
        prefixKey = s"outlet.$p.transactional-id-prefix"
        prefix <- (delivery, all.get(prefixKey)) match
          case (Delivery.ExactlyOnce, None) => Left(missing(prefixKey))
          case (Delivery.AtLeastOnce, Some(_)) =>
            Left(s"$FileName: $prefixKey is given for an at-least-once outlet")
          case (_, stated) => Right(stated)
      yield OutletSettings(p, topic, clientId, delivery, prefix, under(s"outlet.$p.kafka."))

    def sequence[A](es: Vector[Either[String, A]]): Either[String, Vector[A]] =
      es.collectFirst { case Left(e) => e }.toLeft(es.collect { case Right(a) => a })

    for
      _         <- stray.map(unknown).toLeft(())
      pipeline  <- req("pipeline")
      streamlet <- req("streamlet")
      namespace <- req("namespace")
      localText <- req("local")
      local <- localText match
        case "true"  => Right(true)
        case "false" => Right(false)
        case other   => Left(s"$FileName: local is '$other', not true or false")
      inlets  <- sequence(ports("inlet").map(inlet))
      outlets <- sequence(ports("outlet").map(outlet))
    yield Settings(pipeline, streamlet, namespace, local, under("parameter."), inlets, outlets)

  private val InletKeys  = Set("topic", "group", "client-id")
  private val OutletKeys = Set("topic", "client-id", "delivery", "transactional-id-prefix")

  private def portKey(kind: String, key: String, own: Set[String]): Boolean =
    key.startsWith(s"$kind.") && {
      val rest = key.stripPrefix(s"$kind.").dropWhile(_ != '.').drop(1)
      own.contains(rest) || (rest.startsWith("kafka.") && rest.length > "kafka.".length)
    }

  /**
   * The file's text, in its one layout (see [[Settings]]). ASCII only: other characters are `\u`
   * escapes.
   */
  def write(s: Settings): String =
    def line(k: String, v: String) = s"${escapeKey(k)}=${escapeValue(v)}\n"
    def kafka(prefix: String, m: Map[String, String]) =
      m.toVector.sortBy(_._1).map((k, v) => line(s"$prefix.kafka.$k", v)).mkString

    val names = line("pipeline", s.pipeline) + line("streamlet", s.streamlet) +
      line("namespace", s.namespace) + line("local", s.local.toString)
    val params =
      s.parameters.toVector.sortBy(_._1).map((k, v) => line(s"parameter.$k", v)).mkString
    val inlets = s.inlets.sortBy(_.port).map { i =>
      val p = s"inlet.${i.port}"
      line(s"$p.topic", i.topic) + line(s"$p.group", i.group) +
        line(s"$p.client-id", i.clientId) + kafka(p, i.kafka)
    }
    val outlets = s.outlets.sortBy(_.port).map { o =>
      val p = s"outlet.${o.port}"
      line(s"$p.topic", o.topic) + line(s"$p.client-id", o.clientId) +
        line(s"$p.delivery", o.delivery.wire) +
        o.transactionalIdPrefix.fold("")(line(s"$p.transactional-id-prefix", _)) +
        kafka(p, o.kafka)
    }
    (Vector(names) ++ Option.when(params.nonEmpty)(params) ++ inlets ++ outlets).mkString("\n")

  private def escapeKey(k: String): String =
    escape(k, leading = true, separators = true)

  private def escapeValue(v: String): String =
    escape(v, leading = true, separators = false)

  /**
   * Escapes as `java.util.Properties.load` reads: backslash, control characters and anything
   * outside printable ASCII always; leading spaces; and in a key, the separators and comment
   * characters. A value's `=` and `:` are left as they are, which `load` accepts after the
   * separator.
   */
  private def escape(s: String, leading: Boolean, separators: Boolean): String =
    val b = new StringBuilder
    s.zipWithIndex.foreach { (c, i) =>
      c match
        case '\\'                                     => b ++= "\\\\"
        case '\n'                                     => b ++= "\\n"
        case '\r'                                     => b ++= "\\r"
        case '\t'                                     => b ++= "\\t"
        case '\f'                                     => b ++= "\\f"
        case ' ' if (leading && i == 0) || separators => b ++= "\\ "
        case '=' | ':' | '#' | '!' if separators      => b += '\\' += c
        case '#' | '!' if i == 0                      => b += '\\' += c
        case c if c < 0x20 || c > 0x7e                => b ++= f"\\u${c.toInt}%04X"
        case c                                        => b += c
    }
    b.result()
