package com.thinkmorestupidless.ankka.flow.sidecar

import java.nio.charset.StandardCharsets

import scala.jdk.CollectionConverters.*

import com.google.protobuf.ByteString
import com.thinkmorestupidless.ankka.flow.protocol.Json

/**
 * The graph delta contract, `ankka.graph-delta.v1` (contracts/graph-delta.md): parsing one record
 * into a `Delta`, folding a batch to one delta per element, and the statement parameters.
 *
 * This is the one place the sidecar decodes a record's value, and it decodes only this contract: a
 * built-in stage owns its contract as a process owns its own.
 */
object Deltas:

  /** A property value as the graph stores it. */
  enum Value:
    case Text(value: String)
    case Integer(value: Long)
    case Decimal(value: Double)
    case Flag(value: Boolean)
    case Many(values: Vector[Value])

    def toJava: AnyRef = this match
      case Text(v)    => v
      case Integer(v) => java.lang.Long.valueOf(v)
      case Decimal(v) => java.lang.Double.valueOf(v)
      case Flag(v)    => java.lang.Boolean.valueOf(v)
      case Many(vs)   => vs.map(_.toJava).asJava

  enum Delta:
    case NodeMerge(
        id: String,
        version: Long,
        labels: Vector[String],
        properties: Map[String, Value]
    )
    case EdgeMerge(
        id: String,
        version: Long,
        tpe: String,
        from: String,
        to: String,
        properties: Map[String, Value]
    )
    case NodeTombstone(id: String, version: Long)
    case EdgeTombstone(id: String, version: Long, tpe: String, from: String, to: String)

    def id: String
    def version: Long

    /** Nodes and edges are separate id spaces. */
    def space: String = this match
      case _: NodeMerge | _: NodeTombstone => "node"
      case _: EdgeMerge | _: EdgeTombstone => "edge"

  /** A batch folded to one delta per element, in the order the statements run. */
  final case class Folded(
      nodes: Vector[Delta.NodeMerge],
      edges: Vector[Delta.EdgeMerge],
      nodeTombstones: Vector[Delta.NodeTombstone],
      edgeTombstones: Vector[Delta.EdgeTombstone],
      stale: Int
  ):
    def size: Int = nodes.size + edges.size + nodeTombstones.size + edgeTombstones.size

  private val Identifier = "[A-Za-z_][A-Za-z0-9_]*".r
  private val Reserved   = Set("id", "_version", "_deleted")

  def parse(offset: Long, value: ByteString): Either[String, Delta] =
    parse(offset, value.toString(StandardCharsets.UTF_8))

  def parse(offset: Long, text: String): Either[String, Delta] =
    def fail(msg: String) = Left(s"offset $offset: $msg")
    Json.parse(text) match
      case Left(_)                  => fail("not a JSON object")
      case Right(obj @ Json.Obj(_)) => parseObject(obj).left.map(m => s"offset $offset: $m")
      case Right(_)                 => fail("not a JSON object")

  private def parseObject(obj: Json): Either[String, Delta] =
    for
      kind    <- string(obj, "kind").toRight("kind missing")
      id      <- string(obj, "id").filter(_.nonEmpty).toRight("id missing or empty")
      version <- version(obj)
      delta <- kind match
        case "node" =>
          for
            labels     <- labels(obj)
            properties <- properties(obj)
          yield Delta.NodeMerge(id, version, labels, properties)
        case "edge" =>
          for
            (tpe, from, to) <- endpoints(obj).toRight("edge needs type, from and to")
            properties      <- properties(obj)
          yield Delta.EdgeMerge(id, version, tpe, from, to, properties)
        case "tombstone" =>
          string(obj, "element") match
            case Some("node") => Right(Delta.NodeTombstone(id, version))
            case Some("edge") =>
              endpoints(obj)
                .map((t, f, to) => Delta.EdgeTombstone(id, version, t, f, to))
                .toRight("tombstone of an edge needs type, from and to")
            case _ => Left("tombstone needs element 'node' or 'edge'")
        case other => Left(s"unknown kind '$other'")
    yield delta

  private def string(obj: Json, name: String): Option[String] =
    obj.field(name).collect { case Json.Str(s) => s }

  private def version(obj: Json): Either[String, Long] =
    obj.field("version") match
      case Some(Json.Num(n)) if n >= 0 && n.isWhole && n.isValidLong => Right(n.toLongExact)
      case _ => Left("version is not a non-negative integer")

  private def labels(obj: Json): Either[String, Vector[String]] =
    obj.field("labels") match
      case None => Right(Vector.empty)
      case Some(Json.Arr(items)) =>
        val names = items.collect { case Json.Str(s) if Identifier.matches(s) => s }
        if names.size == items.size then Right(names)
        else Left("labels must be an array of identifiers")
      case Some(_) => Left("labels must be an array of identifiers")

  private def endpoints(obj: Json): Option[(String, String, String)] =
    for
      tpe  <- string(obj, "type").filter(Identifier.matches)
      from <- string(obj, "from").filter(_.nonEmpty)
      to   <- string(obj, "to").filter(_.nonEmpty)
    yield (tpe, from, to)

  private def properties(obj: Json): Either[String, Map[String, Value]] =
    obj.field("properties") match
      case None => Right(Map.empty)
      case Some(Json.Obj(fields)) =>
        fields.foldLeft[Either[String, Map[String, Value]]](Right(Map.empty)) {
          case (Right(_), (key, _)) if Reserved(key) => Left(s"property '$key' is reserved")
          case (Right(acc), (key, json)) =>
            value(json)
              .map(v => acc + (key -> v))
              .toRight(s"property '$key' is not a scalar or array of scalars")
          case (left, _) => left
        }
      case Some(_) => Left("properties must be an object")

  private def scalar(json: Json): Option[Value] = json match
    case Json.Str(s)                               => Some(Value.Text(s))
    case Json.Bool(b)                              => Some(Value.Flag(b))
    case Json.Num(n) if n.isWhole && n.isValidLong => Some(Value.Integer(n.toLongExact))
    case Json.Num(n) if n.isWhole                  => None // an integer beyond 64 bits
    case Json.Num(n)                               => Some(Value.Decimal(n.toDouble))
    case _                                         => None

  private def value(json: Json): Option[Value] = json match
    case Json.Arr(items) if items.nonEmpty =>
      val values = items.flatMap(scalar)
      val kinds  = values.map(_.ordinal).distinct
      Option.when(values.size == items.size && kinds.size == 1)(Value.Many(values))
    case Json.Arr(_) => None
    case other       => scalar(other)

  /**
   * One delta per element: the highest version, the first in the batch on a tie. Everything folded
   * away is counted as stale, because applying it could change nothing the survivor does not.
   */
  def fold(deltas: Vector[Delta]): Folded =
    val survivors = deltas.zipWithIndex
      .groupBy((d, _) => d.space -> d.id)
      .values
      .map(_.maxBy((d, i) => (d.version, -i)))
      .toVector
      .sortBy(_._2)
      .map(_._1)
    Folded(
      survivors.collect { case d: Delta.NodeMerge => d },
      survivors.collect { case d: Delta.EdgeMerge => d },
      survivors.collect { case d: Delta.NodeTombstone => d },
      survivors.collect { case d: Delta.EdgeTombstone => d },
      deltas.size - survivors.size
    )

  private def props(ps: Map[String, Value]): java.util.Map[String, AnyRef] =
    ps.map((k, v) => k -> v.toJava).asJava

  private def rows(
      maps: Vector[Map[String, AnyRef]]
  ): java.util.List[java.util.Map[String, AnyRef]] =
    maps.map(_.asJava).asJava

  def nodeRows(ds: Vector[Delta.NodeMerge]) =
    rows(
      ds.map(d =>
        Map[String, AnyRef](
          "id"         -> d.id,
          "version"    -> java.lang.Long.valueOf(d.version),
          "labels"     -> d.labels.asJava,
          "properties" -> props(d.properties)
        )
      )
    )

  def edgeRows(ds: Vector[Delta.EdgeMerge]) =
    rows(
      ds.map(d =>
        Map[String, AnyRef](
          "id"         -> d.id,
          "version"    -> java.lang.Long.valueOf(d.version),
          "type"       -> d.tpe,
          "from"       -> d.from,
          "to"         -> d.to,
          "properties" -> props(d.properties)
        )
      )
    )

  def nodeTombstoneRows(ds: Vector[Delta.NodeTombstone]) =
    rows(
      ds.map(d => Map[String, AnyRef]("id" -> d.id, "version" -> java.lang.Long.valueOf(d.version)))
    )

  def edgeTombstoneRows(ds: Vector[Delta.EdgeTombstone]) =
    rows(
      ds.map(d =>
        Map[String, AnyRef](
          "id"      -> d.id,
          "version" -> java.lang.Long.valueOf(d.version),
          "type"    -> d.tpe,
          "from"    -> d.from,
          "to"      -> d.to
        )
      )
    )
