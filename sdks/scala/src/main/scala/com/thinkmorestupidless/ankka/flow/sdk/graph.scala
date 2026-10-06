package com.thinkmorestupidless.ankka.flow.sdk

import java.nio.charset.StandardCharsets.UTF_8

import com.thinkmorestupidless.ankka.flow.protocol.Json

/**
 * Graph deltas (`ankka.graph-delta.v1`): the records the built-in merge sink reads.
 *
 * A delta states one graph element's whole state — a node, an edge, or a tombstone marking one
 * deleted — with the element's id and a version that rises with its source's history. Its record
 * key is its element key, `node:<id>` or `edge:<id>`, because a compacted topic keeps the last
 * record per key and the sink refuses a delta under any other key. `GraphDeltaOutlet` builds the
 * record, key included, so a mapper never chooses a key. `read` parses an emitted record back, for
 * a mapper's tests.
 */
object graph:
  val SchemaName = "ankka.graph-delta.v1"

  def nodeKey(id: String): Array[Byte] = s"node:$id".getBytes(UTF_8)
  def edgeKey(id: String): Array[Byte] = s"edge:$id".getBytes(UTF_8)

  /**
   * One delta, as `read` returns it. `kind` is `node`, `edge` or `tombstone`; `element` is the kind
   * of element it describes or marks. `type`, `fromId` and `toId` are set for an edge and an edge's
   * tombstone. Properties are `String`, `Boolean`, `Long`, `Double`, or a non-empty `Vector` of one
   * of those; a whole number is a `Long`, as the sink stores it.
   */
  final case class Delta(
      kind: String,
      element: String,
      id: String,
      version: Long,
      labels: Vector[String] = Vector.empty,
      `type`: Option[String] = None,
      fromId: Option[String] = None,
      toId: Option[String] = None,
      properties: Map[String, Any] = Map.empty,
      key: String = ""
  )

  private val Identifier = "[A-Za-z_][A-Za-z0-9_]*".r
  private val Reserved   = Set("id", "_version", "_deleted")

  private def refuse(message: String): Nothing = throw new IllegalArgumentException(message)

  private[sdk] def checkId(name: String, value: String): String =
    if value == null || value.isEmpty then refuse(s"$name must be a non-empty string, not '$value'")
    value

  private[sdk] def checkVersion(value: Long): Long =
    if value < 0 then
      refuse(s"version must be a non-negative integer that fits 64 bits, not $value")
    value

  private[sdk] def checkIdentifier(name: String, value: String): String =
    if value == null || !Identifier.matches(value) then
      refuse(s"$name must be an identifier ([A-Za-z_][A-Za-z0-9_]*), not '$value'")
    value

  /** How the sink reads a scalar: as JSON, or `None` when it refuses it. */
  private def scalar(value: Any): Option[(String, Json)] = value match
    case b: Boolean => Some("flag" -> Json.Bool(b))
    case s: String  => Some("text" -> Json.Str(s))
    case i: Int     => Some("integer" -> Json.Num(BigDecimal(i)))
    case l: Long    => Some("integer" -> Json.Num(BigDecimal(l)))
    case f: Float   => scalar(f.toDouble)
    case d: Double =>
      if d.isNaN || d.isInfinite then None
      else if d.isWhole && d >= Long.MinValue.toDouble && d <= Long.MaxValue.toDouble then
        Some("integer"    -> Json.Num(BigDecimal(d)))
      else Some("decimal" -> Json.Num(BigDecimal(d)))
    case n: BigInt if n.isValidLong => Some("integer" -> Json.Num(BigDecimal(n)))
    case _                          => None

  private[sdk] def checkProperties(properties: Map[String, Any]): Vector[(String, Json)] =
    properties.toVector.sortBy(_._1).map { (name, v) =>
      if Reserved(name) then refuse(s"property '$name' is reserved")
      v match
        case xs: Iterable[?] =>
          val read  = xs.toVector.map(scalar)
          val kinds = read.map(_.map(_._1)).toSet
          if xs.isEmpty || kinds.contains(None) || kinds.size != 1 then
            refuse(
              s"property '$name' must be a scalar or a non-empty list of one kind of scalar, not $v"
            )
          name -> Json.Arr(read.flatten.map(_._2))
        case other =>
          scalar(other) match
            case Some((_, json)) => name -> json
            case None =>
              refuse(
                s"property '$name' must be a string, number, boolean or a non-empty list of one of those, not $v"
              )
    }

  // ── reading, for tests ──────────────────────────────────────────────────────────────────────────

  /**
   * The delta a record carries. Throws `IllegalArgumentException` when the record is not a delta,
   * or when its key is not its element's key — what the sink would refuse.
   */
  def read(record: Record): Delta =
    val body = Json.parse(record.valueString) match
      case Right(o: Json.Obj) => o
      case _                  => refuse("not a JSON object")
    def text(field: String): Option[String] = body.field(field).collect { case Json.Str(s) => s }
    val kind                                = text("kind").getOrElse(refuse("kind missing"))
    val id                                  = checkId("id", text("id").orNull)
    val version = body.field("version") match
      case Some(Json.Num(n)) if n.isWhole && n.isValidLong => checkVersion(n.toLong)
      case other => refuse(s"version must be a non-negative integer that fits 64 bits, not $other")
    def endpoints = (
      checkIdentifier("type", text("type").orNull),
      checkId("from", text("from").orNull),
      checkId("to", text("to").orNull)
    )
    def labels = body.field("labels") match
      case None => Vector.empty
      case Some(Json.Arr(xs)) =>
        xs.map {
          case Json.Str(s) => checkIdentifier("a label", s);
          case x           => refuse(s"a label must be an identifier, not $x")
        }
      case Some(other) => refuse(s"labels must be a list of identifiers, not $other")
    def properties: Map[String, Any] = body.field("properties") match
      case None | Some(Json.Null) => Map.empty
      case Some(Json.Obj(fields)) =>
        fields.map((k, v) => k -> fromJson(v)).toMap.tap(checkProperties)
      case Some(other) => refuse(s"properties must be a mapping, not $other")
    def keyOf(element: String) = s"$element:$id"
    val delta = kind match
      case "node" =>
        Delta("node", "node", id, version, labels, properties = properties, key = keyOf("node"))
      case "edge" =>
        val (t, from, to) = endpoints
        Delta(
          "edge",
          "edge",
          id,
          version,
          `type` = Some(t),
          fromId = Some(from),
          toId = Some(to),
          properties = properties,
          key = keyOf("edge")
        )
      case "tombstone" =>
        text("element") match
          case Some("node") => Delta("tombstone", "node", id, version, key = keyOf("node"))
          case Some("edge") =>
            val (t, from, to) = endpoints
            Delta(
              "tombstone",
              "edge",
              id,
              version,
              `type` = Some(t),
              fromId = Some(from),
              toId = Some(to),
              key = keyOf("edge")
            )
          case _ => refuse("tombstone needs element 'node' or 'edge'")
      case other => refuse(s"unknown kind '$other'")
    record.keyString match
      case None => refuse(s"no key; this delta's element key is '${delta.key}'")
      case Some(k) if k != delta.key =>
        refuse(s"key '$k' is not this delta's element key '${delta.key}'")
      case Some(_) => delta

  private def fromJson(v: Json): Any = v match
    case Json.Str(s)                               => s
    case Json.Bool(b)                              => b
    case Json.Num(n) if n.isWhole && n.isValidLong => n.toLong
    case Json.Num(n)                               => n.toDouble
    case Json.Arr(xs)                              => xs.map(fromJson)
    case other                                     => other

  extension [A](a: A) private def tap(f: A => Any): A = { f(a); a }
