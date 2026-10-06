package com.thinkmorestupidless.ankka.flow.sdk

import java.nio.charset.StandardCharsets.UTF_8

import com.thinkmorestupidless.ankka.flow.protocol.{Fingerprint, Json}

/** A port of a streamlet: a name and a JSON contract named by its schema. */
sealed trait Port:
  def name: String
  def schemaName: String
  final def format: String      = Fingerprint.Format
  final def fingerprint: String = Fingerprint.fingerprint(schemaName)

/** A port a streamlet reads from. */
final class JsonInlet private[sdk] (val name: String, val schemaName: String) extends Port:
  override def toString: String = s"JsonInlet($name, $schemaName)"

/** A port a streamlet writes to. */
sealed trait Outlet extends Port

/** An outlet carrying JSON values named by its schema. */
final class JsonOutlet private[sdk] (val name: String, val schemaName: String) extends Outlet:

  /**
   * The record to this outlet, unchanged: same key, headers and bytes. Replace parts of it with
   * `copy`: `emit(record.copy(value = ...))`.
   */
  def emit(record: Record): Emit = Emit(name, record)

  /** A new record to this outlet. */
  def emit(
      value: Array[Byte],
      key: Option[Array[Byte]] = None,
      headers: Seq[(String, Array[Byte])] = Nil
  ): Emit = Emit(name, Record(value, key, headers))

  override def toString: String = s"JsonOutlet($name, $schemaName)"

/**
 * An outlet of graph deltas. It builds each record with its element key; none can be passed in.
 * `source` is the input the delta was derived from: its headers and offset are carried, so the
 * delta is traceable to it and its record is not counted as skipped.
 */
final class GraphDeltaOutlet private[sdk] (val name: String) extends Outlet:
  import graph.*

  val schemaName: String = SchemaName

  /** The node's whole state at `version`: exactly these labels and these properties. */
  def node(
      id: String,
      version: Long,
      labels: Seq[String] = Nil,
      properties: Map[String, Any] = Map.empty,
      source: Option[Record] = None
  ): Emit =
    build(
      source,
      nodeKey(id),
      "kind"       -> Json.Str("node"),
      "id"         -> Json.Str(checkId("id", id)),
      "version"    -> Json.Num(BigDecimal(checkVersion(version))),
      "labels"     -> Json.Arr(labels.toVector.map(l => Json.Str(checkIdentifier("a label", l)))),
      "properties" -> Json.Obj(checkProperties(properties))
    )

  /** The edge's whole state at `version`, from node `fromId` to node `toId`. */
  def edge(
      id: String,
      version: Long,
      `type`: String,
      fromId: String,
      toId: String,
      properties: Map[String, Any] = Map.empty,
      source: Option[Record] = None
  ): Emit =
    build(
      source,
      edgeKey(id),
      "kind"       -> Json.Str("edge"),
      "id"         -> Json.Str(checkId("id", id)),
      "version"    -> Json.Num(BigDecimal(checkVersion(version))),
      "type"       -> Json.Str(checkIdentifier("type", `type`)),
      "from"       -> Json.Str(checkId("fromId", fromId)),
      "to"         -> Json.Str(checkId("toId", toId)),
      "properties" -> Json.Obj(checkProperties(properties))
    )

  /** Marks the node deleted at `version`. It stays in the graph, marked. */
  def tombstoneNode(id: String, version: Long, source: Option[Record] = None): Emit =
    build(
      source,
      nodeKey(id),
      "kind"    -> Json.Str("tombstone"),
      "element" -> Json.Str("node"),
      "id"      -> Json.Str(checkId("id", id)),
      "version" -> Json.Num(BigDecimal(checkVersion(version)))
    )

  /** Marks the edge deleted at `version`; its type and endpoints say where to find it. */
  def tombstoneEdge(
      id: String,
      version: Long,
      `type`: String,
      fromId: String,
      toId: String,
      source: Option[Record] = None
  ): Emit =
    build(
      source,
      edgeKey(id),
      "kind"    -> Json.Str("tombstone"),
      "element" -> Json.Str("edge"),
      "id"      -> Json.Str(checkId("id", id)),
      "version" -> Json.Num(BigDecimal(checkVersion(version))),
      "type"    -> Json.Str(checkIdentifier("type", `type`)),
      "from"    -> Json.Str(checkId("fromId", fromId)),
      "to"      -> Json.Str(checkId("toId", toId))
    )

  private def build(source: Option[Record], key: Array[Byte], fields: (String, Json)*): Emit =
    val value = Json.compact(Json.Obj(fields.toVector)).getBytes(UTF_8)
    val record = source match
      case Some(r) => r.copy(value = value, key = Some(key))
      case None    => Record(value, Some(key))
    Emit(name, record)

  override def toString: String = s"GraphDeltaOutlet($name)"
