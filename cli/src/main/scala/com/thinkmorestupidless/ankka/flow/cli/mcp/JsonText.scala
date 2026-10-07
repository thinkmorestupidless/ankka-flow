package com.thinkmorestupidless.ankka.flow.cli.mcp

import com.thinkmorestupidless.ankka.flow.protocol.Json

/**
 * What the server needs of JSON beyond the protocol module's AST and parser: field access, and a
 * printer that keeps an object's fields in the order they were written — a client's settings file
 * is a person's, and is written back as they had it.
 */
private[cli] object JsonText:

  def obj(fields: (String, Json)*): Json = Json.Obj(fields.toVector)
  def arr(items: Json*): Json            = Json.Arr(items.toVector)
  def str(s: String): Json               = Json.Str(s)
  def num(n: Long): Json                 = Json.Num(BigDecimal(n))
  def bool(b: Boolean): Json             = Json.Bool(b)

  extension (json: Json)
    def apply(field: String): Option[Json]    = json.field(field)
    def string(field: String): Option[String] = json.field(field).collect { case Json.Str(s) => s }
    def int(field: String): Option[Int] =
      json.field(field).collect { case Json.Num(n) if n.isValidInt => n.toInt }
    def boolean(field: String): Option[Boolean] =
      json.field(field).collect { case Json.Bool(b) => b }
    def strings(field: String): Vector[String] =
      json
        .field(field)
        .collect { case Json.Arr(xs) => xs.collect { case Json.Str(s) => s } }
        .getOrElse(Vector.empty)
    def objectFields: Vector[(String, Json)] = json match
      case Json.Obj(fs) => fs
      case _            => Vector.empty

    /** One line, fields in written order: the protocol's wire form. */
    def render: String = { val sb = new StringBuilder; compact(json, sb); sb.toString }

    /** Two-space indentation, fields in written order: a settings file. */
    def pretty: String = { val sb = new StringBuilder; indented(json, sb, 0); sb.toString }

  private def compact(json: Json, sb: StringBuilder): Unit = json match
    case Json.Arr(items) =>
      sb.append('[')
      items.zipWithIndex.foreach { (item, i) =>
        if i > 0 then sb.append(','); compact(item, sb)
      }
      sb.append(']'): Unit
    case Json.Obj(fs) =>
      sb.append('{')
      fs.zipWithIndex.foreach { case ((k, v), i) =>
        if i > 0 then sb.append(',')
        quote(k, sb); sb.append(':'); compact(v, sb)
      }
      sb.append('}'): Unit
    case other => scalar(other, sb)

  private def indented(json: Json, sb: StringBuilder, depth: Int): Unit =
    def pad(d: Int): Unit = sb.append("  " * d): Unit
    json match
      case Json.Arr(items) if items.isEmpty => sb.append("[]"): Unit
      case Json.Obj(fs) if fs.isEmpty       => sb.append("{}"): Unit
      case Json.Arr(items) =>
        sb.append("[\n")
        items.zipWithIndex.foreach { (item, i) =>
          pad(depth + 1); indented(item, sb, depth + 1)
          if i < items.size - 1 then sb.append(',')
          sb.append('\n')
        }
        pad(depth); sb.append(']'): Unit
      case Json.Obj(fs) =>
        sb.append("{\n")
        fs.zipWithIndex.foreach { case ((k, v), i) =>
          pad(depth + 1); quote(k, sb); sb.append(": "); indented(v, sb, depth + 1)
          if i < fs.size - 1 then sb.append(',')
          sb.append('\n')
        }
        pad(depth); sb.append('}'): Unit
      case other => scalar(other, sb)

  private def scalar(json: Json, sb: StringBuilder): Unit = json match
    case Json.Str(s)  => quote(s, sb)
    case Json.Num(n)  => sb.append(n.bigDecimal.stripTrailingZeros.toPlainString): Unit
    case Json.Bool(b) => sb.append(b): Unit
    case _            => sb.append("null"): Unit

  private def quote(s: String, sb: StringBuilder): Unit =
    sb.append('"')
    s.foreach {
      case '"'          => sb.append("\\\"")
      case '\\'         => sb.append("\\\\")
      case '\n'         => sb.append("\\n")
      case '\r'         => sb.append("\\r")
      case '\t'         => sb.append("\\t")
      case c if c < ' ' => sb.append(f"\\u${c.toInt}%04x")
      case c            => sb.append(c)
    }
    sb.append('"'): Unit
