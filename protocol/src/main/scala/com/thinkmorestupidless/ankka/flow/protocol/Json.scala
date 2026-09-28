package com.thinkmorestupidless.ankka.flow.protocol

/**
 * The smallest JSON the protocol needs: an AST, a parser, and the canonical printer that
 * DESCRIPTOR.md defines. Canonical means what Python's
 * `json.dumps(obj, sort_keys=True, indent=2, ensure_ascii=False)` prints, byte for byte, so a
 * descriptor written by any SDK compares equal as bytes (research R5).
 */
enum Json:
  case Str(value: String)
  case Num(value: BigDecimal)
  case Bool(value: Boolean)
  case Null
  case Arr(items: Vector[Json])
  case Obj(fields: Vector[(String, Json)])

  def field(name: String): Option[Json] = this match
    case Obj(fs) => fs.collectFirst { case (`name`, v) => v }
    case _       => None

object Json:

  /** Canonical form: keys sorted at every level, two-space indent, one trailing newline. */
  def canonical(json: Json): String =
    val sb = new StringBuilder
    write(json, sb, 0)
    sb.append('\n').toString

  /** Compact form, keys sorted. For `Start.config_json` and embedding. */
  def compact(json: Json): String =
    val sb = new StringBuilder
    writeCompact(json, sb)
    sb.toString

  private def write(json: Json, sb: StringBuilder, depth: Int): Unit =
    def indent(d: Int): Unit =
      var i = 0
      while i < d * 2 do
        sb.append(' ')
        i += 1
    json match
      case Arr(items) if items.isEmpty => sb.append("[]"): Unit
      case Obj(fs) if fs.isEmpty       => sb.append("{}"): Unit
      case Arr(items) =>
        sb.append("[\n")
        items.zipWithIndex.foreach { (item, i) =>
          indent(depth + 1)
          write(item, sb, depth + 1)
          if i < items.size - 1 then sb.append(',')
          sb.append('\n')
        }
        indent(depth)
        sb.append(']'): Unit
      case Obj(fs) =>
        sb.append("{\n")
        val sorted = fs.sortBy(_._1)
        sorted.zipWithIndex.foreach { case ((k, v), i) =>
          indent(depth + 1)
          string(k, sb)
          sb.append(": ")
          write(v, sb, depth + 1)
          if i < sorted.size - 1 then sb.append(',')
          sb.append('\n')
        }
        indent(depth)
        sb.append('}'): Unit
      case other => scalar(other, sb)

  private def writeCompact(json: Json, sb: StringBuilder): Unit = json match
    case Arr(items) =>
      sb.append('[')
      items.zipWithIndex.foreach { (item, i) =>
        if i > 0 then sb.append(',')
        writeCompact(item, sb)
      }
      sb.append(']'): Unit
    case Obj(fs) =>
      sb.append('{')
      fs.sortBy(_._1).zipWithIndex.foreach { case ((k, v), i) =>
        if i > 0 then sb.append(',')
        string(k, sb)
        sb.append(':')
        writeCompact(v, sb)
      }
      sb.append('}'): Unit
    case other => scalar(other, sb)

  private def scalar(json: Json, sb: StringBuilder): Unit = json match
    case Str(s)  => string(s, sb)
    case Num(n)  => sb.append(n.bigDecimal.stripTrailingZeros.toPlainString): Unit
    case Bool(b) => sb.append(b): Unit
    case Null    => sb.append("null"): Unit
    case _       => ()

  /** Python's escaping with ensure_ascii=False: only quote, backslash and control characters. */
  private def string(s: String, sb: StringBuilder): Unit =
    sb.append('"')
    s.foreach {
      case '"'          => sb.append("\\\"")
      case '\\'         => sb.append("\\\\")
      case '\n'         => sb.append("\\n")
      case '\r'         => sb.append("\\r")
      case '\t'         => sb.append("\\t")
      case '\b'         => sb.append("\\b")
      case '\f'         => sb.append("\\f")
      case c if c < ' ' => sb.append(f"\\u${c.toInt}%04x")
      case c            => sb.append(c)
    }
    sb.append('"'): Unit

  // ── Parsing ─────────────────────────────────────────────────────────────

  def parse(text: String): Either[String, Json] =
    val p = new Parser(text)
    try
      val v = p.value()
      p.ws()
      if p.pos != text.length then Left(s"unexpected content at offset ${p.pos}") else Right(v)
    catch case e: IllegalArgumentException => Left(e.getMessage)

  private final class Parser(s: String):
    var pos = 0

    def fail(msg: String): Nothing = throw new IllegalArgumentException(s"$msg at offset $pos")

    def ws(): Unit =
      while pos < s.length && " \t\r\n".indexOf(s.charAt(pos)) >= 0 do pos += 1

    def expect(c: Char): Unit =
      ws()
      if pos >= s.length || s.charAt(pos) != c then fail(s"expected '$c'")
      pos += 1

    def value(): Json =
      ws()
      if pos >= s.length then fail("unexpected end of input")
      s.charAt(pos) match
        case '{' => obj()
        case '[' => arr()
        case '"' => Str(str())
        case 't' => literal("true", Bool(true))
        case 'f' => literal("false", Bool(false))
        case 'n' => literal("null", Null)
        case _   => num()

    def literal(word: String, v: Json): Json =
      if !s.startsWith(word, pos) then fail(s"expected $word")
      pos += word.length
      v

    def num(): Json =
      val start = pos
      while pos < s.length && "+-0123456789.eE".indexOf(s.charAt(pos)) >= 0 do pos += 1
      if start == pos then fail("expected a value")
      try Num(BigDecimal(s.substring(start, pos)))
      catch case _: NumberFormatException => fail("malformed number")

    def str(): String =
      expect('"')
      val sb = new StringBuilder
      while
        if pos >= s.length then fail("unterminated string")
        val c = s.charAt(pos)
        pos += 1
        c match
          case '"' => false
          case '\\' =>
            if pos >= s.length then fail("unterminated escape")
            val e = s.charAt(pos)
            pos += 1
            e match
              case 'n' => sb.append('\n')
              case 'r' => sb.append('\r')
              case 't' => sb.append('\t')
              case 'b' => sb.append('\b')
              case 'f' => sb.append('\f')
              case 'u' =>
                if pos + 4 > s.length then fail("short unicode escape")
                sb.append(Integer.parseInt(s.substring(pos, pos + 4), 16).toChar)
                pos += 4
              case other => sb.append(other)
            true
          case other =>
            sb.append(other)
            true
      do ()
      sb.toString

    def arr(): Json =
      expect('[')
      ws()
      if pos < s.length && s.charAt(pos) == ']' then
        pos += 1
        Arr(Vector.empty)
      else
        val b = Vector.newBuilder[Json]
        b += value()
        ws()
        while pos < s.length && s.charAt(pos) == ',' do
          pos += 1
          b += value()
          ws()
        expect(']')
        Arr(b.result())

    def obj(): Json =
      expect('{')
      ws()
      if pos < s.length && s.charAt(pos) == '}' then
        pos += 1
        Obj(Vector.empty)
      else
        val b = Vector.newBuilder[(String, Json)]
        def entry(): Unit =
          ws()
          val k = str()
          expect(':')
          b += k -> value()
        entry()
        ws()
        while pos < s.length && s.charAt(pos) == ',' do
          pos += 1
          entry()
          ws()
        expect('}')
        Obj(b.result())
