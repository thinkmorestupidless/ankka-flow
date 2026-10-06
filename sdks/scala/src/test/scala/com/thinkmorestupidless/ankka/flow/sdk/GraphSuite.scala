package com.thinkmorestupidless.ankka.flow.sdk

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}

import com.thinkmorestupidless.ankka.flow.protocol.Json
import com.thinkmorestupidless.ankka.flow.sdk.testkit.Harness

/**
 * Graph deltas built by the SDK carry their element keys, as protocol/fixtures/graph-deltas says.
 */
class GraphSuite extends munit.FunSuite:

  private val dir =
    Paths.get(sys.props.getOrElse("flow.repo.root", ".")).resolve("protocol/fixtures/graph-deltas")
  private def rows(file: String): Vector[Json] =
    Json.parse(new String(Files.readAllBytes(dir.resolve(file)), UTF_8)) match
      case Right(Json.Arr(xs)) => xs
      case other               => fail(s"$file: $other")

  private object Holder extends Streamlet("holder"):
    val deltas                                = graphDeltaOutlet("deltas")
    def process(batch: Batch): Iterable[Emit] = Nil
  private val out = Holder.deltas

  private def str(j: Json, k: String): String = j.field(k).collect { case Json.Str(s) => s }.get
  private def num(j: Json, k: String): Long =
    j.field(k).collect { case Json.Num(n) => n.toLong }.get
  private def props(j: Json): Map[String, Any] = j.field("properties") match
    case Some(Json.Obj(fs)) => fs.map((k, v) => k -> scalaOf(v)).toMap
    case _                  => Map.empty
  private def scalaOf(v: Json): Any = v match
    case Json.Str(s)                  => s
    case Json.Bool(b)                 => b
    case Json.Num(n) if n.isValidLong => n.toLong
    case Json.Num(n)                  => n.toDouble
    case Json.Arr(xs)                 => xs.map(scalaOf)
    case other                        => other

  private def build(d: Json): Emit = str(d, "kind") match
    case "node" =>
      val labels = d
        .field("labels")
        .collect { case Json.Arr(xs) => xs.collect { case Json.Str(s) => s } }
        .getOrElse(Vector.empty)
      out.node(str(d, "id"), num(d, "version"), labels, props(d))
    case "edge" =>
      out.edge(
        str(d, "id"),
        num(d, "version"),
        str(d, "type"),
        str(d, "from"),
        str(d, "to"),
        props(d)
      )
    case _ if str(d, "element") == "node" => out.tombstoneNode(str(d, "id"), num(d, "version"))
    case _ =>
      out.tombstoneEdge(
        str(d, "id"),
        num(d, "version"),
        str(d, "type"),
        str(d, "from"),
        str(d, "to")
      )

  test("a delta built by the outlet has the fixture's key and value") {
    val keys = rows("keys.json")
    assert(keys.size >= 8)
    keys.foreach { row =>
      val delta = row.field("delta").get
      val emit  = build(delta)
      assertEquals(emit.outlet, "deltas")
      assertEquals(emit.record.keyString, Some(str(row, "key")))
      assertEquals(
        Json.compact(Json.parse(emit.record.valueString).toOption.get),
        Json.compact(delta)
      )
      assertEquals(graph.read(emit.record).key, str(row, "key"))
    }
  }

  test("a delta of the fixture built by the outlet reads back as the fixture's") {
    rows("deltas.json").foreach { row =>
      val delta   = row.field("delta").get
      val key     = str(row, "key")
      val written = graph.read(build(delta).record)
      val fixture =
        graph.read(Record(Json.compact(delta).getBytes(UTF_8), Some(key.getBytes(UTF_8))))
      assertEquals(written, fixture)
      val reads = row.field("reads").collect { case Json.Obj(fs) => fs.map(_._1).toSet }.get
      assertEquals(written.properties.keySet, reads)
    }
  }

  test("a node and an edge with one id have different keys, and a tombstone has its element's") {
    assertNotEquals(
      out.node("same-id", 1).record.keyString,
      out.edge("same-id", 1, "LINKS", "a", "b").record.keyString
    )
    assertEquals(out.tombstoneNode("cart:1", 2).record.keyString, Some("node:cart:1"))
    assertEquals(out.tombstoneEdge("e:1", 2, "LINKS", "a", "b").record.keyString, Some("edge:e:1"))
  }

  private def refused(names: String)(f: => Any): Unit =
    val e = intercept[IllegalArgumentException](f)
    assert(e.getMessage.contains(names), s"'${e.getMessage}' does not name '$names'")

  test("a delta the sink would refuse is refused") {
    refused("id")(out.node("", 1))
    refused("version")(out.node("n", -1))
    refused("label")(out.node("n", 1, labels = Seq("has space")))
    refused("label")(out.node("n", 1, labels = Seq("1st")))
    refused("'id' is reserved")(out.node("n", 1, properties = Map("id" -> "x")))
    refused("'_version' is reserved")(out.node("n", 1, properties = Map("_version" -> 3)))
    refused("'_deleted' is reserved")(out.node("n", 1, properties = Map("_deleted" -> true)))
    refused("property 'a'")(out.node("n", 1, properties = Map("a" -> null)))
    refused("property 'a'")(out.node("n", 1, properties = Map("a" -> Map("b" -> 1))))
    refused("property 'a'")(out.node("n", 1, properties = Map("a" -> Vector.empty)))
    refused("property 'a'")(out.node("n", 1, properties = Map("a" -> Vector(1, "x"))))
    refused("property 'a'")(out.node("n", 1, properties = Map("a" -> Vector(1, 1.5))))
    refused("property 'a'")(out.node("n", 1, properties = Map("a" -> Vector(true, 1))))
    refused("property 'a'")(out.node("n", 1, properties = Map("a" -> Vector(Vector(1)))))
    refused("property 'a'")(out.node("n", 1, properties = Map("a" -> Double.NaN)))
    refused("property 'a'")(out.node("n", 1, properties = Map("a" -> BigInt(2).pow(63))))
    refused("type")(out.edge("e", 1, "checked out", "a", "b"))
    refused("fromId")(out.edge("e", 1, "LINKS", "", "b"))
    refused("toId")(out.edge("e", 1, "LINKS", "a", ""))
    refused("version")(out.tombstoneNode("n", -1))
    refused("type")(out.tombstoneEdge("e", 1, "not ok", "a", "b"))
  }

  test("read refuses a wrongly keyed or keyless delta, and what is not a delta") {
    val emit = out.node("n", 1)
    refused("no key")(graph.read(emit.record.copy(key = None)))
    refused("is not this delta's element key")(
      graph.read(emit.record.copy(key = Some("edge:n".getBytes(UTF_8))))
    )
    refused("not a JSON object")(
      graph.read(Record("[]".getBytes(UTF_8), Some("node:n".getBytes(UTF_8))))
    )
    refused("unknown kind")(
      graph.read(
        Record(
          """{"kind":"x","id":"n","version":1}""".getBytes(UTF_8),
          Some("node:n".getBytes(UTF_8))
        )
      )
    )
  }

  test("read takes a whole number written with an exponent, as the sink does") {
    val r = Record(
      """{"kind":"node","id":"n","version":1e3,"labels":[],"properties":{}}""".getBytes(UTF_8),
      Some("node:n".getBytes(UTF_8))
    )
    assertEquals(graph.read(r).version, 1000L)
  }

  test("the outlet's descriptor is the one a JSON outlet of the contract writes") {
    assertEquals(out.schemaName, "ankka.graph-delta.v1")
    assertEquals(out.fingerprint, Streamlet.portSpec(out).getContract.fingerprint)
  }

  test(
    "a delta built from an input record carries its headers and is not skipped; from nothing, none"
  ) {
    val mapper = new Streamlet("mapper"):
      inlet("in", schemaName = "x.v1")
      val deltas = graphDeltaOutlet("deltas")
      def process(batch: Batch): Iterable[Emit] =
        batch.records.map(r => deltas.node("n", 1, source = Some(r)))
    val h = Harness(mapper)
    h.inlet("in").put("v".getBytes(UTF_8), headers = Seq("ce-id" -> "x".getBytes(UTF_8)))
    h.run()
    assertEquals(h.skipped, Vector.empty)
    assertEquals(h.outlet("deltas").records.head.headers.map(_._1), Seq("ce-id"))
    assertEquals(out.node("n", 1).record.headers, Nil)
  }
