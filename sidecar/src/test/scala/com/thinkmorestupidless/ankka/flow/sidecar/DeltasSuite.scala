package com.thinkmorestupidless.ankka.flow.sidecar

import Deltas.{Delta, Value}

/** The graph delta contract, parsed and folded, with no database (contracts/graph-delta.md). */
class DeltasSuite extends munit.FunSuite:

  private def parse(json: String) = Deltas.parse(7L, json)

  test("the contract's examples parse") {
    assertEquals(
      parse(
        """{"kind": "node", "id": "cart:cart-1", "version": 1790627790360, "labels": ["Cart"], "properties": {"cartId": "cart-1"}}"""
      ),
      Right(
        Delta.NodeMerge(
          "cart:cart-1",
          1790627790360L,
          Vector("Cart"),
          Map("cartId" -> Value.Text("cart-1"))
        )
      )
    )
    assertEquals(
      parse(
        """{"kind": "edge", "id": "e1", "version": 3, "type": "CHECKED_OUT", "from": "a", "to": "b", "properties": {}}"""
      ),
      Right(Delta.EdgeMerge("e1", 3L, "CHECKED_OUT", "a", "b", Map.empty))
    )
    assertEquals(
      parse("""{"kind": "tombstone", "element": "node", "id": "cart:cart-1", "version": 9}"""),
      Right(Delta.NodeTombstone("cart:cart-1", 9L))
    )
    assertEquals(
      parse(
        """{"kind": "tombstone", "element": "edge", "id": "e1", "version": 9, "type": "CHECKED_OUT", "from": "a", "to": "b"}"""
      ),
      Right(Delta.EdgeTombstone("e1", 9L, "CHECKED_OUT", "a", "b"))
    )
  }

  test("labels and properties may be absent, and unknown fields are ignored") {
    assertEquals(
      parse("""{"kind": "node", "id": "n", "version": 0, "note": "from a newer writer"}"""),
      Right(Delta.NodeMerge("n", 0L, Vector.empty, Map.empty))
    )
  }

  test("every validation rule names the offset and the problem") {
    val cases = Seq(
      """not json"""                                 -> "offset 7: not a JSON object",
      """[1, 2]"""                                   -> "offset 7: not a JSON object",
      """{"id": "n", "version": 1}"""                -> "offset 7: kind missing",
      """{"kind": "nod", "id": "n", "version": 1}""" -> "offset 7: unknown kind 'nod'",
      """{"kind": "node", "id": "", "version": 1}""" -> "offset 7: id missing or empty",
      """{"kind": "node", "id": "n", "version": -1}""" -> "offset 7: version is not a non-negative integer",
      """{"kind": "node", "id": "n", "version": 1.5}""" -> "offset 7: version is not a non-negative integer",
      """{"kind": "node", "id": "n", "version": "1"}""" -> "offset 7: version is not a non-negative integer",
      """{"kind": "node", "id": "n", "version": 99999999999999999999}""" -> "offset 7: version is not a non-negative integer",
      """{"kind": "node", "id": "n", "version": 1, "labels": ["a b"]}""" -> "offset 7: labels must be an array of identifiers",
      """{"kind": "node", "id": "n", "version": 1, "labels": "Cart"}""" -> "offset 7: labels must be an array of identifiers",
      """{"kind": "edge", "id": "e", "version": 1, "from": "a", "to": "b"}""" -> "offset 7: edge needs type, from and to",
      """{"kind": "tombstone", "id": "n", "version": 1}""" -> "offset 7: tombstone needs element 'node' or 'edge'",
      """{"kind": "tombstone", "element": "edge", "id": "e", "version": 1}""" -> "offset 7: tombstone of an edge needs type, from and to",
      """{"kind": "node", "id": "n", "version": 1, "properties": {"a": null}}""" -> "offset 7: property 'a' is not a scalar or array of scalars",
      """{"kind": "node", "id": "n", "version": 1, "properties": {"a": {"b": 1}}}""" -> "offset 7: property 'a' is not a scalar or array of scalars",
      """{"kind": "node", "id": "n", "version": 1, "properties": {"a": [1, "x"]}}""" -> "offset 7: property 'a' is not a scalar or array of scalars",
      """{"kind": "node", "id": "n", "version": 1, "properties": {"a": []}}""" -> "offset 7: property 'a' is not a scalar or array of scalars",
      """{"kind": "node", "id": "n", "version": 1, "properties": {"_version": 3}}""" -> "offset 7: property '_version' is reserved",
      """{"kind": "node", "id": "n", "version": 1, "properties": [1]}""" -> "offset 7: properties must be an object"
    )
    cases.foreach((json, expected) => assertEquals(parse(json), Left(expected), json))
  }

  test("numbers keep their kind, and arrays of one scalar kind are kept") {
    val Right(Delta.NodeMerge(_, _, _, props)) =
      parse(
        """{"kind": "node", "id": "n", "version": 1, "properties": {"i": 42, "d": 1.5, "b": true, "s": "x", "xs": [1, 2], "e": 1e3}}"""
      ): @unchecked
    assertEquals(props("i"), Value.Integer(42L))
    assertEquals(props("d"), Value.Decimal(1.5))
    assertEquals(props("b"), Value.Flag(true))
    assertEquals(props("s"), Value.Text("x"))
    assertEquals(props("xs"), Value.Many(Vector(Value.Integer(1L), Value.Integer(2L))))
    assertEquals(props("e"), Value.Integer(1000L))
  }

  test(
    "a batch folds to the highest version per element, the first on a tie, and counts the rest stale"
  ) {
    val a1      = Delta.NodeMerge("a", 1, Vector.empty, Map("v" -> Value.Integer(1)))
    val a3      = Delta.NodeMerge("a", 3, Vector.empty, Map("v" -> Value.Integer(3)))
    val a2      = Delta.NodeTombstone("a", 2)
    val b5      = Delta.NodeMerge("b", 5, Vector.empty, Map("first" -> Value.Flag(true)))
    val b5again = Delta.NodeMerge("b", 5, Vector.empty, Map("first" -> Value.Flag(false)))
    val edgeA   = Delta.EdgeMerge("a", 1, "T", "a", "b", Map.empty) // an edge id equal to a node id
    val folded  = Deltas.fold(Vector(a1, a3, b5, a2, b5again, edgeA))
    assertEquals(folded.nodes, Vector(a3, b5))
    assertEquals(folded.nodeTombstones, Vector.empty)
    assertEquals(folded.edges, Vector(edgeA))
    assertEquals(folded.stale, 3)
    assertEquals(folded.size, 3)
  }

  test("a tombstone with the highest version wins the fold") {
    val folded = Deltas.fold(
      Vector(Delta.NodeMerge("a", 1, Vector.empty, Map.empty), Delta.NodeTombstone("a", 2))
    )
    assertEquals(folded.nodes, Vector.empty)
    assertEquals(folded.nodeTombstones, Vector(Delta.NodeTombstone("a", 2)))
  }

  test("every delta in the shared fixture has the key the fixture gives it") {
    import com.thinkmorestupidless.ankka.flow.protocol.Json
    val file = TestSpecs.repoRoot.resolve("protocol/fixtures/graph-deltas/keys.json")
    val Right(Json.Arr(rows)) =
      Json.parse(new String(java.nio.file.Files.readAllBytes(file), "UTF-8")): @unchecked
    assert(rows.size >= 8, s"only ${rows.size} rows")
    rows.zipWithIndex.foreach { (row, i) =>
      val Some(Json.Str(expected)) = row.field("key"): @unchecked
      val delta                    = Deltas.parse(i.toLong, Json.compact(row.field("delta").get))
      assertEquals(delta.map(Deltas.key), Right(expected), s"row $i")
      assertEquals(delta.map(Deltas.keyBytes(_).toStringUtf8), Right(expected), s"row $i")
    }
  }

  private def kindOf(value: Deltas.Value): String = value match
    case _: Deltas.Value.Text    => "string"
    case _: Deltas.Value.Integer => "integer"
    case _: Deltas.Value.Decimal => "float"
    case _: Deltas.Value.Flag    => "boolean"
    case Deltas.Value.Many(vs)   => s"list:${kindOf(vs.head)}"

  test("every delta in the shared fixture of deltas is read, keyed and typed as the fixture says") {
    import com.thinkmorestupidless.ankka.flow.protocol.Json
    val file = TestSpecs.repoRoot.resolve("protocol/fixtures/graph-deltas/deltas.json")
    val Right(Json.Arr(rows)) =
      Json.parse(new String(java.nio.file.Files.readAllBytes(file), "UTF-8")): @unchecked
    assert(rows.size >= 12, s"only ${rows.size} rows")
    val kinds = rows.zipWithIndex.flatMap { (row, i) =>
      val Some(Json.Str(expected)) = row.field("key"): @unchecked
      val Some(Json.Obj(reads))    = row.field("reads"): @unchecked
      val value                    = Json.compact(row.field("delta").get)
      val Right(delta)             = Deltas.parse(i.toLong, value): @unchecked
      assertEquals(Deltas.key(delta), expected, s"row $i")
      // The key the fixture gives is the key the sink demands of the record.
      assert(Deltas.read(i.toLong, Some(bytes(expected)), bytes(value)).isRight, s"row $i")
      val properties = delta match
        case n: Deltas.Delta.NodeMerge => n.properties
        case e: Deltas.Delta.EdgeMerge => e.properties
        case _                         => Map.empty[String, Deltas.Value]
      val found  = properties.view.mapValues(kindOf).toMap
      val wanted = reads.collect { case (name, Json.Str(kind)) => name -> kind }.toMap
      assertEquals(found, wanted, s"row $i")
      found.values
    }.toSet
    // Every kind of property value the contract has is in the file at least once.
    assertEquals(
      kinds,
      Set("string", "integer", "float", "boolean").flatMap(k => Set(k, s"list:$k"))
    )
  }

  private def bytes(s: String) = com.google.protobuf.ByteString.copyFromUtf8(s)
  private val cart             = """{"kind":"node","id":"cart:cart-1","version":1}"""
  private val tombstone = """{"kind":"tombstone","element":"node","id":"cart:cart-1","version":2}"""

  test("a delta under its own element key is read; a tombstone's key is its element's") {
    assert(
      Deltas
        .read(0, Some(bytes("node:cart:cart-1")), bytes(cart))
        .exists(_.isInstanceOf[Deltas.Read.Applied])
    )
    assertEquals(
      Deltas.read(0, Some(bytes("node:cart:cart-1")), bytes(tombstone)),
      Right(Deltas.Read.Applied(Delta.NodeTombstone("cart:cart-1", 2)))
    )
  }

  test("any other key, the wrong kind, and no key are refused, naming the key expected") {
    assertEquals(
      Deltas.read(7, Some(bytes("cart:cart-1")), bytes(cart)),
      Left("offset 7: key 'cart:cart-1' is not this delta's element key 'node:cart:cart-1'")
    )
    assertEquals(
      Deltas.read(7, Some(bytes("edge:cart:cart-1")), bytes(cart)),
      Left("offset 7: key 'edge:cart:cart-1' is not this delta's element key 'node:cart:cart-1'")
    )
    assertEquals(
      Deltas.read(7, None, bytes(cart)),
      Left("offset 7: no key; this delta's element key is 'node:cart:cart-1'")
    )
    val notUtf8 = com.google.protobuf.ByteString.copyFrom(Array[Byte](-1, -2))
    assert(
      Deltas
        .read(7, Some(notUtf8), bytes(cart))
        .left
        .exists(_.contains("is not this delta's element key"))
    )
  }

  test("a record with no value is a delete marker whatever its key; one byte is not") {
    val empty = com.google.protobuf.ByteString.EMPTY
    assertEquals(Deltas.read(0, Some(bytes("node:cart:cart-1")), empty), Right(Deltas.Read.Marker))
    assertEquals(Deltas.read(0, None, empty), Right(Deltas.Read.Marker))
    assertEquals(Deltas.read(0, Some(bytes("anything at all")), empty), Right(Deltas.Read.Marker))
    assertEquals(Deltas.read(3, Some(bytes("k")), bytes("x")), Left("offset 3: not a JSON object"))
  }
