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
