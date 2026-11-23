package com.thinkmorestupidless.ankka.flow.descriptor

class JsonSuite extends munit.FunSuite:

  test("canonical sorts keys, indents by two and ends with one newline"):
    assertEquals(
      Json.canonical(
        Json.Obj(Vector("b" -> Json.Str("q\"\\\n\u0001é✓"), "a" -> Json.Arr(Vector.empty)))
      ),
      "{\n  \"a\": [],\n  \"b\": \"q\\\"\\\\\\n\\u0001é✓\"\n}\n"
    )

  test("compact sorts keys and adds no space"):
    assertEquals(
      Json.compact(Json.Obj(Vector("z" -> Json.Num(1), "a" -> Json.Bool(true)))),
      """{"a":true,"z":1}"""
    )

  test("numbers print without trailing zeros"):
    assertEquals(
      Json.compact(Json.Arr(Vector(Json.Num(BigDecimal("1.50")), Json.Num(10)))),
      "[1.5,10]"
    )

  test("parse reads what canonical writes"):
    val doc = Json.Obj(
      Vector(
        "s" -> Json.Str("é\u0007"),
        "n" -> Json.Num(BigDecimal("-2.5e3")),
        "a" -> Json.Arr(Vector(Json.Null, Json.Bool(false), Json.Obj(Vector.empty)))
      )
    )
    assertEquals(Json.parse(Json.canonical(doc)).map(Json.canonical), Right(Json.canonical(doc)))

  test("parse refuses trailing content and malformed input"):
    assert(Json.parse("{} x").isLeft)
    assert(Json.parse("{\"a\":").isLeft)
    assert(Json.parse("nope").isLeft)

  test("field finds a key of an object and nothing in any other value"):
    assertEquals(Json.Obj(Vector("k" -> Json.Null)).field("k"), Some(Json.Null))
    assertEquals(Json.Str("k").field("k"), None)
