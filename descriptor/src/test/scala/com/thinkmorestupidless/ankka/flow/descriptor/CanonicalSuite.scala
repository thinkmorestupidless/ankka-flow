package com.thinkmorestupidless.ankka.flow.descriptor

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}

/** Fingerprints as ankka computes them, held to ankka's own fixtures (research R4). */
class CanonicalSuite extends munit.FunSuite:

  private val root    = Path.of(sys.props.getOrElse("flow.repo.root", "."))
  private val fixture = root.resolve("descriptor/fixtures/contracts/fingerprints.json")

  private val cases: Vector[(String, Json, String)] =
    Json.parse(Files.readString(fixture, UTF_8)) match
      case Right(Json.Arr(items)) =>
        items.map { item =>
          (item.field("name"), item.field("schema"), item.field("fingerprint")) match
            case (Some(Json.Str(n)), Some(schema), Some(Json.Str(fp))) => (n, schema, fp)
            case other => throw new IllegalStateException(s"malformed fixture: $other")
        }
      case other => throw new IllegalStateException(s"malformed fixtures file: $other")

  test("the fixtures are ankka's six"):
    assertEquals(cases.map(_._1).toSet.size, 6)

  cases.foreach { (name, schema, expected) =>
    test(s"$name fingerprints as ankka states"):
      // The document as a person would save it: whitespace and key order are not the canonical form.
      val saved = Json.canonical(schema).getBytes(UTF_8)
      assertEquals(Contract.fingerprint(saved), Right(expected))
      assertEquals(Contract.fingerprint(Json.compact(schema).getBytes(UTF_8)), Right(expected))
  }

  test("whitespace and key order do not change a fingerprint; a changed field does"):
    val a = """{ "type": "object",  "required": ["id"] }""".getBytes(UTF_8)
    val b = """{"required":["id"],"type":"object"}""".getBytes(UTF_8)
    val c = """{"required":["id"],"type":"array"}""".getBytes(UTF_8)
    assertEquals(Contract.fingerprint(a), Contract.fingerprint(b))
    assertNotEquals(Contract.fingerprint(a), Contract.fingerprint(c))

  test("a document that is not JSON is a Left"):
    assert(Contract.fingerprint("{not json".getBytes(UTF_8)).isLeft)
    assert(Contract.fingerprint(Array.emptyByteArray).isLeft)

  test("a fingerprint is sha256: and 64 lowercase hex"):
    val fp = Contract.fingerprint("{}".getBytes(UTF_8)).toOption.get
    assert(Contract.validFingerprint(fp), fp)
    assert(!Contract.validFingerprint("sha256:ABC"))

  test("the name follows ankka's rule"):
    assert(Contract.validName("cart-events.v1"))
    assert(Contract.validName("a1"))
    assert(!Contract.validName("a"))
    assert(!Contract.validName("Cart"))
    assert(!Contract.validName("-cart"))
    assert(!Contract.validName("c" * 101))

  test("two ports connect when names are equal and stated fingerprints agree"):
    val fp1 = Some("sha256:" + "1" * 64)
    val fp2 = Some("sha256:" + "2" * 64)
    assert(Contract.connects(Contract("a.v1", fp1), Contract("a.v1", fp1)))
    assert(Contract.connects(Contract("a.v1", fp1), Contract("a.v1", None)))
    assert(Contract.connects(Contract("a.v1", None), Contract("a.v1", fp2)))
    assert(Contract.connects(Contract("a.v1", None), Contract("a.v1", None)))
    assert(!Contract.connects(Contract("a.v1", fp1), Contract("a.v1", fp2)))
    assert(!Contract.connects(Contract("a.v1", fp1), Contract("b.v1", fp1)))

  test("numbers render as ECMAScript prints them"):
    val cases = Vector(
      "0"                     -> "0",
      "-0"                    -> "0",
      "1.0"                   -> "1",
      "-1.5"                  -> "-1.5",
      "1e21"                  -> "1e+21",
      "1e30"                  -> "1e+30",
      "0.002"                 -> "0.002",
      "1e-7"                  -> "1e-7",
      "123456789012345678901" -> "123456789012345680000",
      "333333333.33333333"    -> "333333333.3333333"
    )
    cases.foreach((in, out) => assertEquals(Canonical.number(BigDecimal(in)), out, in))
