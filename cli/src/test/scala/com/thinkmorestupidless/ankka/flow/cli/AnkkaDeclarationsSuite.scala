package com.thinkmorestupidless.ankka.flow.cli

import java.nio.file.{Files, Paths}

import com.thinkmorestupidless.ankka.flow.descriptor.Contract

/** What `ankka projects topics list -p <project> -o json` prints, read by `--ankka-topics`. */
class AnkkaDeclarationsSuite extends munit.FunSuite:

  private val sample = Paths.get(getClass.getResource("/ankka/checkout-topics.json").toURI)

  test("the file ankka prints parses, every field flow does not read ignored") {
    val d = AnkkaDeclarations.load(s"checkout=$sample").fold(fail(_), identity)
    assertEquals(d.project, "checkout")
    assertEquals(
      d.topics.keySet,
      Set(
        "cart-events",
        "clean-carts",
        "cart-counts",
        "notices",
        "cart-checkouts",
        "clean-checkouts"
      )
    )
    assertEquals(
      d.topics("cart-events"),
      Some(
        Contract(
          "cart-events.v1",
          Some("sha256:f19f4516ec394cb282c806a2b02b5ff3e9d4e99a6152cecffe9639c8453d36b8")
        )
      )
    )
    assertEquals(d.topics("notices"), None)
  }

  test(
    "an option that is not project=file, a missing file, or a file that is not the list, each refused"
  ) {
    assert(AnkkaDeclarations.load("checkout").left.exists(_.contains("<project>=<file>")))
    assert(AnkkaDeclarations.load("checkout=/nowhere.json").left.exists(_.contains("cannot read")))
    val notAList = Files.createTempFile("topics", ".json")
    Files.writeString(notAList, """{"topics": []}""")
    assert(
      AnkkaDeclarations.load(s"checkout=$notAList").left.exists(_.contains("JSON array")),
      AnkkaDeclarations.load(s"checkout=$notAList").toString
    )
  }

  test("a contract flow cannot read is refused, naming the topic; a null contract is none") {
    def load(json: String) =
      val f = Files.createTempFile("topics", ".json")
      Files.writeString(f, json)
      AnkkaDeclarations.load(s"checkout=$f")
    for (bad, why) <- Seq(
        """{"fingerprint": "sha256:00"}"""       -> "a contract must have a name",
        """{"name": 7}"""                        -> "a contract must have a name",
        "\"cart-events.v1\""                     -> "a contract must be an object",
        """{"name": "c.v1", "fingerprint": 7}""" -> "a contract's fingerprint must be a string"
      )
    do
      val got = load(s"""[{"name": "cart-events", "contract": $bad}]""")
      assert(got.left.exists(_.endsWith(s": topic 'cart-events': $why")), s"$bad: $got")
    assertEquals(
      load("""[{"name": "notices", "contract": null}]""").map(_.topics),
      Right(Map("notices" -> None))
    )
  }
