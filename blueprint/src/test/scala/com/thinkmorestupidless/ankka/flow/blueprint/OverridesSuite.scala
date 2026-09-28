package com.thinkmorestupidless.ankka.flow.blueprint

import ankka.flow.v1.discovery.ConfigType
import com.thinkmorestupidless.ankka.flow.protocol.Json

import Builders.*

class OverridesSuite extends munit.FunSuite:

  private val router = descriptor(
    name = "cart-router",
    inlets = Seq("in" -> json("cart.v1")),
    outlets = Seq("valid" -> json("cart.v1")),
    parameters = Seq(param("review-threshold", ConfigType.INTEGER, "100"), param("label"))
  )

  private val verified = Blueprint
    .parseString(
      """blueprint {
        |  streamlets { router = cart-router }
        |  topics {
        |    in    { managed = false, bootstrap.servers = "k:9092", consumers = [router.in] }
        |    valid { producers = [router.valid], partitions = 3, topic { retention.ms = 1000 } }
        |  }
        |}""".stripMargin,
      Vector(router)
    )
    .verified
    .fold(p => throw new IllegalStateException(p.toString), identity)

  private def overrides(text: String) = Overrides.parse(text).toOption.get

  test("a topic's partitions and settings are overridden over the blueprint's") {
    val o     = overrides("flow.topics.valid { partitions = 12, topic { retention.ms = 5000 } }")
    val valid = verified.topics.find(_.id == "valid").get
    val s     = TopicSettings.fromConfig(o.topicConfig(valid))
    assertEquals(s.partitions, Some(12))
    assertEquals(s.topicConfig, Map("retention.ms" -> "5000"))
  }

  test(
    "a streamlet's parameter is overridden and type-checked; defaults fill the rest; replicas apply"
  ) {
    val o = overrides(
      "flow.streamlets.router { replicas = 3, config { review-threshold = 250, label = x } }"
    )
    assertEquals(o.replicas("router"), Right(3))
    val params = o.parameters(verified.streamlets.head).toOption.get.toMap
    assertEquals(params("review-threshold"), Json.Num(BigDecimal(250)))
    val bad = overrides("flow.streamlets.router.config { review-threshold = lots, label = x }")
    assert(
      bad
        .parameters(verified.streamlets.head)
        .left
        .toOption
        .get
        .exists(_.contains("= lots is not a integer"))
    )
  }

  test("a required parameter with no value is a problem") {
    val problems = Overrides.empty.parameters(verified.streamlets.head).left.toOption.get
    assertEquals(
      problems,
      Vector("streamlet 'router': parameter 'label' has no default and no value")
    )
  }

  test("unknown topics, streamlets and parameters are problems") {
    val o = overrides(
      "flow.topics.nope { partitions = 1 }\nflow.streamlets.ghost.replicas = 1\nflow.streamlets.router.config { label = x, extra = 1 }"
    )
    assertEquals(
      o.unknownNames(verified),
      Vector("overrides name unknown topic 'nope'", "overrides name unknown streamlet 'ghost'")
    )
    assert(
      o.parameters(verified.streamlets.head)
        .left
        .toOption
        .get
        .exists(_.contains("parameter 'extra' is not declared"))
    )
  }

  test("replicas default to 1 and must not be negative") {
    assertEquals(Overrides.empty.replicas("router"), Right(1))
    assert(overrides("flow.streamlets.router.replicas = -1").replicas("router").isLeft)
  }
