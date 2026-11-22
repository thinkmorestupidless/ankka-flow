package com.thinkmorestupidless.ankka.flow.blueprint

import com.thinkmorestupidless.ankka.flow.descriptor.{Engine, Json, ParameterType}

import Builders.*

class OverridesSuite extends munit.FunSuite:

  private val router = descriptor(
    name = "cart-router",
    inlets = Seq("in" -> json("cart.v1")),
    outlets = Seq("valid" -> json("cart.v1")),
    parameters = Seq(param("review-threshold", ParameterType.Integer, "100"), param("label"))
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

  private def routerStreamlet = verified.streamlets.head

  private val counterDescriptor = descriptor(
    name = "cart-counter",
    inlets = Seq("in" -> json("cart.v1")),
    engine = Engine.Flink
  )

  private val both = Blueprint
    .parseString(
      """blueprint {
        |  streamlets { router = cart-router, counter = cart-counter }
        |  topics {
        |    in    { managed = false, bootstrap.servers = "k:9092", consumers = [router.in] }
        |    valid { producers = [router.valid], consumers = [counter.in] }
        |  }
        |}""".stripMargin,
      Vector(router, counterDescriptor)
    )
    .verified
    .fold(p => throw new IllegalStateException(p.toString), identity)

  private def counter = both.streamlets.find(_.name == "counter").get
  private def cleaner = both.streamlets.find(_.name == "router").get

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
    assertEquals(o.replicas(routerStreamlet), Right(3))
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
    assertEquals(Overrides.empty.replicas(routerStreamlet), Right(1))
    assert(overrides("flow.streamlets.router.replicas = -1").replicas(routerStreamlet).isLeft)
  }

  test("a Flink streamlet's parallelism and flink settings are read") {
    val o = overrides(
      """flow.streamlets.counter {
        |  parallelism = 2
        |  flink {
        |    checkpoint-interval = 30s
        |    job-manager { cpu = 1, memory = 1024m }
        |    task-manager { cpu = 0.5, memory = 2048m }
        |  }
        |}""".stripMargin
    )
    assertEquals(o.parallelism(counter), Right(Some(2)))
    assertEquals(
      o.flink(counter),
      Right(
        Some(
          FlinkOverrides(
            Some("30s"),
            ResourceOverrides(Some("1"), Some("1024m")),
            ResourceOverrides(Some("0.5"), Some("2048m"))
          )
        )
      )
    )
    assertEquals(o.settingProblems(both), Vector.empty)
  }

  test("a Flink streamlet given none of them has none") {
    assertEquals(Overrides.empty.parallelism(counter), Right(None))
    assertEquals(Overrides.empty.flink(counter), Right(None))
    val partial = overrides("flow.streamlets.counter.flink.task-manager.memory = 4g")
    assertEquals(
      partial.flink(counter),
      Right(
        Some(
          FlinkOverrides(None, ResourceOverrides(None, None), ResourceOverrides(None, Some("4g")))
        )
      )
    )
  }

  test("parallelism and flink on a Pekko Streams streamlet are refused, naming key and engine") {
    val o = overrides("flow.streamlets.router { parallelism = 2, flink.checkpoint-interval = 10s }")
    val expected = Vector(
      EngineSettingNotAllowed("router", "flink", Engine.PekkoStreams),
      EngineSettingNotAllowed("router", "parallelism", Engine.PekkoStreams)
    ).map(BlueprintProblem.toMessage)
    assertEquals(o.settingProblems(both), expected)
    assertEquals(o.parallelism(cleaner), Left(expected(1)))
    assertEquals(o.flink(cleaner), Left(expected(0)))
    assert(
      expected(1).contains("'parallelism'") && expected(1).contains("pekko-streams"),
      expected(1)
    )
  }

  test("a Flink streamlet's replicas may only be 0 or 1") {
    assertEquals(Overrides.empty.replicas(counter), Right(1))
    assertEquals(overrides("flow.streamlets.counter.replicas = 0").replicas(counter), Right(0))
    val Left(e) = overrides("flow.streamlets.counter.replicas = 2").replicas(counter): @unchecked
    assert(e.contains("counter") && e.contains("0 or 1"), e)
    assertEquals(overrides("flow.streamlets.router.replicas = 2").replicas(cleaner), Right(2))
  }

  test("parallelism and flink settings must be of their types") {
    assert(overrides("flow.streamlets.counter.parallelism = 0").parallelism(counter).isLeft)
    assert(overrides("flow.streamlets.counter.parallelism = x").parallelism(counter).isLeft)
    for bad <- Vector(
        "flink.checkpoint-interval = soon",
        "flink.job-manager.cpu = lots",
        "flink.task-manager.cpu = 0",
        "flink.task-manager.memory = big",
        "flink.task-manager.disk = 1g",
        "flink.savepoints = on"
      )
    do
      val Left(e) = overrides(s"flow.streamlets.counter.$bad").flink(counter): @unchecked
      assert(e.contains("counter"), e)
  }

  test("flow.ankka.cluster is read, and is ankka unless given") {
    assertEquals(Overrides.empty.ankkaCluster, "ankka")
    assertEquals(overrides("flow.ankka.cluster = pipelines").ankkaCluster, "pipelines")
  }

  test("consumer-config.flow.batch in an override is refused by name") {
    val o         = overrides("flow.topics.valid.consumer-config.flow.batch.max-records = 10")
    val Vector(e) = o.settingProblems(verified): @unchecked
    assert(e.contains("consumer-config.flow.batch") && e.contains("'valid'"), e)
  }
