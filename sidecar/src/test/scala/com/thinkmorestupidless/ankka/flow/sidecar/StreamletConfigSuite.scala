package com.thinkmorestupidless.ankka.flow.sidecar

class StreamletConfigSuite extends munit.FunSuite:

  private val example = """
    flow {
      pipeline  = cart
      streamlet = router
      config    = { review-threshold = 250 }
      inlets {
        in {
          topic             = "shop.cart-events.v1"
          group             = "cart.router.in"
          client-id         = "cart.router.in"
          bootstrap.servers = "kafka:9092"
          connection-config { security.protocol = PLAINTEXT }
          consumer-config   { auto.offset.reset = earliest, max.poll.records = 50 }
          batch { max-records = 10, max-bytes = 64 KiB }
        }
      }
      outlets {
        valid  { topic = "cart.valid-carts",  client-id = "cart.router.valid",  bootstrap.servers = "kafka:9092", connection-config {}, producer-config { linger.ms = 5 } }
        review { topic = "cart.review-carts", bootstrap.servers = "kafka:9092" }
      }
    }
  """

  private val router = TestSpecs.fixture("cart-router").getStreamlet

  test("the example in contracts/sidecar.md parses") {
    val c = StreamletConfig.parseString(example).fold(e => fail(e.mkString), identity)
    assertEquals(c.pipeline, "cart")
    val in = c.inlets("in")
    assertEquals(in.topic, "shop.cart-events.v1")
    assertEquals(in.group, "cart.router.in")
    assertEquals(in.bootstrapServers, "kafka:9092")
    assertEquals(in.connectionConfig, Map("security.protocol" -> "PLAINTEXT"))
    assertEquals(
      in.consumerConfig,
      Map("auto.offset.reset" -> "earliest", "max.poll.records" -> "50")
    )
    assertEquals(in.batch, BatchSettings(10, 64 * 1024))
    assertEquals(c.outlets("valid").producerConfig, Map("linger.ms" -> "5"))
    assertEquals(
      c.outlets("review").clientId,
      "cart.router.review",
      "client id defaults to <pipeline>.<streamlet>.<port>"
    )
    assertEquals(c.check(router), Vector.empty)
  }

  test("config_json carries every parameter typed, the configured value over the default") {
    val c = StreamletConfig.parseString(example).toOption.get
    assertEquals(c.configJson(router), Right("""{"review-threshold":250}"""))
    val defaulted = StreamletConfig
      .parseString(example.replace("config    = { review-threshold = 250 }", ""))
      .toOption
      .get
    assertEquals(defaulted.configJson(router), Right("""{"review-threshold":100}"""))
  }

  test("a value that is not the parameter's type, and a missing required parameter, are problems") {
    val bad = StreamletConfig.parseString(example.replace("250", "lots")).toOption.get
    assert(
      bad.check(router).exists(_.contains("'lots' is not a integer")),
      bad.check(router).toString
    )
    val every = TestSpecs.fixture("every-type").getStreamlet
    val c     = StreamletConfig.parseString(example).toOption.get
    assert(
      c.configJson(every)
        .left
        .toOption
        .get
        .exists(_.contains("parameter 'required' has no default and no value"))
    )
  }

  test("ports must be exactly the descriptor's") {
    val c        = StreamletConfig.parseString(example.replace("review {", "audit {")).toOption.get
    val problems = c.check(router)
    assert(
      problems.exists(
        _.contains("outlet 'review' is declared but streamlet.conf does not configure it")
      ),
      problems.toString
    )
    assert(problems.exists(_.contains("configures outlet 'audit'")), problems.toString)
  }

  test("a missing field is a problem, not an exception") {
    assert(StreamletConfig.parseString("flow { pipeline = x }").isLeft)
  }

  private val sink =
    com.thinkmorestupidless.ankka.flow.protocol.Builtins.neo4jMergeSink.getStreamlet

  private def stageConf(stage: String) = s"""
    flow {
      pipeline  = checkouts
      streamlet = graph
      config    = { secret = neo4j-shop }
      $stage
      inlets { in { topic = "checkouts.graph-deltas", bootstrap.servers = "kafka:9092" } }
    }
  """

  test("a stage block selects a built-in stage and names its credentials directory") {
    val c = StreamletConfig
      .parseString(
        stageConf(
          """stage { name = neo4j-merge-sink, neo4j { credentials-dir = "/etc/flow/neo4j" } }"""
        )
      )
      .toOption
      .get
    assertEquals(
      c.stage,
      Some(StageConfig("neo4j-merge-sink", Some(Neo4jStageConfig("/etc/flow/neo4j"))))
    )
    assertEquals(c.outlets, Map.empty)
    assertEquals(c.check(sink), Vector.empty)
  }

  test("no stage block means a process") {
    assertEquals(StreamletConfig.parseString(example).toOption.get.stage, None)
  }

  test("a stage this sidecar does not have, or a merge sink without credentials, is refused") {
    val unknown = StreamletConfig.parseString(stageConf("stage { name = nope }")).toOption.get
    assertEquals(
      unknown.check(sink),
      Vector("stage 'nope' is not built into this sidecar; it has: neo4j-merge-sink")
    )
    val noCredentials =
      StreamletConfig.parseString(stageConf("stage { name = neo4j-merge-sink }")).toOption.get
    assertEquals(
      noCredentials.check(sink),
      Vector("stage 'neo4j-merge-sink' needs flow.stage.neo4j.credentials-dir")
    )
  }
