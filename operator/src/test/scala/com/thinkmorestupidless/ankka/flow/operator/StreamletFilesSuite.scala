package com.thinkmorestupidless.ankka.flow.operator

import com.thinkmorestupidless.ankka.flow.protocol.DescriptorJson
import com.thinkmorestupidless.ankka.flow.sidecar.{BatchSettings, StreamletConfig}

import Fixtures.*

/** What the operator renders is exactly what the sidecar reads (contracts/sidecar.md). */
class StreamletFilesSuite extends munit.FunSuite:

  private val rendered = Rendering.render(cart(threshold = 250), settings, observed, "t")
  private val topics   = rendered.resolved.map(t => t.id -> t).toMap
  private val router   = cart(threshold = 250).getSpec.streamlets.head
  private val files    = StreamletFiles.render("cart", router, topics).toOption.get

  test(
    "streamlet.conf parses with the sidecar's own parser into every port's topic, group, client id and connection"
  ) {
    val c = StreamletConfig.parseString(files.streamletConf).fold(e => fail(e.mkString), identity)
    assertEquals((c.pipeline, c.streamlet), ("cart", "router"))
    val in = c.inlets("in")
    assertEquals(in.topic, "shop.cart-events.v1")
    assertEquals(in.group, "cart.router.in")
    assertEquals(in.clientId, "cart.router.in")
    assertEquals(in.bootstrapServers, "shop-kafka:9092")
    assertEquals(in.connectionConfig, Map("sasl.mechanism" -> "PLAIN"))
    assertEquals(in.consumerConfig, Map("auto.offset.reset" -> "earliest"))
    assertEquals(in.batch, BatchSettings.Default)
    assertEquals(c.outlets("valid").topic, "cart.valid-carts")
    assertEquals(c.outlets("review").clientId, "cart.router.review")
    assertEquals(c.outlets("review").connectionConfig, Map("security.protocol" -> "PLAINTEXT"))
  }

  test("the sidecar accepts the files: ports match and the config is typed") {
    val c          = StreamletConfig.parseString(files.streamletConf).toOption.get
    val descriptor = DescriptorJson.read(files.descriptorJson).toOption.get.getStreamlet
    assertEquals(c.check(descriptor), Vector.empty)
    assertEquals(c.configJson(descriptor), Right("""{"review-threshold":250}"""))
  }

  test("the hash is stable and covers both files") {
    assertEquals(StreamletFiles.render("cart", router, topics).toOption.get.hash, files.hash)
    assertNotEquals(files.copy(streamletConf = files.streamletConf + " ").hash, files.hash)
  }

  // ── Built-in streamlets (feature 002) ─────────────────────────────────────────────────────────

  private val graphRendered = Rendering.render(graph(), settings, graphObserved, "t")
  private val graphTopics   = graphRendered.resolved.map(t => t.id -> t).toMap
  private val sink          = graph().getSpec.streamlets.find(_.builtin).get
  private val mapperSpec    = graph().getSpec.streamlets.find(!_.builtin).get

  test("a built-in streamlet's streamlet.conf carries the stage block and still parses") {
    val f = StreamletFiles.render("checkouts", sink, graphTopics, Some("41")).toOption.get
    assert(
      f.streamletConf.contains(
        """  stage {
          |    name = "neo4j-merge-sink"
          |    neo4j { credentials-dir = "/etc/flow/neo4j" }
          |  }""".stripMargin
      ),
      f.streamletConf
    )
    val c = StreamletConfig.parseString(f.streamletConf).fold(e => fail(e.mkString), identity)
    assertEquals(c.inlets("in").topic, "checkouts.graph-deltas")
    assert(c.outlets.isEmpty)
    assert(!f.streamletConf.contains("password"))
  }

  test("a streamlet with a process has no stage block") {
    val f = StreamletFiles.render("checkouts", mapperSpec, graphTopics).toOption.get
    assert(!f.streamletConf.contains("stage"), f.streamletConf)
  }

  test("the Secret's version is in the hash; its absence and a new version both change it") {
    def hash(v: Option[String]) =
      StreamletFiles.render("checkouts", sink, graphTopics, v).toOption.get.hash
    assertNotEquals(hash(Some("41")), hash(Some("42")))
    assertNotEquals(hash(None), hash(Some("41")))
    assertEquals(hash(Some("41")), hash(Some("41")))
  }
