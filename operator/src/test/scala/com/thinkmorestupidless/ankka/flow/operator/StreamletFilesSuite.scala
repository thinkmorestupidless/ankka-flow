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
