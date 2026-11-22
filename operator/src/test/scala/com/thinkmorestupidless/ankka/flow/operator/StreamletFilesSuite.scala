package com.thinkmorestupidless.ankka.flow.operator

import com.thinkmorestupidless.ankka.flow.descriptor.{
  Delivery,
  InletSettings,
  OutletSettings,
  Settings as FlowSettings
}

import Fixtures.*

/** The settings file the operator renders for a streamlet, and the hash that rolls it. */
class StreamletFilesSuite extends munit.FunSuite:

  private val resource           = cart(maxTotal = 250)
  private val rendered           = Rendering.render(resource, settings, observed, "t")
  private val topics             = rendered.resolved.map(t => t.id -> t).toMap
  private def spec(name: String) = resource.getSpec.streamlets.find(_.name == name).get
  private def files(name: String) =
    StreamletFiles.render("cart", "shop", spec(name), topics).fold(e => fail(e), identity)
  private def parsed(name: String) =
    FlowSettings.parse(files(name).properties).fold(e => fail(e), identity)

  test("the cleaner's settings are its own ports' resolved topics, and no other's (R6)") {
    assertEquals(
      parsed("cleaner"),
      FlowSettings(
        pipeline = "cart",
        streamlet = "cleaner",
        namespace = "shop",
        local = false,
        parameters = Map("max-total" -> "250"),
        inlets = Vector(
          InletSettings(
            "in",
            "shop.cart-events.v1",
            "cart.cleaner.in",
            "cart.cleaner.in",
            Map(
              "auto.offset.reset" -> "earliest",
              "bootstrap.servers" -> "shop-kafka:9092",
              "sasl.mechanism"    -> "PLAIN"
            )
          )
        ),
        outlets = Vector(
          OutletSettings(
            "out",
            "cart.clean-carts",
            "cart.cleaner.out",
            Delivery.AtLeastOnce,
            None,
            Map("bootstrap.servers" -> "kafka.kafka.svc:9092", "security.protocol" -> "PLAINTEXT")
          )
        )
      )
    )
    assertEquals(files("cleaner").properties, FlowSettings.write(parsed("cleaner")))
  }

  test("an inlet of a managed topic reads committed records only") {
    val in = parsed("sink").inlet("in").get
    assertEquals(in.topic, "cart.clean-carts")
    assertEquals(in.kafka.get("isolation.level"), Some("read_committed"))
    assertEquals(parsed("cleaner").inlet("in").get.kafka.get("isolation.level"), None)
  }

  test("a parameter the resource leaves out is written with its descriptor's default") {
    val bare = spec("cleaner").copy(config = Map.empty)
    val text = StreamletFiles.render("cart", "shop", bare, topics).toOption.get.properties
    assert(text.contains("parameter.max-total=10000\n"), text)
  }

  test("the hash is stable and covers the settings and the descriptor") {
    assertEquals(files("cleaner").hash, files("cleaner").hash)
    assertNotEquals(
      files("cleaner").copy(properties = files("cleaner").properties + " ").hash,
      files("cleaner").hash
    )
    assertNotEquals(files("cleaner").copy(descriptorJson = "{}").hash, files("cleaner").hash)
  }
