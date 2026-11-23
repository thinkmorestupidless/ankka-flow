package com.thinkmorestupidless.ankka.flow.blueprint

import Builders.*

/** A topic named by an ankka project and name: what it parses to, and what it may not say. */
class AnkkaTopicsSuite extends munit.FunSuite:

  private val cleaner = descriptor(
    name = "cleaner",
    inlets = Seq("in" -> json("checkout-notice.v1")),
    outlets = Seq("out" -> json("clean-checkout.v1"))
  )

  private def blueprint(checkouts: String, cleaned: String = ""): Blueprint =
    Blueprint.parseString(
      s"""blueprint {
         |  name = shop
         |  streamlets { cleaner = cleaner }
         |  topics {
         |    checkouts { $checkouts, consumers = [cleaner.in] }
         |    cleaned { ankka = "checkout/clean-checkouts", producers = [cleaner.out] $cleaned }
         |  }
         |}
         |""".stripMargin,
      Vector(cleaner)
    )

  private def messages(b: Blueprint): Vector[String] = b.problems.map(BlueprintProblem.toMessage)

  private def topic(b: Blueprint, id: String): VerifiedTopic =
    b.verified
      .fold(p => fail(p.map(BlueprintProblem.toMessage).mkString("\n")), identity)
      .topics
      .find(_.id == id)
      .get

  test("`ankka = \"project/name\"` parses to the project and the name") {
    assertEquals(
      AnkkaTopicRef.parse("checkout/cart-checkouts"),
      Some(AnkkaTopicRef("checkout", "cart-checkouts"))
    )
    val t = topic(blueprint("""ankka = "checkout/cart-checkouts""""), "checkouts")
    assertEquals(t.ankka, Some(AnkkaTopicRef("checkout", "cart-checkouts")))
  }

  test("its Kafka name is <project>.<name>, whatever the pipeline") {
    val b = blueprint("""ankka = "checkout/cart-checkouts"""")
    assertEquals(topic(b, "checkouts").kafkaName("shop"), "checkout.cart-checkouts")
    assertEquals(topic(b, "cleaned").kafkaName("other"), "checkout.clean-checkouts")
  }

  test("it is unmanaged, and may have producers: the pipeline writes it but never creates it") {
    val b = blueprint("""ankka = "checkout/cart-checkouts"""")
    assertEquals(messages(b), Vector.empty)
    assertEquals(topic(b, "checkouts").managed, false)
    assertEquals(topic(b, "cleaned").managed, false)
    assertEquals(topic(b, "cleaned").producers.map(_.portPath.toString), Vector("cleaner.out"))
  }

  test("its cluster is `ankka`, the overrides' flow.ankka.cluster, or its own cluster") {
    val b = blueprint("""ankka = "checkout/cart-checkouts", cluster = mine""")
    assertEquals(topic(b, "cleaned").clusterFor(Overrides.DefaultAnkkaCluster), Some("ankka"))
    assertEquals(topic(b, "cleaned").clusterFor("pipelines"), Some("pipelines"))
    assertEquals(topic(b, "checkouts").clusterFor("pipelines"), Some("mine"))
  }

  test("an ankka topic declared managed is refused") {
    assertEquals(
      messages(blueprint("""ankka = "checkout/cart-checkouts", managed = true""")),
      Vector(
        "Topic 'checkouts' is an ankka topic, and an ankka topic is the ankka project's: a pipeline never creates it."
      )
    )
  }

  test("an ankka topic that also names its Kafka topic or brokers is refused") {
    assertEquals(
      messages(blueprint("""ankka = "checkout/cart-checkouts", topic.name = "x"""")),
      Vector(
        "Topic 'checkouts' is an ankka topic and names topic.name; ankka names its Kafka topic and its brokers."
      )
    )
    assertEquals(
      messages(blueprint("""ankka = "checkout/cart-checkouts", bootstrap.servers = "k:9092"""")),
      Vector(
        "Topic 'checkouts' is an ankka topic and names bootstrap.servers; ankka names its Kafka topic and its brokers."
      )
    )
  }

  test("a value that is not project/name is refused") {
    for bad <- Seq("cart-checkouts", "checkout/", "/cart-checkouts", "a/b/c", "Checkout/x") do
      assertEquals(
        messages(blueprint(s"""ankka = "$bad"""")),
        Vector(
          s"Topic 'checkouts' names ankka topic '$bad', which is not <project>/<name>."
        ),
        bad
      )
  }
