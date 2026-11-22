package com.thinkmorestupidless.ankka.flow.cli

import java.nio.file.Files

import com.thinkmorestupidless.ankka.flow.descriptor.{DescriptorJson, Parameter, ParameterType}

import CliFixtures.*

/**
 * `flow verify`: every refusal in contracts/cli.md, all problems in one pass (FR-004, FR-006,
 * SC-002).
 */
class VerifySuite extends munit.FunSuite:

  private def refused(r: Result, fragments: String*): Unit =
    assertEquals(r.code, 1, s"expected a refusal; out: ${r.out} err: ${r.err}")
    fragments.foreach(f => assert(r.err.contains(f), s"stderr lacks '$f':\n${r.err}"))

  private def conf(text: String) =
    val f = Files.createTempFile("conf", ".conf")
    Files.writeString(f, text)
    f.toString

  test("a binary switch naming no executable is refused, not quietly run in process") {
    val failure = intercept[IllegalArgumentException] {
      CliFixtures.Driver.from(Some("/nowhere/flow"))
    }
    assert(failure.getMessage.contains("flow.cli.binary"), failure.getMessage)
    assertEquals(CliFixtures.Driver.from(None), CliFixtures.Driver.InProcess)
  }

  test("the cart pipeline verifies, and the version command answers") {
    val r = verify(cart)
    assertEquals(r.code, 0, r.err)
    assertEquals(r.out.trim, "verified: 2 streamlets, 3 topics")
    assertEquals(r.err, "")
    val v = flow("version")
    assertEquals(v.code, 0)
    assert(v.out.contains("descriptor 2.0"), v.out)
  }

  test("the cart's descriptors are the descriptor module's fixtures, byte for byte") {
    for name <- Vector("cart-cleaner.json", "cart-counter.json") do
      assertEquals(
        Files.readString(cart.resolve(s"descriptors/$name")),
        Files.readString(repoRoot.resolve(s"descriptor/fixtures/descriptors/$name")),
        name
      )
  }

  test(
    "an outlet and an inlet naming different contracts are refused naming both ports and both contracts (S2.1)"
  ) {
    val dir = variant(descriptors =
      Map("cart-counter.json" -> (_.replace("clean-carts.v1", "clean-carts.v2")))
    )
    refused(
      verify(dir),
      "cleaner.out",
      "counter.in",
      "contract clean-carts.v1",
      "contract clean-carts.v2"
    )
  }

  test("renaming one side verifies again") {
    val dir = variant(descriptors = Map("cart-counter.json" -> identity))
    assertEquals(verify(dir).code, 0)
  }

  test("a malformed fingerprint is refused") {
    refused(
      verify(
        variant(descriptors = Map("cart-cleaner.json" -> (_.replace("sha256:f19f", "sha256:F19F"))))
      ),
      "descriptor cart-cleaner.json",
      "must be sha256: and 64 lowercase hex digits"
    )
  }

  test("a descriptor naming an engine flow does not know is refused") {
    refused(
      verify(
        variant(descriptors =
          Map("cart-counter.json" -> (_.replace("\"engine\": \"flink\"", "\"engine\": \"spark\"")))
        )
      ),
      "descriptor cart-counter.json",
      "engine 'spark' is not one flow knows (pekko-streams, flink)"
    )
  }

  test("a descriptor that names no engine is refused") {
    refused(
      verify(
        variant(descriptors =
          Map("cart-cleaner.json" -> (_.replace("\"engine\": \"pekko-streams\",", "")))
        )
      ),
      "descriptor cart-cleaner.json",
      "a descriptor must name an engine (pekko-streams, flink)"
    )
  }

  test("a version one descriptor is refused: it names no engine") {
    refused(
      verify(variant(extraDescriptors = Map("cart-router.json" -> Files.readString(v1Router)))),
      "descriptor cart-router.json",
      "a descriptor must name an engine (pekko-streams, flink)"
    )
  }

  test("an inlet connected to nothing is refused (S2.2)") {
    refused(
      verify(variant(blueprint = _.replace("consumers  = [counter.in]", ""))),
      "Inlet counter.in is not connected"
    )
  }

  test("an outlet connected to nothing is allowed, and noted") {
    val r = verify(
      variant(blueprint = _.replace("cart-counts {\n      producers = [counter.out]\n    }", ""))
    )
    assertEquals(r.code, 0, r.err)
    assert(r.err.contains("note: Outlet counter.out is not connected"), r.err)
  }

  test("a port no descriptor declares is refused naming it (S2.3)") {
    refused(
      verify(variant(blueprint = _.replace("[counter.in]", "[counter.input]"))),
      "'counter.input' does not point to a known streamlet inlet or outlet",
      "counter.in"
    )
  }

  test("a streamlet naming a descriptor that does not exist is refused naming both (S2.3)") {
    refused(
      verify(variant(blueprint = _.replace("counter = cart-counter", "counter = cart-tally"))),
      "'counter'",
      "'cart-tally'"
    )
  }

  test("a port bound to two topics is refused") {
    refused(
      verify(
        variant(blueprint =
          _.replace("producers = [counter.out]", "producers = [counter.out, cleaner.out]")
        )
      ),
      "'cleaner.out' is bound to more than one topic"
    )
  }

  test("an unmanaged topic with producers, or naming no brokers or cluster, is refused (FR-005)") {
    val withProducer = variant(
      blueprint =
        _.replace("cluster    = shop", "cluster    = shop\n      producers = [counter.out]")
          .replace("cart-counts {\n      producers = [counter.out]\n    }", "")
    )
    refused(verify(withProducer), "is not managed but has producers counter.out")
    refused(
      verify(variant(blueprint = _.replace("cluster    = shop", ""))),
      "names no bootstrap.servers or cluster"
    )
  }

  test("an illegal topic name is refused") {
    refused(
      verify(variant(blueprint = _.replace("\"shop.cart-events.v1\"", "\"shop cart events!\""))),
      "is not a valid topic name"
    )
  }

  test("a required parameter with no value, and a value that is not its type, are refused") {
    val cleaner = DescriptorJson
      .read(Files.readString(cart.resolve("descriptors/cart-cleaner.json")))
      .toOption
      .get
    val withRequired = cleaner.copy(streamlet =
      cleaner.streamlet.copy(parameters =
        cleaner.streamlet.parameters :+ Parameter("region", "", ParameterType.String, None)
      )
    )
    val dir =
      variant(descriptors = Map("cart-cleaner.json" -> (_ => DescriptorJson.write(withRequired))))
    refused(verify(dir), "parameter 'region' has no default and no value")
    refused(
      verify(
        dir,
        "--conf",
        conf("flow.streamlets.cleaner.config { region = eu, max-total = lots }")
      ),
      "parameter 'max-total' = lots is not a integer"
    )
  }

  test("overrides naming an unknown topic or streamlet are refused") {
    refused(
      verify(
        cart,
        "--conf",
        conf("flow.topics.nope.partitions = 1\nflow.streamlets.ghost.replicas = 2")
      ),
      "unknown topic 'nope'",
      "unknown streamlet 'ghost'"
    )
  }

  test("the cart's deploy-time configuration verifies") {
    val r = verify(cart, "--conf", cart.resolve("overrides.conf").toString)
    assertEquals(r.code, 0, r.err)
  }

  test("parallelism or flink on a Pekko Streams streamlet is refused, naming the key and engine") {
    val r = verify(cart, "--conf", conf("flow.streamlets.cleaner { parallelism = 2 }"))
    refused(r, "'cleaner'", "pekko-streams", "'parallelism'")
    assertEquals(r.lines.count(_.contains("'parallelism'")), 1, r.err)
    refused(
      verify(cart, "--conf", conf("flow.streamlets.cleaner.flink.checkpoint-interval = 10s")),
      "'cleaner'",
      "pekko-streams",
      "'flink'"
    )
  }

  test("a Flink streamlet's replicas may only be 0 or 1") {
    refused(
      verify(cart, "--conf", conf("flow.streamlets.counter.replicas = 2")),
      "streamlet 'counter': a flink streamlet's replicas may only be 0 or 1"
    )
    assertEquals(verify(cart, "--conf", conf("flow.streamlets.counter.replicas = 0")).code, 0)
  }

  test("consumer-config.flow.batch is refused by name") {
    refused(
      verify(
        variant(blueprint =
          _.replace(
            "consumer-config { auto.offset.reset = earliest }",
            "consumer-config { auto.offset.reset = earliest, flow.batch.max-records = 10 }"
          )
        )
      ),
      "Topic 'cart-events' sets consumer-config.flow.batch, which is no longer a setting"
    )
  }

  test("every problem comes in one pass") {
    val dir = variant(
      blueprint = _.replace("[counter.in]", "[counter.input]").replace("cluster    = shop", ""),
      descriptors = Map("cart-cleaner.json" -> (_.replace("sha256:f19f", "sha256:F19F")))
    )
    val r = verify(dir)
    assertEquals(r.code, 1)
    assert(r.lines.size >= 3, r.err)
    assert(
      r.err.contains("lowercase hex") && r.err.contains("counter.input") && r.err.contains(
        "names no bootstrap.servers"
      ),
      r.err
    )
  }

  test("verify needs no image, no network and no language runtime (FR-006)") {
    val dir = variant()
    Files.delete(dir.resolve("images.conf"))
    assertEquals(verify(dir).code, 0)
  }

  test("usage errors exit 2") {
    assertEquals(flow("verify").code, 2)
    assertEquals(flow("frobnicate").code, 2)
  }
