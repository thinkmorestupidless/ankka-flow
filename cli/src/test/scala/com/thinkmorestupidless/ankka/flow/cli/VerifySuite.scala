package com.thinkmorestupidless.ankka.flow.cli

import java.nio.file.Files

import ankka.flow.v1.discovery.{ConfigParameter, ConfigType}
import com.thinkmorestupidless.ankka.flow.protocol.{DescriptorJson, Fingerprint}

import CliFixtures.*

/**
 * `flow verify`: every refusal in contracts/cli.md, all problems in one pass (FR-004, FR-006,
 * SC-002).
 */
class VerifySuite extends munit.FunSuite:

  private def refused(r: Result, fragments: String*): Unit =
    assertEquals(r.code, 1, s"expected a refusal; out: ${r.out} err: ${r.err}")
    fragments.foreach(f => assert(r.err.contains(f), s"stderr lacks '$f':\n${r.err}"))

  test("the cart pipeline verifies, and the version command answers") {
    val r = verify(cart)
    assertEquals(r.code, 0, r.err)
    assertEquals(r.out.trim, "verified: 2 streamlets, 3 topics")
    val v = flow("version")
    assertEquals(v.code, 0)
    assert(v.out.contains("protocol 1.0"), v.out)
  }

  test(
    "an outlet and an inlet naming different schemas are refused naming both ports and both contracts (S2.1)"
  ) {
    val dir = variant(descriptors =
      Map(
        "sink.json" -> (_.replace("cart-events.v1", "cart-events.v2").replace(
          Fingerprint.fingerprint("cart-events.v1"),
          Fingerprint.fingerprint("cart-events.v2")
        ))
      )
    )
    refused(verify(dir), "router.valid", "sink.in", "json cart-events.v1", "json cart-events.v2")
  }

  test("renaming one side verifies again") {
    val dir = variant(descriptors = Map("sink.json" -> identity))
    assertEquals(verify(dir).code, 0)
  }

  test("a format other than json is refused by name") {
    refused(
      verify(
        variant(descriptors =
          Map("sink.json" -> (_.replace("\"format\": \"json\"", "\"format\": \"avro\"")))
        )
      ),
      "format 'avro'"
    )
  }

  test("a hand-edited fingerprint is refused") {
    refused(
      verify(
        variant(descriptors =
          Map("sink.json" -> (_.replace(Fingerprint.fingerprint("cart-events.v1"), "AAAA")))
        )
      ),
      "fingerprint does not match"
    )
  }

  test("an inlet connected to nothing is refused (S2.2)") {
    refused(
      verify(variant(blueprint = _.replace("consumers  = [sink.in]", ""))),
      "Inlet sink.in is not connected"
    )
  }

  test("an outlet connected to nothing is allowed, and noted") {
    val r = verify(
      variant(blueprint = _.replace("review-carts {\n      producers = [router.review]\n    }", ""))
    )
    assertEquals(r.code, 0, r.err)
    assert(r.err.contains("note: Outlet router.review is not connected"), r.err)
  }

  test("a port no descriptor declares is refused naming it (S2.3)") {
    refused(
      verify(variant(blueprint = _.replace("[sink.in]", "[sink.input]"))),
      "'sink.input' does not point to a known streamlet inlet or outlet",
      "sink.in"
    )
  }

  test("a streamlet naming a descriptor that does not exist is refused naming both (S2.3)") {
    refused(
      verify(variant(blueprint = _.replace("sink   = sink", "sink   = cart-sink"))),
      "'sink'",
      "'cart-sink'"
    )
  }

  test("a port bound to two topics is refused") {
    refused(
      verify(
        variant(blueprint =
          _.replace("producers = [router.review]", "producers = [router.review, router.valid]")
        )
      ),
      "'router.valid' is bound to more than one topic"
    )
  }

  test("an unmanaged topic with producers, or naming no brokers or cluster, is refused (FR-005)") {
    val withProducer = variant(
      blueprint =
        _.replace("cluster    = shop", "cluster    = shop\n      producers = [router.review]")
          .replace("review-carts {\n      producers = [router.review]\n    }", "")
    )
    refused(verify(withProducer), "is not managed but has producers router.review")
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
    val router = DescriptorJson
      .read(Files.readString(cart.resolve("descriptors/cart-router.json")))
      .toOption
      .get
    val withRequired = router.withStreamlet(
      router.getStreamlet.withConfigParameters(
        router.getStreamlet.configParameters :+ ConfigParameter("region", "", ConfigType.STRING, "")
      )
    )
    val dir =
      variant(descriptors = Map("cart-router.json" -> (_ => DescriptorJson.write(withRequired))))
    refused(verify(dir), "parameter 'region' has no default and no value")
    val conf = Files.createTempFile("conf", ".conf")
    Files.writeString(
      conf,
      "flow.streamlets.router.config { region = eu, review-threshold = lots }"
    )
    refused(
      verify(dir, "--conf", conf.toString),
      "parameter 'review-threshold' = lots is not a integer"
    )
  }

  test("overrides naming an unknown topic or streamlet are refused") {
    val conf = Files.createTempFile("conf", ".conf")
    Files.writeString(conf, "flow.topics.nope.partitions = 1\nflow.streamlets.ghost.replicas = 2")
    refused(
      verify(cart, "--conf", conf.toString),
      "unknown topic 'nope'",
      "unknown streamlet 'ghost'"
    )
  }

  test("every problem comes in one pass") {
    val dir = variant(
      blueprint = _.replace("[sink.in]", "[sink.input]").replace("cluster    = shop", ""),
      descriptors =
        Map("cart-router.json" -> (_.replace("\"format\": \"json\"", "\"format\": \"avro\"")))
    )
    val r = verify(dir)
    assertEquals(r.code, 1)
    assert(r.lines.size >= 3, r.err)
    assert(
      r.err.contains("format 'avro'") && r.err.contains("sink.input") && r.err.contains(
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

  test("a built-in streamlet verifies with no descriptor file for it") {
    val r = verify(graph, "--conf", graph.resolve("overrides.conf").toString)
    assertEquals(r.code, 0, r.err)
    assertEquals(r.out.trim, "verified: 2 streamlets, 2 topics")
  }

  test("the mapper's descriptor is canonical and valid") {
    val text = Files.readString(graph.resolve("descriptors/mapper.json"))
    val spec = DescriptorJson.read(text).toOption.get
    assertEquals(DescriptorJson.write(spec), text)
    assertEquals(
      spec.getStreamlet.outlets.map(_.getContract.fingerprint),
      Seq(Fingerprint.fingerprint("ankka.graph-delta.v1"))
    )
  }

  test("an outlet of another contract connected to the built-in's inlet is refused naming both") {
    val dir =
      graphVariant(mapper =
        _.replace("ankka.graph-delta.v1", "other.v1").replace(
          Fingerprint.fingerprint("ankka.graph-delta.v1"),
          Fingerprint.fingerprint("other.v1")
        )
      )
    refused(
      verify(dir, "--conf", graph.resolve("overrides.conf").toString),
      "mapper.deltas",
      "graph.in",
      "other.v1",
      "ankka.graph-delta.v1"
    )
  }

  test("an unknown built-in is refused listing the built-ins that exist") {
    val dir = graphVariant(blueprint = _.replace("builtin/neo4j-merge-sink", "builtin/nope"))
    refused(verify(dir), "builtin/nope", "the built-ins are: neo4j-merge-sink")
  }

  test("a blueprint of built-ins alone verifies with no --descriptors") {
    val dir = Files.createTempDirectory("builtin-only")
    Files.writeString(
      dir.resolve("blueprint.conf"),
      """blueprint {
        |  streamlets { graph = builtin/neo4j-merge-sink }
        |  topics {
        |    deltas { managed = false, bootstrap.servers = "kafka:9092", consumers = [graph.in] }
        |  }
        |}
        |""".stripMargin
    )
    val conf = Files.writeString(dir.resolve("o.conf"), "flow.streamlets.graph.config.secret = s")
    val r    = flow("verify", dir.resolve("blueprint.conf").toString, "--conf", conf.toString)
    assertEquals(r.code, 0, r.err)
  }

  test("the built-in's parameters are checked like any streamlet's") {
    refused(verify(graph), "streamlet 'graph': parameter 'secret' has no default and no value")
    val conf = Files.createTempFile("graph", ".conf")
    Files.writeString(conf, "flow.streamlets.graph.config { secret = s, batch-size = 3 }")
    refused(verify(graph, "--conf", conf.toString), "parameter 'batch-size' is not declared")
  }

  // ── delta topics: compacted by default, and said so ──────────────────────────────────────

  private val graphConf = Seq("--conf", graph.resolve("overrides.conf").toString)

  private def withPolicy(policy: String) =
    graphVariant(blueprint =
      _.replace("partitions = 3", s"partitions = 3\n      topic { cleanup.policy = $policy }")
    )

  test("a managed delta topic is reported as compacted, and nothing else is") {
    val r = verify(graph, graphConf*)
    assertEquals(r.code, 0, r.err)
    assertEquals(
      r.lines.filter(_.contains("graph deltas")),
      Vector(
        "note: Topic 'graph-deltas' carries graph deltas and is compacted (cleanup.policy = compact)."
      )
    )
    // the cart pipeline carries no deltas: no note, and its output is as it was
    val plain = verify(cart)
    assertEquals(plain.err, "")
    assertEquals(plain.out.trim, "verified: 2 streamlets, 3 topics")
  }

  test("a blueprint's own cleanup policy is kept, and the note says what it gives up") {
    val deleted = verify(withPolicy("delete"), graphConf*)
    assertEquals(deleted.code, 0, deleted.err)
    assert(
      deleted.err.contains(
        "note: Topic 'graph-deltas' carries graph deltas and sets cleanup.policy = delete; it will not hold the whole graph and cannot be relied on to rebuild it."
      ),
      deleted.err
    )
    // a comma separates fields in HOCON, so a policy naming both is quoted
    val both = verify(withPolicy("\"compact,delete\""), graphConf*)
    assertEquals(both.code, 0, both.err)
    assert(
      both.err.contains(
        "note: Topic 'graph-deltas' carries graph deltas and sets cleanup.policy = compact,delete; records older than its retention are gone from a rebuild."
      ),
      both.err
    )
    val compact = verify(withPolicy("compact"), graphConf*)
    assert(
      compact.err.contains(
        "note: Topic 'graph-deltas' carries graph deltas and is compacted (cleanup.policy = compact)."
      ),
      compact.err
    )
  }

  test("a --conf cleanup policy wins over the blueprint's and over the default") {
    val conf = Files.createTempFile("policy", ".conf")
    Files.writeString(conf, "flow.topics.graph-deltas { topic { cleanup.policy = delete } }")
    Seq(graph, withPolicy("compact")).foreach { dir =>
      val r = verify(dir, (graphConf ++ Seq("--conf", conf.toString))*)
      assertEquals(r.code, 0, r.err)
      assert(
        r.err.contains("sets cleanup.policy = delete; it will not hold the whole graph"),
        r.err
      )
    }
  }

  test("an unmanaged delta topic is its owner's, and the note says so") {
    val dir = Files.createTempDirectory("unmanaged-deltas")
    Files.writeString(
      dir.resolve("blueprint.conf"),
      """blueprint {
        |  streamlets { graph = builtin/neo4j-merge-sink }
        |  topics {
        |    deltas { managed = false, bootstrap.servers = "kafka:9092", consumers = [graph.in] }
        |  }
        |}
        |""".stripMargin
    )
    val conf = Files.writeString(dir.resolve("o.conf"), "flow.streamlets.graph.config.secret = s")
    val r    = flow("verify", dir.resolve("blueprint.conf").toString, "--conf", conf.toString)
    assertEquals(r.code, 0, r.err)
    assert(
      r.err.contains(
        "note: Topic 'deltas' carries graph deltas and is not managed; whether it is compacted is its owner's."
      ),
      r.err
    )
  }

  test("usage errors exit 2") {
    assertEquals(flow("verify").code, 2)
    assertEquals(flow("frobnicate").code, 2)
  }
