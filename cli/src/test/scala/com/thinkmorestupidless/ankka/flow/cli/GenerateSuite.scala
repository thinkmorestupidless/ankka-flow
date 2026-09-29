package com.thinkmorestupidless.ankka.flow.cli

import java.nio.file.Files

import scala.jdk.CollectionConverters.*

import com.thinkmorestupidless.ankka.flow.crd.{FlowSerialization, OnDelete}
import com.thinkmorestupidless.ankka.flow.protocol.{DescriptorJson, Json}

import CliFixtures.*

/** `flow generate`: the resource carries everything the operator needs (S2.5). */
class GenerateSuite extends munit.FunSuite:

  private def generate(extra: String*) =
    flow(
      Seq(
        "generate",
        cart.resolve("blueprint.conf").toString,
        "--descriptors",
        cart.resolve("descriptors").toString,
        "--images",
        cart.resolve("images.conf").toString,
        "--version",
        "0.3.1",
        "-n",
        "shop"
      ) ++ extra*
    )

  test("the emitted resource carries every descriptor, image, binding and topic") {
    val r = generate("--conf", cart.resolve("overrides.conf").toString)
    assertEquals(r.code, 0, r.err)
    // Strings that look like numbers stay strings, or kubectl and the API server read numbers.
    assert(r.out.contains("protocolVersion: \"1.0\""), r.out)
    assert(r.out.contains("default_value: \"100\""), r.out)
    val resource = FlowSerialization.fromYaml(r.out)
    assertEquals(resource.getKind, "AnkkaFlow")
    assertEquals(resource.getApiVersion, "flow.ankka.thinkmorestupidless.com/v1alpha1")
    assertEquals(resource.getMetadata.getName, "cart")
    assertEquals(resource.getMetadata.getNamespace, "shop")
    val spec = resource.getSpec
    assertEquals((spec.pipeline, spec.version, spec.protocolVersion), ("cart", "0.3.1", "1.0"))

    val router = spec.streamlets.find(_.name == "router").get
    assertEquals(router.image, "ghcr.io/example/cart-router:0.3.1")
    assertEquals(router.replicas, 3)
    assertEquals(router.inlets, Map("in" -> "cart-events"))
    assertEquals(router.outlets, Map("valid" -> "valid-carts", "review" -> "review-carts"))
    assertEquals(router.config("review-threshold").asInt, 250)
    // the descriptor is embedded byte-equal to the file's streamlet object
    val file = DescriptorJson
      .read(Files.readString(cart.resolve("descriptors/cart-router.json")))
      .toOption
      .get
    val embedded = DescriptorJson
      .streamletFromJson(Json.parse(router.descriptor.toString).toOption.get)
      .toOption
      .get
    assertEquals(embedded, file.getStreamlet)
    assertEquals(spec.streamlets.find(_.name == "sink").get.replicas, 1)

    val topics = spec.topics.map(t => t.id -> t).toMap
    assertEquals(topics("cart-events").name, "shop.cart-events.v1")
    assertEquals(topics("cart-events").managed, false)
    assertEquals(topics("cart-events").cluster, Some("shop"))
    assertEquals(topics("cart-events").consumerConfig, Map("auto.offset.reset" -> "earliest"))
    assertEquals(topics("valid-carts").name, "cart.valid-carts")
    assertEquals(topics("valid-carts").partitions, Some(12))
    assertEquals(topics("valid-carts").topicConfig, Map("retention.ms" -> "86400000"))
    assertEquals(topics("review-carts").name, "cart.review-carts")
    assertEquals(topics("review-carts").partitions, None, "left for the cluster's default")
  }

  private def generateGraph(extra: String*) =
    flow(
      Seq(
        "generate",
        graph.resolve("blueprint.conf").toString,
        "--descriptors",
        graph.resolve("descriptors").toString,
        "--images",
        graph.resolve("images.conf").toString,
        "--conf",
        graph.resolve("overrides.conf").toString,
        "--version",
        "0.1.0",
        "-n",
        "shop"
      ) ++ extra*
    )

  test("a built-in streamlet needs no image and is recorded as built in") {
    val r = generateGraph()
    assertEquals(r.code, 0, r.err)
    val spec   = FlowSerialization.fromYaml(r.out).getSpec
    val sink   = spec.streamlets.find(_.name == "graph").get
    val mapper = spec.streamlets.find(_.name == "mapper").get
    assertEquals((sink.builtin, sink.image), (true, ""))
    assertEquals((mapper.builtin, mapper.image), (false, "ghcr.io/example/checkout-graph:0.1.0"))
    assertEquals(sink.config("secret").asText, "neo4j-shop")
    assertEquals(sink.config("transaction-timeout").asText, "30s")
    assertEquals(sink.inlets, Map("in" -> "graph-deltas"))
    val fixture = DescriptorJson
      .read(Files.readString(repoRoot.resolve("protocol/fixtures/builtin/neo4j-merge-sink.json")))
      .toOption
      .get
    val embedded = DescriptorJson
      .streamletFromJson(Json.parse(sink.descriptor.toString).toOption.get)
      .toOption
      .get
    assertEquals(embedded, fixture.getStreamlet)
    assert(!r.out.contains("sidecar"), r.out)
  }

  test("an image given for a built-in streamlet is refused") {
    val r = generateGraph("--image", "graph=registry/neo4j-sink:1")
    assertEquals(r.code, 1)
    assert(r.err.contains("Streamlet 'graph' is built in and takes no image."), r.err)
  }

  test("nothing in the resource names the sidecar image (FR-020)") {
    val r = generate()
    assertEquals(r.code, 0, r.err)
    assert(!r.out.contains("sidecar"), r.out)
  }

  test("a streamlet without an image is refused; --image supplies one") {
    val dir = variant()
    Files.writeString(dir.resolve("images.conf"), "router = \"r:1\"")
    val base = Seq(
      "generate",
      dir.resolve("blueprint.conf").toString,
      "--descriptors",
      dir.resolve("descriptors").toString,
      "--images",
      dir.resolve("images.conf").toString
    )
    val refused = flow(base*)
    assertEquals(refused.code, 1)
    assert(refused.err.contains("Streamlet 'sink' has no image"), refused.err)
    assertEquals(flow((base ++ Seq("--image", "sink=s:1"))*).code, 0)
  }

  test("-o writes the file; the pipeline id comes from --pipeline, else blueprint.name") {
    val out = Files.createTempFile("cart", ".yaml")
    val r   = generate("-o", out.toString, "--pipeline", "cart-eu")
    assertEquals(r.code, 0, r.err)
    val resource = FlowSerialization.fromYaml(Files.readString(out))
    assertEquals(resource.getSpec.pipeline, "cart-eu")
    assertEquals(
      resource.getSpec.topics.find(_.id == "valid-carts").get.name,
      "cart-eu.valid-carts"
    )
    assert(
      resource.getMetadata.getLabels.asScala.contains("flow.ankka.thinkmorestupidless.com/pipeline")
    )
  }

  test("managed topics are kept on delete unless --delete-managed-topics says otherwise") {
    // Deleting a topic loses its data, so it is never the default: the flag is the only way to ask.
    val kept = generate()
    assertEquals(kept.code, 0, kept.err)
    assertEquals(FlowSerialization.fromYaml(kept.out).getSpec.onDelete, OnDelete(OnDelete.Keep))
    val deleted = generate("--delete-managed-topics")
    assertEquals(deleted.code, 0, deleted.err)
    assertEquals(
      FlowSerialization.fromYaml(deleted.out).getSpec.onDelete,
      OnDelete(OnDelete.Delete)
    )
  }

  test("an illegal pipeline id is refused") {
    assertEquals(generate("--pipeline", "Cart_EU").code, 1)
  }
