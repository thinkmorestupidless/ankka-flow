package com.thinkmorestupidless.ankka.flow.cli

import java.nio.file.Files

import scala.jdk.CollectionConverters.*

import com.thinkmorestupidless.ankka.flow.crd.{FlinkSpec, FlowSerialization, OnDelete, Resources}
import com.thinkmorestupidless.ankka.flow.descriptor.{DescriptorJson, Json}

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
        "0.6.0",
        "-n",
        "shop"
      ) ++ extra*
    )

  private def embedded(node: com.fasterxml.jackson.databind.JsonNode) =
    DescriptorJson.streamletFromJson(Json.parse(node.toString).toOption.get).toOption.get

  private def fromFile(name: String) =
    DescriptorJson.read(Files.readString(cart.resolve(s"descriptors/$name"))).toOption.get.streamlet

  test("the emitted resource carries every descriptor, engine, image, binding and topic") {
    val r = generate("--conf", cart.resolve("overrides.conf").toString)
    assertEquals(r.code, 0, r.err)
    // Strings that look like numbers stay strings, or kubectl and the API server read numbers.
    assert(r.out.contains("descriptorVersion: \"2.0\""), r.out)
    assert(r.out.contains("default_value: \"10000\""), r.out)
    val resource = FlowSerialization.fromYaml(r.out)
    assertEquals(resource.getKind, "AnkkaFlow")
    assertEquals(resource.getApiVersion, "flow.ankka.thinkmorestupidless.com/v1alpha1")
    assertEquals(resource.getMetadata.getName, "carts")
    assertEquals(resource.getMetadata.getNamespace, "shop")
    val spec = resource.getSpec
    assertEquals((spec.pipeline, spec.version, spec.descriptorVersion), ("carts", "0.6.0", "2.0"))

    val cleaner = spec.streamlets.find(_.name == "cleaner").get
    assertEquals(cleaner.image, "ghcr.io/example/cart-cleaner:0.6.0")
    assertEquals(cleaner.engine, "pekko-streams")
    assertEquals(cleaner.replicas, 3)
    assertEquals((cleaner.parallelism, cleaner.flink), (None, None))
    assertEquals(cleaner.inlets, Map("in" -> "cart-events"))
    assertEquals(cleaner.outlets, Map("out" -> "clean-carts"))
    assertEquals(cleaner.config("max-total").asInt, 250)
    // the descriptor is embedded equal to the file's streamlet object
    assertEquals(embedded(cleaner.descriptor), fromFile("cart-cleaner.json"))

    val counter = spec.streamlets.find(_.name == "counter").get
    assertEquals(counter.image, "ghcr.io/example/cart-counter:0.6.0")
    assertEquals(counter.engine, "flink")
    assertEquals(counter.replicas, 1)
    assertEquals(counter.parallelism, Some(2))
    assertEquals(
      counter.flink,
      Some(
        FlinkSpec(
          entryClass = "cart.counter.CartCounter",
          version = "2.2",
          checkpointInterval = "30s",
          jobManager = Resources("1", "1024m"),
          taskManager = Resources("1", "2048m")
        )
      )
    )
    assertEquals(counter.config("window").asText, "1m")
    assertEquals(embedded(counter.descriptor), fromFile("cart-counter.json"))

    val topics = spec.topics.map(t => t.id -> t).toMap
    assertEquals(topics("cart-events").name, "shop.cart-events.v1")
    assertEquals(topics("cart-events").managed, false)
    assertEquals(topics("cart-events").cluster, Some("shop"))
    assertEquals(topics("cart-events").consumerConfig, Map("auto.offset.reset" -> "earliest"))
    assertEquals(topics("clean-carts").name, "carts.clean-carts")
    assertEquals(topics("clean-carts").partitions, Some(12))
    assertEquals(topics("clean-carts").topicConfig, Map("retention.ms" -> "86400000"))
    assertEquals(topics("cart-counts").name, "carts.cart-counts")
    assertEquals(topics("cart-counts").partitions, None, "left for the cluster's default")
  }

  test("a Flink streamlet with no deploy-time settings gets the job defaults") {
    val r = generate()
    assertEquals(r.code, 0, r.err)
    val counter = FlowSerialization.fromYaml(r.out).getSpec.streamlets.find(_.name == "counter").get
    assertEquals(counter.parallelism, None)
    assertEquals(
      counter.flink,
      Some(FlinkSpec(entryClass = "cart.counter.CartCounter", version = "2.2"))
    )
    assertEquals(counter.flink.map(_.checkpointInterval), Some("10s"))
  }

  test("nothing in the resource is version one's: no protocol version, built-in or batch") {
    val r = generate()
    assertEquals(r.code, 0, r.err)
    assertEquals(r.err, "")
    Seq("protocolVersion", "builtin", "batch", "maxRecords").foreach(w =>
      assert(!r.out.contains(w), s"$w in:\n${r.out}")
    )
  }

  test("nothing in the resource names the sidecar image (FR-020)") {
    val r = generate()
    assertEquals(r.code, 0, r.err)
    assert(!r.out.contains("sidecar"), r.out)
  }

  test("a streamlet without an image is refused; --image supplies one") {
    val dir = variant()
    Files.writeString(dir.resolve("images.conf"), "cleaner = \"c:1\"")
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
    assert(refused.err.contains("Streamlet 'counter' has no image"), refused.err)
    assertEquals(flow((base ++ Seq("--image", "counter=k:1"))*).code, 0)
  }

  test("-o writes the file; the pipeline id comes from --pipeline, else blueprint.name") {
    val out = Files.createTempFile("cart", ".yaml")
    val r   = generate("-o", out.toString, "--pipeline", "carts-eu")
    assertEquals(r.code, 0, r.err)
    val resource = FlowSerialization.fromYaml(Files.readString(out))
    assertEquals(resource.getSpec.pipeline, "carts-eu")
    assertEquals(
      resource.getSpec.topics.find(_.id == "clean-carts").get.name,
      "carts-eu.clean-carts"
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
