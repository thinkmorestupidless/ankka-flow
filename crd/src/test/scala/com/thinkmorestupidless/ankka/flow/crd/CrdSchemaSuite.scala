package com.thinkmorestupidless.ankka.flow.crd

import scala.jdk.CollectionConverters.*

import com.fasterxml.jackson.databind.JsonNode
import io.fabric8.kubernetes.api.model.apiextensions.v1.{CustomResourceDefinition, JSONSchemaProps}

/**
 * The hand-written CRD and the Scala model agree: every field the model has is declared in the
 * schema, and the resource in contracts/resource-and-operator.md round-trips.
 */
class CrdSchemaSuite extends munit.FunSuite:

  private val crd: CustomResourceDefinition =
    FlowSerialization().unmarshal(
      getClass.getResourceAsStream(AnkkaFlowDefinition.manifestResource),
      classOf[CustomResourceDefinition]
    )

  private val schema = crd.getSpec.getVersions.get(0).getSchema.getOpenAPIV3Schema

  private def fields[A](c: Class[A]): Set[String] =
    c.getDeclaredFields.toSet.map(_.getName).filterNot(n => n.contains("$") || n == "MODULE")

  private def props(p: JSONSchemaProps): Set[String] = p.getProperties.asScala.keySet.toSet

  private def item(p: JSONSchemaProps, name: String) = p.getProperties.get(name).getItems.getSchema

  test("the definition's identity is the model's") {
    assertEquals(crd.getMetadata.getName, AnkkaFlowDefinition.crdName)
    assertEquals(crd.getSpec.getGroup, AnkkaFlowDefinition.group)
    assertEquals(crd.getSpec.getNames.getKind, AnkkaFlowDefinition.kind)
    assertEquals(crd.getSpec.getNames.getShortNames.asScala.toList, List("aflow"))
  }

  test("every field of the spec, streamlet, topic, batch and status is declared") {
    val spec   = schema.getProperties.get("spec")
    val status = schema.getProperties.get("status")
    assertEquals(props(spec), fields(classOf[AnkkaFlowSpec]))
    assertEquals(props(spec.getProperties.get("onDelete")), fields(classOf[OnDelete]))
    assertEquals(props(item(spec, "streamlets")), fields(classOf[StreamletSpec]))
    assertEquals(props(item(spec, "topics")), fields(classOf[TopicSpec]))
    assertEquals(props(item(spec, "topics").getProperties.get("batch")), fields(classOf[BatchSpec]))
    assertEquals(props(status), fields(classOf[AnkkaFlowStatus]))
    assertEquals(props(item(status, "streamlets")), fields(classOf[StreamletStatus]))
    assertEquals(props(item(status, "topics")), fields(classOf[TopicStatus]))
  }

  test("a resource round-trips, the descriptor object and optional numbers intact") {
    val mapper = FlowSerialization.mapper()
    val descriptor = mapper.readTree(
      """{"name":"cart-router","inlets":[{"name":"in","contract":{"format":"json","schema_name":"cart-events.v1","fingerprint":"x"}}]}"""
    )
    val spec = AnkkaFlowSpec(
      pipeline = "cart",
      version = "0.3.1",
      protocolVersion = "1.0",
      streamlets = List(
        StreamletSpec(
          "router",
          "img:1",
          3,
          Map("review-threshold" -> mapper.readTree("100")),
          Map("in"               -> "cart-events"),
          Map("valid"            -> "valid-carts"),
          descriptor
        )
      ),
      topics = List(
        TopicSpec(
          id = "cart-events",
          name = "shop.cart-events.v1",
          managed = false,
          cluster = Some("shop")
        ),
        TopicSpec(
          id = "valid-carts",
          name = "cart.valid-carts",
          partitions = Some(6),
          replicas = Some(1),
          topicConfig = Map("retention.ms" -> "86400000")
        )
      )
    )
    val resource = AnkkaFlow("shop", "cart", spec)
    val yaml     = FlowSerialization.toYaml(resource)
    val back     = FlowSerialization.fromYaml(yaml)
    // and through fabric8's own serialization, as the operator reads it from the API server
    val viaClient =
      FlowSerialization().unmarshal(FlowSerialization().asJson(resource), classOf[AnkkaFlow])
    assertEquals(viaClient.getSpec, spec)
    assertEquals(back.getSpec, spec)
    assertEquals(back.getSpec.topics(1).partitions.map(_ + 1), Some(7))
    assertEquals(back.getSpec.streamlets.head.descriptor.get("name").asText, "cart-router")
    assert(!yaml.contains("empty"), yaml)
    val _: JsonNode = back.getSpec.streamlets.head.config("review-threshold")
  }
