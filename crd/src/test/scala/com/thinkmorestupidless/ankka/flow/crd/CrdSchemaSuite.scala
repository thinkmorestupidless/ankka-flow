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

  private def obj(p: JSONSchemaProps, name: String) = p.getProperties.get(name)

  test("every field of the spec, streamlet, flink, topic and status is declared") {
    val spec      = schema.getProperties.get("spec")
    val status    = schema.getProperties.get("status")
    val streamlet = item(spec, "streamlets")
    val flink     = obj(streamlet, "flink")
    assertEquals(props(spec), fields(classOf[AnkkaFlowSpec]))
    assertEquals(props(obj(spec, "onDelete")), fields(classOf[OnDelete]))
    assertEquals(props(streamlet), fields(classOf[StreamletSpec]))
    assertEquals(props(flink), fields(classOf[FlinkSpec]))
    assertEquals(props(obj(flink, "jobManager")), fields(classOf[Resources]))
    assertEquals(props(obj(flink, "taskManager")), fields(classOf[Resources]))
    assertEquals(props(item(spec, "topics")), fields(classOf[TopicSpec]))
    assertEquals(props(obj(item(spec, "topics"), "ankka")), fields(classOf[AnkkaRef]))
    assertEquals(props(status), fields(classOf[AnkkaFlowStatus]))
    assertEquals(props(item(status, "conditions")), fields(classOf[Condition]))
    assertEquals(props(item(status, "streamlets")), fields(classOf[StreamletStatus]))
    assertEquals(props(item(item(status, "streamlets"), "inlets")), fields(classOf[InletStatus]))
    assertEquals(props(item(status, "topics")), fields(classOf[TopicStatus]))
  }

  test("version one's fields are gone") {
    val spec = schema.getProperties.get("spec")
    assert(!props(spec).contains("protocolVersion"))
    assert(!props(item(spec, "streamlets")).contains("builtin"))
    assert(!props(item(spec, "topics")).contains("batch"))
  }

  test("a streamlet requires a name and a descriptor") {
    val spec = schema.getProperties.get("spec")
    assertEquals(item(spec, "streamlets").getRequired.asScala.toSet, Set("name", "descriptor"))
  }

  test("a resource round-trips, the descriptor object and optional numbers intact") {
    val mapper = FlowSerialization.mapper()
    val descriptor = mapper.readTree(
      """{"name":"cart-counter","engine":"flink","inlets":[{"name":"in","contract":{"name":"clean-carts.v1"}}]}"""
    )
    val spec = AnkkaFlowSpec(
      pipeline = "carts",
      version = "0.6.0",
      descriptorVersion = "2.0",
      streamlets = List(
        StreamletSpec(
          name = "cleaner",
          image = "img:1",
          engine = "pekko-streams",
          replicas = 3,
          config = Map("max-total" -> mapper.readTree("10000")),
          inlets = Map("in" -> "cart-events"),
          outlets = Map("out" -> "clean-carts"),
          descriptor = descriptor
        ),
        StreamletSpec(
          name = "counter",
          image = "img:2",
          engine = "flink",
          parallelism = Some(2),
          flink = Some(
            FlinkSpec(
              entryClass = "cart.counter.CartCounter",
              version = "2.2",
              taskManager = Resources("1", "2048m")
            )
          ),
          config = Map("window" -> mapper.readTree("\"1m\"")),
          inlets = Map("in" -> "clean-carts"),
          descriptor = descriptor
        )
      ),
      topics = List(
        TopicSpec(
          id = "cart-events",
          name = "checkout.cart-checkouts",
          managed = false,
          ankka = Some(AnkkaRef("checkout", "cart-checkouts")),
          cluster = Some("ankka")
        ),
        TopicSpec(
          id = "clean-carts",
          name = "carts.clean-carts",
          partitions = Some(6),
          replicas = Some(1),
          topicConfig = Map("retention.ms" -> "86400000")
        )
      )
    )
    val resource = AnkkaFlow("shop", "carts", spec)
    val yaml     = FlowSerialization.toYaml(resource)
    val back     = FlowSerialization.fromYaml(yaml)
    // and through fabric8's own serialization, as the operator reads it from the API server
    val viaClient =
      FlowSerialization().unmarshal(FlowSerialization().asJson(resource), classOf[AnkkaFlow])
    assertEquals(viaClient.getSpec, spec)
    assertEquals(back.getSpec, spec)
    assertEquals(back.getSpec.topics(1).partitions.map(_ + 1), Some(7))
    assertEquals(back.getSpec.streamlets(1).parallelism.map(_ + 1), Some(3))
    assertEquals(back.getSpec.streamlets.head.descriptor.get("name").asText, "cart-counter")
    assert(!yaml.contains("empty"), yaml)
    assertEquals(back.getSpec.streamlets.head.parallelism, None)
    assertEquals(back.getSpec.streamlets(1).flink.map(_.checkpointInterval), Some("10s"))
    assertEquals(back.getSpec.streamlets(1).flink.map(_.jobManager), Some(Resources("1", "1024m")))
    val _: JsonNode = back.getSpec.streamlets.head.config("max-total")
  }

  test("a status round-trips with its conditions and its inlets' lag") {
    val status = AnkkaFlowStatus(
      phase = AnkkaFlowStatus.Degraded,
      conditions = List(
        Condition(Condition.TopicNotGranted, "True", "Read", "no grant", "2026-11-22T10:00:00Z"),
        Condition(Condition.Ready, "False", "Degraded", "", "2026-11-22T10:00:00Z")
      ),
      streamlets = List(
        StreamletStatus(
          name = "counter",
          engine = "flink",
          desired = 1,
          ready = 1,
          detail = "running",
          inlets = List(
            InletStatus("in", "carts.clean-carts", "carts.counter.in", Some(12L)),
            InletStatus("other", "carts.other", "carts.counter.other", None)
          )
        )
      )
    )
    val resource = AnkkaFlow("shop", "carts", AnkkaFlowSpec(pipeline = "carts"))
    resource.setStatus(status)
    val back = FlowSerialization.fromYaml(FlowSerialization.toYaml(resource))
    assertEquals(back.getStatus, status)
    assertEquals(back.getStatus.streamlets.head.inlets.head.lag.map(_ + 1), Some(13L))
  }

  test("a report differing only in its times is the same report") {
    val a = AnkkaFlowStatus(
      phase = "Ready",
      lastTransitionTime = "t1",
      conditions = List(Condition(Condition.Ready, "True", "Ready", "", "t1"))
    )
    val b = a.copy(
      lastTransitionTime = "t2",
      conditions = List(Condition(Condition.Ready, "True", "Ready", "", "t2"))
    )
    assert(a.sameReport(b))
    assert(!a.sameReport(b.copy(phase = "Degraded")))
  }
