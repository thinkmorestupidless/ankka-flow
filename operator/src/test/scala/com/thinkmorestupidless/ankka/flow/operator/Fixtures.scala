package com.thinkmorestupidless.ankka.flow.operator

import java.nio.file.{Files, Paths}

import scala.concurrent.duration.*

import com.fasterxml.jackson.databind.JsonNode
import com.thinkmorestupidless.ankka.flow.crd.*

/** The cart pipeline as a resource, built from the protocol's descriptor fixtures. */
object Fixtures:

  private val mapper = FlowSerialization.mapper()
  private val root = Paths.get(sys.props.getOrElse("flow.repo.root", ".")).toAbsolutePath.normalize

  def descriptor(name: String): JsonNode =
    mapper
      .readTree(Files.readString(root.resolve(s"protocol/fixtures/descriptors/$name.json")))
      .get("streamlet")

  val settings: Settings = Settings(
    sidecarImage = Some("ankka-flow-sidecar:test"),
    clustersNamespace = "ankka-flow",
    resyncInterval = 5.minutes,
    retryMinBackoff = 1.second,
    retryMaxBackoff = 5.minutes,
    maxConcurrentReconciles = 1,
    reportingInstance = "test"
  )

  val defaultCluster: KafkaCluster =
    KafkaCluster(
      "default",
      "kafka.kafka.svc:9092",
      Map("security.protocol" -> "PLAINTEXT"),
      partitions = Some(3),
      replicas = Some(1)
    )
  val shopCluster: KafkaCluster =
    KafkaCluster("shop", "shop-kafka:9092", Map("sasl.mechanism" -> "PLAIN"))

  val observed: Observed =
    Observed(clusters = Map("default" -> defaultCluster, "shop" -> shopCluster))

  def cart(
      threshold: Int = 100,
      routerImage: String = "ghcr.io/example/cart-router:0.3.1",
      routerReplicas: Int = 1,
      withSink: Boolean = true
  ): AnkkaFlow =
    val router = StreamletSpec(
      name = "router",
      image = routerImage,
      replicas = routerReplicas,
      config = Map("review-threshold" -> mapper.readTree(threshold.toString)),
      inlets = Map("in" -> "cart-events"),
      outlets = Map("valid" -> "valid-carts", "review" -> "review-carts"),
      descriptor = descriptor("cart-router")
    )
    val sink = StreamletSpec(
      name = "sink",
      image = "ghcr.io/example/cart-sink:0.3.1",
      inlets = Map("in" -> "valid-carts"),
      descriptor = descriptor("sink")
    )
    val resource = AnkkaFlow(
      "shop",
      "cart",
      AnkkaFlowSpec(
        pipeline = "cart",
        version = "0.3.1",
        protocolVersion = "1.0",
        streamlets = if withSink then List(router, sink) else List(router),
        topics = List(
          TopicSpec(
            id = "cart-events",
            name = "shop.cart-events.v1",
            managed = false,
            cluster = Some("shop"),
            consumerConfig = Map("auto.offset.reset" -> "earliest")
          ),
          TopicSpec(
            id = "valid-carts",
            name = "cart.valid-carts",
            partitions = Some(6),
            replicas = Some(1),
            topicConfig = Map("retention.ms" -> "86400000")
          ),
          TopicSpec(id = "review-carts", name = "cart.review-carts")
        )
      )
    )
    resource.getMetadata.setUid("uid-1")
    resource.getMetadata.setGeneration(1L)
    resource

  private def streamletOf(path: String): JsonNode =
    mapper.readTree(Files.readString(root.resolve(path))).get("streamlet")

  /** The Neo4j merge sink's descriptor, from its committed canonical JSON. */
  val neo4jMergeSink: JsonNode = streamletOf("protocol/fixtures/builtin/neo4j-merge-sink.json")

  val neo4jSecret: SecretState =
    SecretState("41", Set("uri", "username", "password", "database"))

  /** What a reconcile of the graph pipeline sees when its sink's Secret exists. */
  val graphObserved: Observed = observed.copy(secrets = Map("neo4j-shop" -> neo4jSecret))

  /** A mapper in front of the built-in Neo4j merge sink (feature 002). */
  def graph(
      sinkImage: String = "",
      sinkConfig: Map[String, JsonNode] = Map(
        "secret"              -> mapper.readTree("\"neo4j-shop\""),
        "transaction-timeout" -> mapper.readTree("\"30s\"")
      ),
      sinkDescriptor: JsonNode = neo4jMergeSink
  ): AnkkaFlow =
    val mapperStreamlet = StreamletSpec(
      name = "mapper",
      image = "ghcr.io/example/checkout-graph:0.1.0",
      inlets = Map("in" -> "cart-checkouts"),
      outlets = Map("deltas" -> "graph-deltas"),
      descriptor = streamletOf("cli/src/test/resources/blueprints/graph/descriptors/mapper.json")
    )
    val sink = StreamletSpec(
      name = "graph",
      image = sinkImage,
      config = sinkConfig,
      inlets = Map("in" -> "graph-deltas"),
      descriptor = sinkDescriptor,
      builtin = true
    )
    val resource = AnkkaFlow(
      "shop",
      "checkouts",
      AnkkaFlowSpec(
        pipeline = "checkouts",
        version = "0.1.0",
        protocolVersion = "1.0",
        streamlets = List(mapperStreamlet, sink),
        topics = List(
          TopicSpec(
            id = "cart-checkouts",
            name = "cart-checkouts",
            managed = false,
            cluster = Some("default")
          ),
          TopicSpec(
            id = "graph-deltas",
            name = "checkouts.graph-deltas",
            partitions = Some(3),
            replicas = Some(1)
          )
        )
      )
    )
    resource.getMetadata.setUid("uid-2")
    resource.getMetadata.setGeneration(1L)
    resource
