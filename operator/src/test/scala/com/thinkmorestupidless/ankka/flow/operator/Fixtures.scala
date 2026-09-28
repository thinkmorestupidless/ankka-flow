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
