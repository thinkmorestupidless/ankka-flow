package com.thinkmorestupidless.ankka.flow.operator

import java.nio.file.{Files, Paths}

import scala.concurrent.duration.*

import com.fasterxml.jackson.databind.JsonNode
import com.thinkmorestupidless.ankka.flow.crd.*

/** The cart pipeline as a resource, built from the descriptor module's fixtures. */
object Fixtures:

  private val mapper = FlowSerialization.mapper()
  private val root = Paths.get(sys.props.getOrElse("flow.repo.root", ".")).toAbsolutePath.normalize

  def descriptor(name: String): JsonNode =
    mapper
      .readTree(Files.readString(root.resolve(s"descriptor/fixtures/descriptors/$name.json")))
      .get("streamlet")

  val settings: Settings = Settings(
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

  /**
   * A cleaner reading an unmanaged topic and writing a managed one, a sink reading that, and a
   * managed topic no streamlet binds, left to the cluster's defaults.
   */
  def cart(
      maxTotal: Int = 10000,
      cleanerImage: String = "ghcr.io/example/cart-cleaner:0.6.0",
      cleanerReplicas: Int = 1,
      withSink: Boolean = true
  ): AnkkaFlow =
    val cleaner = StreamletSpec(
      name = "cleaner",
      image = cleanerImage,
      engine = "pekko-streams",
      replicas = cleanerReplicas,
      config = Map("max-total" -> mapper.readTree(maxTotal.toString)),
      inlets = Map("in" -> "cart-events"),
      outlets = Map("out" -> "clean-carts"),
      descriptor = descriptor("cart-cleaner")
    )
    val sink = StreamletSpec(
      name = "sink",
      image = "ghcr.io/example/cart-sink:0.6.0",
      engine = "pekko-streams",
      inlets = Map("in" -> "clean-carts"),
      descriptor = descriptor("sink")
    )
    val resource = AnkkaFlow(
      "shop",
      "cart",
      AnkkaFlowSpec(
        pipeline = "cart",
        version = "0.6.0",
        descriptorVersion = "2.0",
        streamlets = if withSink then List(cleaner, sink) else List(cleaner),
        topics = List(
          TopicSpec(
            id = "cart-events",
            name = "shop.cart-events.v1",
            managed = false,
            cluster = Some("shop"),
            consumerConfig = Map("auto.offset.reset" -> "earliest")
          ),
          TopicSpec(
            id = "clean-carts",
            name = "cart.clean-carts",
            partitions = Some(6),
            replicas = Some(1),
            topicConfig = Map("retention.ms" -> "86400000")
          ),
          TopicSpec(id = "cart-counts", name = "cart.cart-counts")
        )
      )
    )
    resource.getMetadata.setUid("uid-1")
    resource.getMetadata.setGeneration(1L)
    resource

  /** The state store the Flink suites observe: MinIO, as the local overlay installs it. */
  val stateStore: StateStore = StateStore(
    "s3://flow-state/dev",
    Map("s3.endpoint"       -> "http://minio.minio:9000", "s3.path-style-access" -> "true"),
    Map("AWS_ACCESS_KEY_ID" -> "minio", "AWS_SECRET_ACCESS_KEY"                  -> "minio-secret")
  )

  val flinkObserved: Observed = observed.copy(flinkCrd = true, stateStore = Some(stateStore))

  /** The cart pipeline with the counter on Flink reading the cleaner's managed output. */
  def cartWithCounter(
      counterImage: String = "ghcr.io/example/cart-counter:0.6.0",
      counterReplicas: Int = 1,
      countsManaged: Boolean = true
  ): AnkkaFlow =
    val counter = StreamletSpec(
      name = "counter",
      image = counterImage,
      engine = "flink",
      replicas = counterReplicas,
      parallelism = Some(2),
      flink = Some(
        FlinkSpec(
          entryClass = "cart.counter.CartCounter",
          version = "2.2",
          checkpointInterval = "30s",
          jobManager = Resources("1", "1024m"),
          taskManager = Resources("0.5", "2048m")
        )
      ),
      config = Map("window" -> mapper.readTree("\"1m\"")),
      inlets = Map("in" -> "clean-carts"),
      outlets = Map("out" -> "cart-counts"),
      descriptor = descriptor("cart-counter")
    )
    val r = cart(withSink = false)
    r.setSpec(
      r.getSpec.copy(
        streamlets = r.getSpec.streamlets :+ counter,
        topics = r.getSpec.topics.map(t =>
          if t.id == "cart-counts" && !countsManaged then
            t.copy(managed = false, name = "shop.cart-counts.v1", cluster = Some("shop"))
          else t
        )
      )
    )
    r
