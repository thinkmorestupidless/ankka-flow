package com.thinkmorestupidless.ankka.flow.operator

import java.io.ByteArrayInputStream
import java.time.Duration as JDuration
import java.util.{Properties, UUID}

import scala.collection.mutable
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.Try

import com.thinkmorestupidless.ankka.flow.crd.*
import io.fabric8.kubernetes.client.{Config, KubernetesClient, KubernetesClientBuilder}
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.testcontainers.k3s.K3sContainer
import org.testcontainers.utility.DockerImageName

/**
 * The real thing on k3s (US3, US4): the operator and a Kafka running in the cluster, the cart
 * pipeline applied, and the pipeline's promises checked from outside through Kafka's external
 * listener. Skipped with `-Dflow.cluster.tests=off`.
 */
class FlowClusterSuite extends munit.FunSuite:

  override def munitIgnore: Boolean         = sys.props.get("flow.cluster.tests").contains("off")
  override val munitTimeout: FiniteDuration = 15.minutes

  private val K3sImage      = "rancher/k3s:v1.35.1-k3s1"
  private val KafkaImage    = sys.props.getOrElse("flow.kafka.image", "apache/kafka:3.9.1")
  private val tag           = sys.props.getOrElse("flow.image.tag", "latest")
  private val operatorImage = s"ankka-flow-operator:$tag"
  private val sidecarImage  = s"ankka-flow-sidecar:$tag"
  private val sampleImage   = s"sample-cart-router:$tag"
  private val NodePort      = 30094
  private val Namespace     = "shop"

  private var k3s: K3sContainer        = scala.compiletime.uninitialized
  private var client: KubernetesClient = scala.compiletime.uninitialized
  private var external: String         = ""

  // ── helpers ──────────────────────────────────────────────────────────────────────────────

  def eventually[A](within: FiniteDuration, what: String)(check: => A): A =
    val deadline        = within.fromNow
    var last: Throwable = null
    while deadline.hasTimeLeft() do
      try return check
      catch
        case e: (AssertionError | Exception) =>
          last = e
          Thread.sleep(2000)
    throw new AssertionError(
      s"timed out after $within waiting for $what: ${Option(last).map(_.getMessage).getOrElse("")}",
      last
    )

  private def admin[A](f: Admin => A): A =
    val p = new Properties()
    p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, external)
    val a = Admin.create(p)
    try f(a)
    finally a.close()

  private def partitionsOf(topic: String): Option[Int] =
    Try(
      admin(
        _.describeTopics(java.util.List.of(topic)).allTopicNames().get().get(topic).partitions.size
      )
    ).toOption

  private def produce(topic: String, records: Seq[(String, String)]): Unit =
    val p = new Properties()
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, external)
    val producer = new KafkaProducer(p, new ByteArraySerializer, new ByteArraySerializer)
    try
      records.foreach((k, v) =>
        producer.send(new ProducerRecord(topic, k.getBytes, v.getBytes)).get()
      )
    finally producer.close()

  private def consume(
      topics: Seq[String],
      atLeast: Int,
      within: FiniteDuration = 2.minutes
  ): Vector[ConsumerRecord[Array[Byte], Array[Byte]]] =
    val p = new Properties()
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, external)
    p.put(ConsumerConfig.GROUP_ID_CONFIG, s"verify-${UUID.randomUUID()}")
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    val c = new KafkaConsumer(p, new ByteArrayDeserializer, new ByteArrayDeserializer)
    try
      c.subscribe(topics.asJava)
      val out      = mutable.ArrayBuffer.empty[ConsumerRecord[Array[Byte], Array[Byte]]]
      val deadline = within.fromNow
      while out.size < atLeast && deadline.hasTimeLeft() do
        out ++= c.poll(JDuration.ofMillis(500)).asScala
      out.toVector
    finally c.close()

  private def flows = client.resources(classOf[AnkkaFlow]).inNamespace(Namespace)

  private def status: Option[AnkkaFlowStatus] =
    Option(flows.withName("cart").get()).flatMap(r => Option(r.getStatus))

  private def events(reason: String): Vector[String] =
    client
      .resources(classOf[io.fabric8.kubernetes.api.model.events.v1.Event])
      .inNamespace(Namespace)
      .list()
      .getItems
      .asScala
      .toVector
      .filter(e => e.getReason == reason && e.getRegarding.getKind == "AnkkaFlow")
      .map(_.getNote)

  private val mapper = FlowSerialization.mapper()

  private def streamlet(name: String, threshold: Int, replicas: Int, outletPrefix: String) =
    StreamletSpec(
      name = name,
      image = sampleImage,
      replicas = replicas,
      config = Map("review-threshold" -> mapper.readTree(threshold.toString)),
      inlets = Map("in" -> "cart-events"),
      outlets = Map("valid" -> s"$outletPrefix-valid", "review" -> s"$outletPrefix-review"),
      descriptor = Fixtures.descriptor("cart-router")
    )

  private def spec(
      routerThreshold: Int = 100,
      routerReplicas: Int = 1,
      auditReplicas: Int = 1
  ): AnkkaFlowSpec =
    AnkkaFlowSpec(
      pipeline = "cart",
      version = "test",
      protocolVersion = "1.0",
      streamlets = List(
        streamlet("router", routerThreshold, routerReplicas, "router"),
        streamlet("audit", 1000, auditReplicas, "audit")
      ),
      topics = List(
        TopicSpec(
          id = "cart-events",
          name = "shop.cart-events.v1",
          managed = false,
          bootstrapServers = Some("kafka.kafka.svc:9092")
        ),
        TopicSpec(id = "router-valid", name = "cart.valid-carts", partitions = Some(6)),
        TopicSpec(id = "router-review", name = "cart.review-carts"),
        TopicSpec(id = "audit-valid", name = "cart.audit-valid"),
        TopicSpec(id = "audit-review", name = "cart.audit-review")
      )
    )

  private def apply(s: AnkkaFlowSpec): Unit =
    val r = AnkkaFlow(Namespace, "cart", s)
    client.resource(r).fieldManager("test").forceConflicts().serverSideApply(): Unit

  private def readyPods(streamlet: String): Int =
    client.pods
      .inNamespace(Namespace)
      .withLabel(Labels.StreamletKey, streamlet)
      .list()
      .getItems
      .asScala
      .count { p =>
        Option(p.getStatus.getConditions)
          .exists(_.asScala.exists(c => c.getType == "Ready" && c.getStatus == "True")) &&
        Option(p.getMetadata.getDeletionTimestamp).isEmpty
      }

  private def kafkaManifest(externalPort: Int): String =
    s"""apiVersion: v1
       |kind: Namespace
       |metadata: { name: kafka }
       |---
       |apiVersion: v1
       |kind: Service
       |metadata: { name: kafka, namespace: kafka }
       |spec:
       |  selector: { app: kafka }
       |  ports: [ { name: internal, port: 9092 } ]
       |---
       |apiVersion: v1
       |kind: Service
       |metadata: { name: kafka-external, namespace: kafka }
       |spec:
       |  type: NodePort
       |  selector: { app: kafka }
       |  ports: [ { name: external, port: 9094, targetPort: 9094, nodePort: $NodePort } ]
       |---
       |apiVersion: apps/v1
       |kind: StatefulSet
       |metadata: { name: kafka, namespace: kafka }
       |spec:
       |  serviceName: kafka
       |  replicas: 1
       |  selector: { matchLabels: { app: kafka } }
       |  template:
       |    metadata: { labels: { app: kafka } }
       |    spec:
       |      containers:
       |        - name: kafka
       |          image: $KafkaImage
       |          imagePullPolicy: IfNotPresent
       |          env:
       |            - { name: KAFKA_NODE_ID, value: "1" }
       |            - { name: KAFKA_PROCESS_ROLES, value: "broker,controller" }
       |            - { name: KAFKA_LISTENERS, value: "INTERNAL://:9092,CONTROLLER://:9093,EXTERNAL://:9094" }
       |            - { name: KAFKA_ADVERTISED_LISTENERS, value: "INTERNAL://kafka.kafka.svc:9092,EXTERNAL://localhost:$externalPort" }
       |            - { name: KAFKA_LISTENER_SECURITY_PROTOCOL_MAP, value: "CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT" }
       |            - { name: KAFKA_CONTROLLER_LISTENER_NAMES, value: "CONTROLLER" }
       |            - { name: KAFKA_CONTROLLER_QUORUM_VOTERS, value: "1@localhost:9093" }
       |            - { name: KAFKA_INTER_BROKER_LISTENER_NAME, value: "INTERNAL" }
       |            - { name: KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR, value: "1" }
       |            - { name: KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR, value: "1" }
       |            - { name: KAFKA_TRANSACTION_STATE_LOG_MIN_ISR, value: "1" }
       |            - { name: KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS, value: "0" }
       |            - { name: KAFKA_AUTO_CREATE_TOPICS_ENABLE, value: "false" }
       |          readinessProbe: { tcpSocket: { port: 9092 }, periodSeconds: 3 }
       |""".stripMargin

  private def load(yaml: String): Unit =
    client.load(new ByteArrayInputStream(yaml.getBytes("UTF-8"))).serverSideApply(): Unit

  override def beforeAll(): Unit =
    if !munitIgnore then
      k3s = new K3sContainer(DockerImageName.parse(K3sImage))
      k3s.addExposedPort(NodePort)
      k3s.start()
      external = s"localhost:${k3s.getMappedPort(NodePort)}"
      Seq(KafkaImage, operatorImage, sidecarImage, sampleImage).foreach(
        ClusterImages.importInto(k3s, _)
      )
      client = new KubernetesClientBuilder()
        .withConfig(Config.fromKubeconfig(k3s.getKubeConfigYaml))
        .withKubernetesSerialization(FlowSerialization())
        .build()
      client
        .load(getClass.getResourceAsStream(AnkkaFlowDefinition.manifestResource))
        .serverSideApply()
      eventually(1.minute, "the CRD to be established") {
        val crd = client.apiextensions.v1.customResourceDefinitions
          .withName(AnkkaFlowDefinition.crdName)
          .get()
        assert(
          crd.getStatus.getConditions.asScala.exists(c =>
            c.getType == "Established" && c.getStatus == "True"
          )
        )
      }
      load(kafkaManifest(k3s.getMappedPort(NodePort)))
      val operatorYaml = scala.io.Source
        .fromInputStream(getClass.getResourceAsStream("/ankka-flow/install/operator.yaml"))
        .mkString
        .replace("ankka-flow-operator:latest", operatorImage)
        .replace("ankka-flow-sidecar:latest", sidecarImage)
      load(operatorYaml)
      load(
        """apiVersion: v1
          |kind: Secret
          |metadata: { name: kafka-cluster-default, namespace: ankka-flow }
          |stringData: { bootstrap.servers: "kafka.kafka.svc:9092", partitions: "3", replicas: "1" }
          |""".stripMargin
      )
      load(s"apiVersion: v1\nkind: Namespace\nmetadata: { name: $Namespace }\n")
      eventually(4.minutes, "Kafka to be reachable from outside")(
        assert(Try(admin(_.listTopics().names().get())).isSuccess)
      )
      eventually(3.minutes, "the operator to be ready") {
        assertEquals(
          Option(
            client.apps.deployments
              .inNamespace("ankka-flow")
              .withName("ankka-flow-operator")
              .get()
              .getStatus
              .getReadyReplicas
          ).map(_.intValue),
          Some(1)
        )
      }

  override def afterAll(): Unit =
    Option(client).foreach(_.close())
    Option(k3s).foreach(_.stop())

  // ── US3 ──────────────────────────────────────────────────────────────────────────────────

  test(
    "S3.1-S3.3: managed topics created, the unmanaged one untouched, two-container pods, the pipeline Ready, records flowing"
  ) {
    admin(
      _.createTopics(java.util.List.of(new NewTopic("shop.cart-events.v1", 3, 1.toShort)))
        .all()
        .get()
    )
    // A managed topic that already exists with other partitions is kept and reported.
    admin(
      _.createTopics(java.util.List.of(new NewTopic("cart.audit-review", 5, 1.toShort))).all().get()
    )
    apply(spec())

    eventually(2.minutes, "the managed topics") {
      assertEquals(partitionsOf("cart.valid-carts"), Some(6))
      assertEquals(partitionsOf("cart.review-carts"), Some(3), "sized by the default cluster")
    }
    assertEquals(partitionsOf("shop.cart-events.v1"), Some(3), "the unmanaged topic is as it was")
    assertEquals(
      partitionsOf("cart.audit-review"),
      Some(5),
      "an existing managed topic is left as it is"
    )
    eventually(1.minute, "the TopicDiffers event")(
      assert(events("TopicDiffers").exists(_.contains("cart.audit-review")))
    )

    eventually(6.minutes, s"the pipeline to be Ready (status: $status)")(
      assertEquals(status.map(_.phase), Some(AnkkaFlowStatus.Ready))
    )
    val pod = client.pods
      .inNamespace(Namespace)
      .withLabel(Labels.StreamletKey, "router")
      .list()
      .getItems
      .asScala
      .head
    assertEquals(
      pod.getSpec.getContainers.asScala.map(_.getName).toList,
      List("sidecar", "process")
    )
    val process = pod.getSpec.getContainers.asScala.find(_.getName == "process").get
    assert(
      process.getPorts.isEmpty && process.getReadinessProbe == null && process.getVolumeMounts.isEmpty
    )
    assert(events("TopicCreated").nonEmpty)

    produce(
      "shop.cart-events.v1",
      (0 until 50).map(i => s"cart-${i % 10}" -> s"""{"id":$i,"total":${(i * 37) % 200}}""")
    )
    val routed = consume(Seq("cart.valid-carts", "cart.review-carts"), 50)
    val ids = routed
      .map(r => """"id":(\d+)""".r.findFirstMatchIn(new String(r.value)).get.group(1).toInt)
      .toSet
    assertEquals(ids, (0 until 50).toSet)
    routed.foreach { r =>
      val total = """"total":(\d+)""".r.findFirstMatchIn(new String(r.value)).get.group(1).toInt
      assertEquals(r.topic, if total > 100 then "cart.review-carts" else "cart.valid-carts")
    }
  }

  test("S3.5: three replicas share one consumer group") {
    apply(spec(routerReplicas = 3))
    eventually(5.minutes, "three ready router pods")(assertEquals(readyPods("router"), 3))
    eventually(2.minutes, "three members in cart.router.in") {
      val group = admin(
        _.describeConsumerGroups(java.util.List.of("cart.router.in"))
          .all()
          .get()
          .get("cart.router.in")
      )
      assertEquals(group.members.size, 3)
    }
  }

  test("S3.7: a changed parameter rolls only that streamlet") {
    def generation(name: String) = client.apps.deployments
      .inNamespace(Namespace)
      .withName(s"flow-cart-$name")
      .get()
      .getMetadata
      .getGeneration
      .longValue
    val routerBefore = generation("router")
    val auditBefore  = generation("audit")
    apply(spec(routerThreshold = 250, routerReplicas = 3))
    eventually(2.minutes, "the router's Deployment to change")(
      assert(generation("router") > routerBefore)
    )
    eventually(1.minute, "the StreamletRolled event")(
      assert(events("StreamletRolled").exists(_.contains("'router'")))
    )
    eventually(6.minutes, "the pipeline to be Ready again")(
      assertEquals(status.map(_.phase), Some(AnkkaFlowStatus.Ready))
    )
    assertEquals(generation("audit"), auditBefore, "the audit streamlet was not touched")
  }

  // ── US4 ──────────────────────────────────────────────────────────────────────────────────

  private def requestReset(id: String): Unit =
    flows
      .withName("cart")
      .edit((f: AnkkaFlow) =>
        ResetRequest.withRequest(f, ResetRequest.Request(id, List("router")))
      ): Unit

  private def done: Option[String] = ResetRequest.done(flows.withName("cart").get())

  private def committed(group: String): Long =
    admin(
      _.listConsumerGroupOffsets(group)
        .partitionsToOffsetAndMetadata()
        .get()
        .asScala
        .values
        .map(_.offset)
        .sum
    )

  test("S4.2: a reset requested while the router runs is refused and stays pending") {
    requestReset("r-1")
    eventually(1.minute, "the ResetRefused event")(
      assert(events("ResetRefused").exists(n => n.contains("r-1") && n.contains("router")))
    )
    assertNotEquals(done, Some("r-1"))
  }

  test(
    "S4.1: scaled to zero, the pending reset runs: one event per group, the done marker, offsets at the start"
  ) {
    assertEquals(committed("cart.router.in"), 50L)
    apply(spec(routerThreshold = 250, routerReplicas = 0))
    eventually(3.minutes, "the reset to be carried out")(assertEquals(done, Some("r-1")))
    assert(
      events("ResetOffsets").exists(_.startsWith("cart.router.in:")),
      events("ResetOffsets").toString
    )
    assert(
      !events("ResetOffsets").exists(_.startsWith("cart.audit.in:")),
      "only the named streamlet"
    )
    assertEquals(committed("cart.router.in"), 0L)
  }

  test("S4.1: scaled back up, every input is delivered again (SC-004)") {
    apply(spec(routerThreshold = 250, routerReplicas = 1))
    eventually(4.minutes, "the router to reread its input")(
      assertEquals(committed("cart.router.in"), 50L)
    )
    val routed = consume(Seq("cart.valid-carts", "cart.review-carts"), 100)
    val ids =
      routed.map(r => """"id":(\d+)""".r.findFirstMatchIn(new String(r.value)).get.group(1).toInt)
    assertEquals(
      ids.groupBy(identity).values.map(_.size).min,
      2,
      "every event twice: once before the reset, once after"
    )
  }

  test("S4.3: an operator restart does not repeat a reset") {
    val before = events("ResetOffsets").size
    client.pods
      .inNamespace("ankka-flow")
      .withLabel("app.kubernetes.io/name", "ankka-flow-operator")
      .delete()
    eventually(3.minutes, "a new operator pod") {
      assertEquals(
        Option(
          client.apps.deployments
            .inNamespace("ankka-flow")
            .withName("ankka-flow-operator")
            .get()
            .getStatus
            .getReadyReplicas
        ).map(_.intValue),
        Some(1)
      )
    }
    Thread.sleep(15000)
    assertEquals(done, Some("r-1"))
    assertEquals(events("ResetOffsets").size, before)
    assertEquals(committed("cart.router.in"), 50L)
  }

  test("S4.4: the router's lag is exported under its client id") {
    val pod = eventually(3.minutes, "a ready router pod") {
      client.pods
        .inNamespace(Namespace)
        .withLabel(Labels.StreamletKey, "router")
        .list()
        .getItems
        .asScala
        .find(p =>
          Option(p.getStatus.getConditions)
            .exists(_.asScala.exists(c => c.getType == "Ready" && c.getStatus == "True"))
        )
        .get
    }
    val forward =
      client.pods.inNamespace(Namespace).withName(pod.getMetadata.getName).portForward(2050)
    try
      val metrics = eventually(1.minute, "the metrics endpoint") {
        val body =
          scala.io.Source.fromURL(s"http://localhost:${forward.getLocalPort}/metrics").mkString
        assert(body.contains("""client_id="cart.router.in""""), body.take(500))
        body
      }
      assert(
        metrics.contains("kafka_consumer_consumer_fetch_manager_metrics_records_lag{"),
        "no lag series"
      )
      assert(metrics.contains("ankka_flow_sidecar_stalled_seconds"), "no sidecar stall series")
    finally forward.close()
  }
