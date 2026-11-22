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
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.testcontainers.k3s.K3sContainer
import org.testcontainers.utility.DockerImageName

/**
 * The real thing on k3s: the operator and a Kafka running in the cluster, the sample cleaner
 * applied between two managed topics, and the pipeline's promises checked from outside through
 * Kafka's external listener. Skipped with `-Dflow.cluster.tests=off`.
 */
class FlowClusterSuite extends munit.FunSuite:

  override def munitIgnore: Boolean         = sys.props.get("flow.cluster.tests").contains("off")
  override val munitTimeout: FiniteDuration = 15.minutes

  private val K3sImage      = "rancher/k3s:v1.35.1-k3s1"
  private val KafkaImage    = sys.props.getOrElse("flow.kafka.image", "apache/kafka:3.9.1")
  private val tag           = sys.props.getOrElse("flow.image.tag", "latest")
  private val operatorImage = s"ankka-flow-operator:$tag"
  private val cleanerImage  = s"sample-cart-cleaner:$tag"
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

  private val Events = "cart.cart-events"
  private val Clean  = "cart.clean-carts"
  private val Group  = "cart.cleaner.in"

  private def spec(maxTotal: Int = 150, replicas: Int = 1): AnkkaFlowSpec =
    AnkkaFlowSpec(
      pipeline = "cart",
      version = "test",
      descriptorVersion = "2.0",
      streamlets = List(
        StreamletSpec(
          name = "cleaner",
          image = cleanerImage,
          engine = "pekko-streams",
          replicas = replicas,
          config = Map("max-total" -> mapper.readTree(maxTotal.toString)),
          inlets = Map("in" -> "cart-events"),
          outlets = Map("out" -> "clean-carts"),
          descriptor = Fixtures.descriptor("cart-cleaner")
        )
      ),
      topics = List(
        TopicSpec(id = "cart-events", name = Events, partitions = Some(3)),
        TopicSpec(id = "clean-carts", name = Clean, partitions = Some(3))
      )
    )

  /** Fifty cart events, every tenth malformed; totals 0 to 196. */
  private val inputs: Vector[(String, String)] = (0 until 50).toVector.map { i =>
    val cart = s"cart-${i % 10}"
    cart -> (if i % 10 == 9 then s"not an event $i"
             else s"""{"cartId":"$cart","seq":${i + 1},"total":${(i * 37) % 200},"at":$i}""")
  }

  /** The seq of every event the cleaner keeps under a maximum. */
  private def kept(max: Int): Set[Int] =
    (0 until 50).filter(i => i % 10 != 9 && (i * 37) % 200 <= max).map(_ + 1).toSet

  private def seqs(records: Vector[ConsumerRecord[Array[Byte], Array[Byte]]]): Vector[Int] =
    records.map(r => """"seq":(\d+)""".r.findFirstMatchIn(new String(r.value)).get.group(1).toInt)

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
      Seq(KafkaImage, operatorImage, cleanerImage).foreach(
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

  // ── A Pekko Streams streamlet on a cluster ───────────────────────────────────────────────

  test(
    "a Pekko Streams streamlet reads its inlet and writes its outlet: Ready, one container, cleaned"
  ) {
    apply(spec())
    eventually(2.minutes, "the managed topics") {
      assertEquals(partitionsOf(Events), Some(3))
      assertEquals(partitionsOf(Clean), Some(3))
    }
    eventually(6.minutes, s"the pipeline to be Ready (status: $status)")(
      assertEquals(status.map(_.phase), Some(AnkkaFlowStatus.Ready))
    )
    val pod = client.pods
      .inNamespace(Namespace)
      .withLabel(Labels.StreamletKey, "cleaner")
      .list()
      .getItems
      .asScala
      .head
    assertEquals(pod.getSpec.getContainers.asScala.map(_.getName).toList, List("streamlet"))
    assertEquals(
      client.secrets
        .inNamespace(Namespace)
        .withName("flow-cart-cleaner")
        .get()
        .getData
        .keySet
        .asScala
        .toSet,
      Set("streamlet.properties")
    )

    produce(Events, inputs)
    val cleaned = consume(Seq(Clean), kept(150).size)
    assertEquals(seqs(cleaned).toSet, kept(150))
    cleaned.foreach { r =>
      val cart = """"cartId":"([^"]+)"""".r.findFirstMatchIn(new String(r.value)).get.group(1)
      assertEquals(new String(r.key), cart, "keyed by its cart")
    }
  }

  test("three replicas share one consumer group") {
    apply(spec(replicas = 3))
    eventually(5.minutes, "three ready cleaner pods")(assertEquals(readyPods("cleaner"), 3))
    eventually(2.minutes, s"three members in $Group") {
      val group = admin(
        _.describeConsumerGroups(java.util.List.of(Group)).all().get().get(Group)
      )
      assertEquals(group.members.size, 3)
    }
  }

  test("a changed parameter rolls that streamlet") {
    def generation = client.apps.deployments
      .inNamespace(Namespace)
      .withName("flow-cart-cleaner")
      .get()
      .getMetadata
      .getGeneration
      .longValue
    val before = generation
    apply(spec(maxTotal = 250, replicas = 3))
    eventually(2.minutes, "the cleaner's Deployment to change")(assert(generation > before))
    eventually(1.minute, "the StreamletRolled event")(
      assert(events("StreamletRolled").exists(_.contains("'cleaner'")))
    )
    eventually(6.minutes, "the pipeline to be Ready again")(
      assertEquals(status.map(_.phase), Some(AnkkaFlowStatus.Ready))
    )
    val props = new String(
      java.util.Base64.getDecoder.decode(
        client.secrets
          .inNamespace(Namespace)
          .withName("flow-cart-cleaner")
          .get()
          .getData
          .get("streamlet.properties")
      )
    )
    assert(props.contains("parameter.max-total=250\n"), props)
  }

  // ── Reset ────────────────────────────────────────────────────────────────────────────────

  private def requestReset(id: String): Unit =
    flows
      .withName("cart")
      .edit((f: AnkkaFlow) =>
        ResetRequest.withRequest(f, ResetRequest.Request(id, List("cleaner")))
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

  test("a reset requested while the cleaner runs is refused and stays pending") {
    requestReset("r-1")
    eventually(1.minute, "the ResetRefused event")(
      assert(events("ResetRefused").exists(n => n.contains("r-1") && n.contains("cleaner")))
    )
    assertNotEquals(done, Some("r-1"))
  }

  test("a Pekko Streams streamlet is reset to read its topics again from the start") {
    // Stopped, it drains: every record it read is committed, the last of each partition too.
    apply(spec(maxTotal = 250, replicas = 0))
    eventually(3.minutes, "the reset to be carried out")(assertEquals(done, Some("r-1")))
    assert(events("ResetOffsets").exists(_.startsWith(s"$Group:")), events("ResetOffsets").toString)
    assertEquals(committed(Group), 0L)

    apply(spec(maxTotal = 250, replicas = 1))
    // Every event under the old maximum before the reset, and every one under the new after it.
    // At least once: a record whose partition moved in a rebalance may be there more often.
    eventually(4.minutes, "every event read again") {
      val counts = seqs(consume(Seq(Clean), kept(150).size + kept(250).size, 20.seconds))
        .groupBy(identity)
        .view
        .mapValues(_.size)
        .toMap
      assertEquals(counts.keySet, kept(250))
      kept(250).foreach { n =>
        val least = if kept(150).contains(n) then 2 else 1
        assert(counts(n) >= least, s"seq $n is there ${counts(n)} times, fewer than $least")
      }
    }
  }

  test("an operator restart does not repeat a reset") {
    val before   = events("ResetOffsets").size
    val position = committed(Group)
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
    assert(committed(Group) >= position, s"${committed(Group)} after $position")
  }

  // ── Version one ──────────────────────────────────────────────────────────────────────────

  test("a version one resource is refused, saying a descriptor must name an engine") {
    load(
      s"""apiVersion: ${AnkkaFlowDefinition.apiVersion}
         |kind: AnkkaFlow
         |metadata: { name: legacy, namespace: $Namespace }
         |spec:
         |  pipeline: legacy
         |  version: "0.5.0"
         |  streamlets:
         |    - name: router
         |      image: sample-cart-router:0.5.0
         |      replicas: 1
         |      inlets: { in: legacy-in }
         |      descriptor:
         |        name: cart-router
         |        description: Routes cart events to the valid or review outlet.
         |        inlets:
         |          - name: in
         |            contract: { format: json, schema_name: cart-events.v1 }
         |        outlets: []
         |        config_parameters: []
         |  topics:
         |    - { id: legacy-in, name: legacy.in }
         |""".stripMargin
    )
    def legacy = Option(flows.withName("legacy").get()).flatMap(r => Option(r.getStatus))
    eventually(2.minutes, s"the legacy pipeline to fail (status: $legacy)")(
      assertEquals(legacy.map(_.phase), Some(AnkkaFlowStatus.Failed))
    )
    assert(
      events("Refused").exists(
        _.contains(com.thinkmorestupidless.ankka.flow.descriptor.DescriptorJson.NoEngine)
      ),
      events("Refused").toString
    )
    assert(
      client.apps.deployments.inNamespace(Namespace).withName("flow-legacy-router").get() == null
    )
  }
