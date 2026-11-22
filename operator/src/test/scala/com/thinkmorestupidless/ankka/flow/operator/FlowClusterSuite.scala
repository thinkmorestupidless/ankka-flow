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
 * The real thing on k3s: the operator and a Kafka running in the cluster, the sample cleaner
 * applied between two managed topics, and the pipeline's promises checked from outside through
 * Kafka's external listener. Then the sample counter as a Flink streamlet, with the Flink
 * Kubernetes Operator and MinIO as its state store. Skipped with `-Dflow.cluster.tests=off`; the
 * Flink cases alone with `-Dflow.flink.tests=off`.
 */
class FlowClusterSuite extends munit.FunSuite:

  override def munitIgnore: Boolean         = sys.props.get("flow.cluster.tests").contains("off")
  override val munitTimeout: FiniteDuration = 15.minutes

  private val K3sImage      = "rancher/k3s:v1.35.1-k3s1"
  private val KafkaImage    = sys.props.getOrElse("flow.kafka.image", "apache/kafka:3.9.1")
  private val tag           = sys.props.getOrElse("flow.image.tag", "latest")
  private val operatorImage = s"ankka-flow-operator:$tag"
  private val cleanerImage  = s"sample-cart-cleaner:$tag"
  private val counterImage  = s"sample-cart-counter:$tag"
  private val flinkOff      = sys.props.get("flow.flink.tests").contains("off")
  private val NodePort      = 30094
  private val AclNodePort   = 30095
  private val Namespace     = "shop"

  private var k3s: K3sContainer        = scala.compiletime.uninitialized
  private var client: KubernetesClient = scala.compiletime.uninitialized
  private var external: String         = ""

  // ── helpers ──────────────────────────────────────────────────────────────────────────────

  def eventually[A](within: FiniteDuration, what: => String)(check: => A): A =
    val deadline        = within.fromNow
    var last: Throwable = null
    while deadline.hasTimeLeft() do
      try return check
      catch
        case e: (AssertionError | Exception) =>
          last = e
          Thread.sleep(2000)
    // What the operators were doing meanwhile: the first thing to read when a wait times out.
    println(s"[operator log, waiting for $what]\n${operatorLog(200)}")
    println(s"[flink operator log]\n${podLog("flink-operator", "flink-kubernetes-operator", 120)}")
    println(
      s"[JobException events in $Namespace]\n" + Try(
        client.v1.events
          .inNamespace(Namespace)
          .list()
          .getItems
          .asScala
          .filter(_.getReason == "JobException")
          .map(e =>
            s"${e.getInvolvedObject.getName} uid ${e.getInvolvedObject.getUid} count ${e.getCount} " +
              s"first ${e.getFirstTimestamp} last ${e.getLastTimestamp} eventTime ${Option(e.getEventTime).map(_.getTime)} " +
              s"series ${Option(e.getSeries)
                  .map(sr => s"${sr.getCount}@${Option(sr.getLastObservedTime).map(_.getTime)}")}: " +
              Option(e.getMessage).getOrElse("").take(120)
          )
          .mkString("\n")
      ).getOrElse("(none)")
    )
    println(
      s"[FlinkDeployments in $Namespace]\n" + Try(
        client
          .genericKubernetesResources(Fabric8Executor.FlinkDeploymentContext)
          .inNamespace(Namespace)
          .list()
          .getItems
          .asScala
          .map(d =>
            s"${d.getMetadata.getName} generation ${d.getMetadata.getGeneration}: " + mapper
              .writeValueAsString(d.getAdditionalProperties)
          )
          .mkString("\n")
      ).getOrElse("(none)")
    )
    throw new AssertionError(
      s"timed out after $within waiting for $what: ${Option(last).map(_.getMessage).getOrElse("")}",
      last
    )

  private def operatorLog(lines: Int): String = podLog("ankka-flow", "ankka-flow-operator", lines)

  private def podLog(namespace: String, deployment: String, lines: Int): String =
    Try(
      client.apps.deployments
        .inNamespace(namespace)
        .withName(deployment)
        .tailingLines(lines)
        .getLog()
    ).fold(e => s"(no log: ${e.getMessage})", identity)

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

  private def events(reason: String, of: String = ""): Vector[String] =
    client
      .resources(classOf[io.fabric8.kubernetes.api.model.events.v1.Event])
      .inNamespace(Namespace)
      .list()
      .getItems
      .asScala
      .toVector
      .filter(e =>
        e.getReason == reason && e.getRegarding.getKind == "AnkkaFlow" &&
          (of.isEmpty || e.getRegarding.getName == of)
      )
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
      k3s.addExposedPort(AclNodePort)
      k3s.start()
      external = s"localhost:${k3s.getMappedPort(NodePort)}"
      ClusterImages.importInto(k3s, KafkaImage)
      ClusterImages.importInto(k3s, operatorImage, cleanerImage)
      if !flinkOff then
        // The counter and its two variants share every layer but one: one archive.
        ClusterImages.importInto(
          k3s,
          counterImage,
          s"$counterImage-added",
          s"$counterImage-retyped"
        )
        ClusterImages.importInto(k3s, sys.props("flow.flink.operator.image"))
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

  // ── A Flink streamlet on a cluster ───────────────────────────────────────────────────────
  //
  // The sample counter on its own pipeline `counts`, between two managed topics. The cases run in
  // order: refused for want of the Flink operator, then of the state store; then running, killed,
  // upgraded twice and reset. Every window is a minute of the events' own time, from `base`.

  private def flinkTest(name: String)(body: => Any)(using munit.Location): Unit =
    test(if flinkOff then name.ignore else new munit.TestOptions(name))(body)

  private val CountsIn             = "counts.clean-carts"
  private val CountsOut            = "counts.cart-counts"
  private val base                 = (System.currentTimeMillis / 60000) * 60000 - 20 * 60000
  private def window(n: Int): Long = base + n * 60000L

  private def countsSpec(image: String = counterImage, replicas: Int = 1): AnkkaFlowSpec =
    AnkkaFlowSpec(
      pipeline = "counts",
      version = "test",
      descriptorVersion = "2.0",
      streamlets = List(
        StreamletSpec(
          name = "counter",
          image = image,
          engine = "flink",
          replicas = replicas,
          parallelism = Some(1),
          flink = Some(
            FlinkSpec(
              entryClass = "cart.counter.CartCounter",
              version = "2.2",
              checkpointInterval = "5s",
              jobManager = Resources("0.5", "1024m"),
              taskManager = Resources("0.5", "1024m")
            )
          ),
          config = Map("window" -> mapper.readTree("\"1m\"")),
          inlets = Map("in" -> "clean-carts"),
          outlets = Map("out" -> "cart-counts"),
          descriptor = Fixtures.descriptor("cart-counter")
        )
      ),
      topics = List(
        TopicSpec(id = "clean-carts", name = CountsIn, partitions = Some(3)),
        TopicSpec(id = "cart-counts", name = CountsOut, partitions = Some(3))
      )
    )

  private def applyCounts(s: AnkkaFlowSpec): Unit =
    client
      .resource(AnkkaFlow(Namespace, "counts", s))
      .fieldManager("test")
      .forceConflicts()
      .serverSideApply(): Unit

  private def counts                                = Option(flows.withName("counts").get())
  private def countsStatus: Option[AnkkaFlowStatus] = counts.flatMap(r => Option(r.getStatus))
  private def condition(kind: String): Option[Condition] =
    countsStatus.flatMap(_.conditions.find(c => c.`type` == kind && c.status == Condition.True))

  /** A change the operator reconciles at once, as an edit by a person would be. */
  private def touch(): Unit =
    flows.withName("counts").edit { (f: AnkkaFlow) =>
      f.getMetadata.getAnnotations.put("test/touched", System.nanoTime.toString); f
    }: Unit

  private def flinkDeployment: Option[io.fabric8.kubernetes.api.model.GenericKubernetesResource] =
    Option(
      client
        .genericKubernetesResources(Fabric8Executor.FlinkDeploymentContext)
        .inNamespace(Namespace)
        .withName("flow-counts-counter")
        .get()
    )
  private def job: Option[FlinkState] = flinkDeployment.map(Fabric8Executor.flinkState(_))
  private def jobStatusJson: String =
    flinkDeployment
      .flatMap(d => Option(d.getAdditionalProperties.get("status")))
      .map(mapper.writeValueAsString)
      .getOrElse("no FlinkDeployment")

  private def readyAt(image: String): Unit =
    eventually(8.minutes, s"counts to be Ready at $image (status: $countsStatus; job: $job)") {
      assertEquals(countsStatus.map(_.phase), Some(AnkkaFlowStatus.Ready))
      assertEquals(job.map(_.image), Some(image))
      assertEquals(job.flatMap(_.jobState), Some("RUNNING"))
    }

  /** A clean cart event at `at`; the cleaner's output shape the counter reads. */
  private def cartEvent(cart: String, seq: Int, at: Long): (String, String) =
    cart -> s"""{"cartId":"$cart","seq":$seq,"total":1,"at":$at}"""

  /** `n` events of one cart in window `w`, a second apart, in an order that is not time order. */
  private def eventsIn(cart: String, w: Int, n: Int): Seq[(String, String)] =
    scala.util
      .Random(w * 31 + cart.hashCode)
      .shuffle((0 until n).toVector)
      .map(i => cartEvent(cart, i, window(w) + 1000L * i))

  /** One event at the start of window `w` on every partition, so the watermark passes `w - 1`. */
  private def tick(w: Int): Unit =
    val p = new Properties()
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, external)
    val producer = new KafkaProducer(p, new ByteArraySerializer, new ByteArraySerializer)
    try
      (0 until 3).foreach { partition =>
        val (k, v) = cartEvent(s"tick-$partition", 0, window(w))
        producer.send(new ProducerRecord(CountsIn, partition, k.getBytes, v.getBytes)).get(): Unit
      }
    finally producer.close()

  /** Every count written so far, as Kafka shows it to a `read_committed` reader. */
  private def committedCounts(
      atLeast: Int,
      within: FiniteDuration,
      topic: String = CountsOut
  ): Vector[(String, Long, Long)] =
    val p = new Properties()
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, external)
    p.put(ConsumerConfig.GROUP_ID_CONFIG, s"verify-${UUID.randomUUID()}")
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
    val c = new KafkaConsumer(p, new ByteArrayDeserializer, new ByteArrayDeserializer)
    try
      c.subscribe(java.util.List.of(topic))
      val out      = mutable.ArrayBuffer.empty[(String, Long, Long)]
      val deadline = within.fromNow
      while (out.size < atLeast || out.isEmpty) && deadline.hasTimeLeft() do
        c.poll(JDuration.ofMillis(500)).asScala.foreach { r =>
          val n = mapper.readTree(r.value)
          out += ((n.get("cartId").asText, n.get("windowStart").asLong, n.get("count").asLong))
        }
      out.toVector
    finally c.close()

  private def countOf(cart: String, w: Int, within: FiniteDuration = 3.minutes): Long =
    eventually(within, s"the count of $cart in window $w") {
      val found = committedCounts(1, 20.seconds).filter(c => c._1 == cart && c._2 == window(w))
      assert(found.nonEmpty, s"no count of $cart in window $w yet")
      found.map(_._3).last
    }

  private def counterPods: Vector[io.fabric8.kubernetes.api.model.Pod] =
    client.pods
      .inNamespace(Namespace)
      .withLabel(Labels.PipelineKey, "counts")
      .list()
      .getItems
      .asScala
      .toVector
      .filter(p => Option(p.getMetadata.getDeletionTimestamp).isEmpty)

  /** The objects under one prefix of the state store, listed by MinIO's client in a Job. */
  private def stateObjects(prefix: String): Vector[String] =
    val name = s"list-${UUID.randomUUID().toString.take(8)}"
    load(
      s"""apiVersion: batch/v1
         |kind: Job
         |metadata: { name: $name, namespace: minio }
         |spec:
         |  backoffLimit: 3
         |  template:
         |    spec:
         |      restartPolicy: Never
         |      containers:
         |        - name: mc
         |          image: $minioClientImage
         |          imagePullPolicy: IfNotPresent
         |          env: [ { name: MC_HOST_s, value: "http://flow:flow-state-password@minio.minio.svc:9000" } ]
         |          args: [ "ls", "--recursive", "s/flow-state/$prefix" ]
         |""".stripMargin
    )
    eventually(3.minutes, s"the listing of $prefix") {
      val job = client.batch.v1.jobs.inNamespace("minio").withName(name).get()
      assertEquals(Option(job.getStatus.getSucceeded).map(_.intValue), Some(1))
    }
    val log = client.batch.v1.jobs.inNamespace("minio").withName(name).getLog
    log.linesIterator.map(_.trim).filter(_.nonEmpty).map(_.split("\\s+").last).toVector.sorted

  private lazy val minioYaml: String =
    java.nio.file.Files.readString(
      java.nio.file.Paths
        .get(sys.props.getOrElse("flow.repo.root", "."))
        .resolve("kustomization/components/minio/minio.yaml")
    )
  private lazy val minioClientImage: String =
    """image: (\S*minio-client\S*)""".r.findFirstMatchIn(minioYaml).get.group(1)

  private val StateDir = "s3://flow-state/suite"

  private def restartOperator(): Unit =
    client.pods
      .inNamespace("ankka-flow")
      .withLabel("app.kubernetes.io/name", "ankka-flow-operator")
      .delete(): Unit
    eventually(3.minutes, "a new operator pod") {
      val pods = client.pods
        .inNamespace("ankka-flow")
        .withLabel("app.kubernetes.io/name", "ankka-flow-operator")
        .list()
        .getItems
        .asScala
      assert(
        pods.exists(p =>
          Option(p.getMetadata.getDeletionTimestamp).isEmpty &&
            p.getStatus.getConditions.asScala.exists(c =>
              c.getType == "Ready" && c.getStatus == "True"
            )
        )
      )
    }

  flinkTest(
    "a pipeline with a Flink streamlet is refused, naming the Flink operator, where it is not installed"
  ) {
    val started = System.nanoTime
    applyCounts(countsSpec())
    eventually(1.minute, s"FlinkOperatorMissing (status: $countsStatus)") {
      assertEquals(countsStatus.map(_.phase), Some(AnkkaFlowStatus.Failed))
      assert(condition(Condition.FlinkOperatorMissing).isDefined, countsStatus.toString)
      assert(
        events(Condition.FlinkOperatorMissing, "counts").exists(
          _.contains("Flink Kubernetes Operator")
        )
      )
    }
    assert((System.nanoTime - started).nanos < 1.minute)
    assertEquals(flinkDeployment, None)
  }

  flinkTest("with the Flink operator but no flink-state, it is refused naming the Secret") {
    client
      .load(getClass.getResourceAsStream("/flink-operator.yaml"))
      .serverSideApply(): Unit
    eventually(5.minutes, "the Flink operator to be ready") {
      assertEquals(
        Option(
          client.apps.deployments
            .inNamespace("flink-operator")
            .withName("flink-kubernetes-operator")
            .get()
            .getStatus
            .getReadyReplicas
        ).map(_.intValue),
        Some(1)
      )
    }
    // The informer on FlinkDeployments starts with the operator when the CRD is there.
    restartOperator()
    val started = System.nanoTime
    touch()
    eventually(1.minute, s"StateStoreMissing (status: $countsStatus)") {
      assert(condition(Condition.StateStoreMissing).isDefined, countsStatus.toString)
      assertEquals(condition(Condition.FlinkOperatorMissing), None)
      assert(
        events(Condition.StateStoreMissing, "counts").exists(_.contains(StateStore.SecretName))
      )
    }
    assert((System.nanoTime - started).nanos < 1.minute)
  }

  flinkTest("a Flink streamlet reads its inlet and writes its outlet: a FlinkDeployment, Ready") {
    load(
      minioYaml.replaceAll("""image: (\S*/minio@\S*)""", s"image: ${sys.props("flow.minio.image")}")
    )
    eventually(4.minutes, "MinIO's bucket") {
      val job = client.batch.v1.jobs.inNamespace("minio").withName("minio-bucket").get()
      assertEquals(Option(job.getStatus.getSucceeded).map(_.intValue), Some(1))
    }
    load(
      s"""apiVersion: v1
         |kind: Secret
         |metadata: { name: flink-state, namespace: ankka-flow }
         |stringData:
         |  dir: $StateDir
         |  flink-config: |
         |    s3.endpoint=http://minio.minio.svc:9000
         |    s3.path-style-access=true
         |  env: |
         |    AWS_ACCESS_KEY_ID=flow
         |    AWS_SECRET_ACCESS_KEY=flow-state-password
         |    ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-presto-2.2.1.jar
         |""".stripMargin
    )
    touch()
    eventually(2.minutes, "the counts topics") {
      assertEquals(partitionsOf(CountsIn), Some(3))
      assertEquals(partitionsOf(CountsOut), Some(3))
    }
    readyAt(counterImage)
    // The job manager's Deployment is the Flink operator's; ours would carry our managed-by label.
    assert(
      Option(
        client.apps.deployments.inNamespace(Namespace).withName("flow-counts-counter").get()
      ).forall(d =>
        Option(d.getMetadata.getLabels).flatMap(_.asScala.get("app.kubernetes.io/managed-by"))
          != Some("ankka-flow")
      )
    )
    assertEquals(countsStatus.get.conditions.map(_.`type`), List(Condition.Ready))
  }

  flinkTest("a Flink streamlet groups records into windows by the time each record carries") {
    val carts = (0 until 4).map(j => s"c$j")
    produce(
      CountsIn,
      carts.flatMap(c => eventsIn(c, 0, 5)) ++ carts.zipWithIndex.flatMap((c, j) =>
        eventsIn(c, 1, j + 1)
      )
    )
    tick(3)
    carts.zipWithIndex.foreach { (c, j) =>
      assertEquals(countOf(c, 0), 5L, c)
      assertEquals(countOf(c, 1), (j + 1).toLong, c)
    }
  }

  flinkTest("a Flink streamlet that fails continues from its state and counts no record twice") {
    produce(CountsIn, eventsIn("k", 4, 10))
    Thread.sleep(15000) // two checkpoints hold the open window
    val before = counterPods.map(_.getMetadata.getUid).toSet
    assert(before.size >= 2, s"a job manager and a task manager: $before")
    counterPods.foreach(p =>
      client.pods.inNamespace(Namespace).resource(p).withGracePeriod(0).delete()
    )
    eventually(6.minutes, s"the job to run again on new pods (job: $job)") {
      val now = counterPods
      assert(
        now.size >= 2 && now.forall(p => !before.contains(p.getMetadata.getUid)),
        now.map(_.getMetadata.getName).toString
      )
      assertEquals(job.flatMap(_.jobState), Some("RUNNING"))
    }
    produce(CountsIn, eventsIn("k", 4, 20).filter(e => !eventsIn("k", 4, 10).contains(e)).take(10))
    tick(6)
    assertEquals(countOf("k", 4, 5.minutes), 20L)
    val all   = committedCounts(1, 20.seconds)
    val twice = all.groupBy(c => (c._1, c._2)).filter(_._2.size > 1)
    assert(twice.isEmpty, s"windows written more than once: $twice")
  }

  flinkTest("a Flink streamlet keeps its state when a new image of it is deployed") {
    produce(CountsIn, eventsIn("a", 7, 6))
    Thread.sleep(15000)
    applyCounts(countsSpec(image = s"$counterImage-added"))
    readyAt(s"$counterImage-added")
    produce(CountsIn, (6 until 10).map(i => cartEvent("a", i, window(7) + 1000L * i)))
    tick(9)
    assertEquals(countOf("a", 7, 5.minutes), 10L)
  }

  flinkTest(
    "a new image that cannot read the state leaves it failed with StateUnreadable, the state untouched"
  ) {
    produce(CountsIn, eventsIn("r", 10, 3))
    Thread.sleep(15000)
    val checkpoints = "suite/shop/counts/counter/1/checkpoints"
    applyCounts(countsSpec(image = s"$counterImage-retyped"))
    eventually(
      12.minutes,
      s"StateUnreadable (status: $countsStatus; FlinkDeployment: $jobStatusJson)"
    ) {
      assert(condition(Condition.StateUnreadable).isDefined)
    }
    // What V6 pins R17's texts to.
    println(s"[V6] the FlinkDeployment of an image that cannot read its state: $jobStatusJson")
    val at = stateObjects(checkpoints)
    assert(at.exists(_.contains("_metadata")), s"no completed checkpoint under $checkpoints: $at")
    Thread.sleep(30000)
    assertEquals(stateObjects(checkpoints), at)
    assert(
      countsStatus.get.streamlets.head.detail.contains("deploy the previous image"),
      countsStatus.toString
    )
  }

  flinkTest(
    "a reset Flink streamlet discards its state and reads its topics again from the start"
  ) {
    applyCounts(countsSpec(image = s"$counterImage-added"))
    readyAt(s"$counterImage-added")
    flows
      .withName("counts")
      .edit((f: AnkkaFlow) =>
        ResetRequest.withRequest(f, ResetRequest.Request("f-1", List("counter")))
      ): Unit
    eventually(1.minute, "the ResetRefused event")(
      assert(
        events("ResetRefused", "counts").exists(n => n.contains("f-1") && n.contains("counter"))
      )
    )
    assertNotEquals(counts.flatMap(ResetRequest.done), Some("f-1"))

    applyCounts(countsSpec(image = s"$counterImage-added", replicas = 0))
    eventually(5.minutes, s"the reset to be carried out (job: $job)")(
      assertEquals(counts.flatMap(ResetRequest.done), Some("f-1"))
    )
    assert(
      events("StateDiscarded", "counts").exists(_.contains("epoch 2")),
      events("StateDiscarded", "counts").toString
    )
    assertEquals(counts.map(Fabric8Executor.stateEpochs), Some(Map("counter" -> 2)))
    eventually(2.minutes, "the FlinkDeployment again, at epoch 2") {
      val spec = flinkDeployment.map(_.getAdditionalProperties.get("spec"))
      val dir =
        spec.flatMap(Fabric8Executor.at(_, "flinkConfiguration", "execution.checkpointing.dir"))
      assert(dir.exists(_.toString.contains("/counter/2/")), spec.toString)
    }

    applyCounts(countsSpec(image = s"$counterImage-added", replicas = 1))
    readyAt(s"$counterImage-added")
    eventually(5.minutes, "the first windows counted again") {
      val again = committedCounts(1, 20.seconds).filter(c => c._1 == "c0" && c._2 == window(0))
      assertEquals(again.map(_._3), Vector(5L, 5L))
    }
  }

  // ── The whole sample pipeline: a Pekko Streams streamlet feeding a Flink one ──────────────

  private val repoRoot = java.nio.file.Path.of(sys.props("flow.repo.root"))
  private val sample   = repoRoot.resolve("samples/cart-pipeline")
  private val flowBin  = repoRoot.resolve("cli/target/universal/stage/bin/flow")
  private val SampleIn = "shop.cart-events.v1"

  /** The staged `flow`, against the suite's cluster: its exit code and everything it printed. */
  private def flow(args: String*): (Int, String) =
    val kubeconfig = java.nio.file.Files.createTempFile("k3s", ".yaml")
    java.nio.file.Files.writeString(kubeconfig, k3s.getKubeConfigYaml)
    val out = new StringBuilder
    val log = scala.sys.process.ProcessLogger(l => out.append(l).append('\n'))
    val code = scala.sys.process
      .Process(flowBin.toString +: args, sample.toFile, "KUBECONFIG" -> kubeconfig.toString)
      .!(log)
    (code, out.toString)

  /** The sample's resource, generated by `flow` with the suite's images and small Flink pods. */
  private def generateSample(stopped: Boolean): String =
    val conf = java.nio.file.Files.createTempFile("suite", ".conf")
    java.nio.file.Files.writeString(
      conf,
      s"""flow.streamlets.cleaner { replicas = ${if stopped then 0 else 1} }
         |flow.streamlets.counter {
         |  replicas = ${if stopped then 0 else 1}
         |  flink {
         |    checkpoint-interval = 5s
         |    job-manager { cpu = 0.5, memory = 1024m }
         |    task-manager { cpu = 0.5, memory = 1024m }
         |  }
         |}
         |""".stripMargin
    )
    val out = java.nio.file.Files.createTempFile("carts", ".yaml")
    val (code, printed) = flow(
      "generate",
      "blueprint.conf",
      "--descriptors",
      "flow",
      "--image",
      s"cleaner=$cleanerImage",
      "--image",
      s"counter=$counterImage",
      "--conf",
      conf.toString,
      "--version",
      "suite",
      "-n",
      Namespace,
      "-o",
      out.toString
    )
    assertEquals(code, 0, printed)
    java.nio.file.Files.readString(out)

  private def carts                                = Option(flows.withName("carts").get())
  private def cartsStatus: Option[AnkkaFlowStatus] = carts.flatMap(r => Option(r.getStatus))
  private def inletsOf(streamlet: String): List[InletStatus] =
    cartsStatus.toList.flatMap(_.streamlets).filter(_.name == streamlet).flatMap(_.inlets)

  /** produce.sh's events: cart i % 10 at `start` + 6i seconds, every tenth not a cart event. */
  private def sampleEvents(start: Long): Vector[(String, String)] =
    (0 until 50).toVector.map { i =>
      val cart = s"cart-${i % 10}"
      cart -> (if i % 10 == 9 then s"not a cart event $i"
               else
                 s"""{"cartId":"$cart","seq":${i + 1},"total":${(i * 37) % 200},"at":${start + i * 6000L}}""")
    }

  /**
   * The cart windows verify.sh expects closed: minute w of an event, ending 10 s before the last.
   */
  private def closedWindows(start: Long): Set[(String, Long)] =
    val last = start + 49 * 6000L
    (0 until 50)
      .filter(_ % 10 != 9)
      .map(i => (s"cart-${i % 10}", start + (i * 6 / 60) * 60000L))
      .filter((_, w) => w + 60000L <= last - 10000L)
      .toSet

  flinkTest("the sample pipeline: a Pekko Streams streamlet feeds a Flink one over one topic") {
    // The earlier Flink cases' pipeline would hold the cluster's room for a second job.
    flows.withName("counts").delete(): Unit
    eventually(3.minutes, "the counts pipeline to be gone")(assert(counterPods.isEmpty))
    load(java.nio.file.Files.readString(sample.resolve("k8s/kafka-cluster-shop.yaml")))
    admin(
      _.createTopics(java.util.List.of(new NewTopic(SampleIn, 3, 1.toShort)))
        .all()
        .get()
    )

    val (verified, printed) = flow("verify", "blueprint.conf", "--descriptors", "flow")
    assertEquals(verified, 0, printed)
    assert(printed.contains("verified: 2 streamlets, 3 topics"), printed)
    val yaml = generateSample(stopped = false)
    assert(yaml.contains("engine: pekko-streams") && yaml.contains("engine: flink"), yaml)
    load(yaml)
    eventually(8.minutes, s"carts to be Ready (status: $cartsStatus)")(
      assertEquals(cartsStatus.map(_.phase), Some(AnkkaFlowStatus.Ready))
    )

    val start = (System.currentTimeMillis / 60000) * 60000
    produce(SampleIn, sampleEvents(start))
    val cleaned = consume(Seq("carts.clean-carts"), 45, 3.minutes)
    assertEquals(cleaned.size, 45)
    assert(cleaned.forall(r => !new String(r.value).contains("not a cart event")))
    val expected = closedWindows(start)
    eventually(5.minutes, "every closed window counted once") {
      val counted = committedCounts(expected.size, 20.seconds, "carts.cart-counts")
      assertEquals(counted.map(c => (c._1, c._2)).toSet, expected)
      assertEquals(counted.size, expected.size, "no window written twice")
      assert(counted.forall(_._3 == 1L), counted.toString)
    }

    eventually(2.minutes, s"a lag for each inlet (status: $cartsStatus)") {
      assertEquals(
        inletsOf("cleaner").map(i => (i.topic, i.group, i.lag)),
        List((SampleIn, "carts.cleaner.in", Some(0L)))
      )
      val counter = inletsOf("counter")
      assertEquals(
        counter.map(i => (i.topic, i.group)),
        List(("carts.clean-carts", "carts.counter.in"))
      )
      assert(counter.forall(_.lag.isDefined), counter.toString)
    }

    // Both streamlets stopped, reset by the staged `flow`, and started again.
    load(generateSample(stopped = true))
    eventually(5.minutes, s"both stopped (status: $cartsStatus)") {
      assertEquals(
        client.pods
          .inNamespace(Namespace)
          .withLabel(Labels.PipelineKey, "carts")
          .list()
          .getItems
          .asScala
          .count(p => Option(p.getMetadata.getDeletionTimestamp).isEmpty),
        0
      )
      assertEquals(
        cartsStatus.toList.flatMap(_.streamlets).find(_.name == "counter").map(_.detail),
        Some("suspended")
      )
    }
    val (reset, said) = flow("reset", "carts", "-n", Namespace)
    assertEquals(reset, 0, said)
    eventually(3.minutes, "both groups reset") {
      val notes = events("ResetOffsets", "carts")
      assert(notes.exists(_.startsWith("carts.cleaner.in")), notes.toString)
      assert(notes.exists(_.startsWith("carts.counter.in")), notes.toString)
      assert(events("StateDiscarded", "carts").exists(_.contains("epoch 2")))
    }
    load(generateSample(stopped = false))
    eventually(8.minutes, s"carts Ready again (status: $cartsStatus)")(
      assertEquals(cartsStatus.map(_.phase), Some(AnkkaFlowStatus.Ready))
    )
    assertEquals(consume(Seq("carts.clean-carts"), 90, 3.minutes).size, 90, "the input read again")
  }

  // ── ankka topics, as a machine, on a broker that checks ACLs ──────────────────────────────

  private val Users = Map(
    "admin"    -> "admin-secret",
    "network"  -> "network-secret",
    "stranger" -> "stranger-secret",
    "reader"   -> "reader-secret"
  )
  private val CartCheckouts  = "checkout.cart-checkouts"
  private val CleanCheckouts = "checkout.clean-checkouts"

  /**
   * A second broker standing in for ankka's: SASL/PLAIN users for machines, the standard
   * authorizer.
   */
  private def aclKafkaManifest(externalPort: Int): String =
    val jaas = "org.apache.kafka.common.security.plain.PlainLoginModule required " +
      Users.map((u, p) => s"""user_$u=\\"$p\\"""").mkString(" ") + ";"
    s"""apiVersion: v1
       |kind: Namespace
       |metadata: { name: kafka-acl }
       |---
       |apiVersion: v1
       |kind: Service
       |metadata: { name: kafka, namespace: kafka-acl }
       |spec:
       |  selector: { app: kafka-acl }
       |  ports: [ { name: internal, port: 9092 } ]
       |---
       |apiVersion: v1
       |kind: Service
       |metadata: { name: kafka-external, namespace: kafka-acl }
       |spec:
       |  type: NodePort
       |  selector: { app: kafka-acl }
       |  ports: [ { name: external, port: 9094, targetPort: 9094, nodePort: $AclNodePort } ]
       |---
       |apiVersion: apps/v1
       |kind: StatefulSet
       |metadata: { name: kafka, namespace: kafka-acl }
       |spec:
       |  serviceName: kafka
       |  replicas: 1
       |  selector: { matchLabels: { app: kafka-acl } }
       |  template:
       |    metadata: { labels: { app: kafka-acl } }
       |    spec:
       |      containers:
       |        - name: kafka
       |          image: $KafkaImage
       |          imagePullPolicy: IfNotPresent
       |          env:
       |            - { name: KAFKA_NODE_ID, value: "1" }
       |            - { name: KAFKA_PROCESS_ROLES, value: "broker,controller" }
       |            - { name: KAFKA_LISTENERS, value: "BROKER://:9095,CONTROLLER://:9093,INTERNAL://:9092,EXTERNAL://:9094" }
       |            - { name: KAFKA_ADVERTISED_LISTENERS, value: "BROKER://localhost:9095,INTERNAL://kafka.kafka-acl.svc:9092,EXTERNAL://localhost:$externalPort" }
       |            - { name: KAFKA_LISTENER_SECURITY_PROTOCOL_MAP, value: "BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT,INTERNAL:SASL_PLAINTEXT,EXTERNAL:SASL_PLAINTEXT" }
       |            - { name: KAFKA_CONTROLLER_LISTENER_NAMES, value: "CONTROLLER" }
       |            - { name: KAFKA_CONTROLLER_QUORUM_VOTERS, value: "1@localhost:9093" }
       |            - { name: KAFKA_INTER_BROKER_LISTENER_NAME, value: "BROKER" }
       |            - { name: KAFKA_SASL_ENABLED_MECHANISMS, value: "PLAIN" }
       |            - { name: KAFKA_LISTENER_NAME_INTERNAL_PLAIN_SASL_JAAS_CONFIG, value: "$jaas" }
       |            - { name: KAFKA_LISTENER_NAME_EXTERNAL_PLAIN_SASL_JAAS_CONFIG, value: "$jaas" }
       |            - { name: KAFKA_AUTHORIZER_CLASS_NAME, value: "org.apache.kafka.metadata.authorizer.StandardAuthorizer" }
       |            - { name: KAFKA_SUPER_USERS, value: "User:admin;User:ANONYMOUS" }
       |            - { name: KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND, value: "false" }
       |            - { name: KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR, value: "1" }
       |            - { name: KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR, value: "1" }
       |            - { name: KAFKA_TRANSACTION_STATE_LOG_MIN_ISR, value: "1" }
       |            - { name: KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS, value: "0" }
       |            - { name: KAFKA_AUTO_CREATE_TOPICS_ENABLE, value: "false" }
       |          readinessProbe: { tcpSocket: { port: 9092 }, periodSeconds: 3 }
       |""".stripMargin

  private def saslProps(user: String): Properties =
    val p = new Properties()
    p.put(
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
      s"localhost:${k3s.getMappedPort(AclNodePort)}"
    )
    p.put("security.protocol", "SASL_PLAINTEXT")
    p.put("sasl.mechanism", "PLAIN")
    p.put(
      "sasl.jaas.config",
      s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="$user" password="${Users(
          user
        )}";"""
    )
    p

  private def aclAdmin[A](f: Admin => A): A =
    val a = Admin.create(saslProps("admin"))
    try f(a)
    finally a.close()

  import org.apache.kafka.common.acl.{
    AccessControlEntry,
    AclBinding,
    AclOperation,
    AclPermissionType
  }
  import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourceType}

  /** What ankka's grants give a machine: read its inlet's topic, write its outlet's, its groups. */
  private def machineAcls(user: String, machine: String): java.util.List[AclBinding] =
    def allow(
        kind: ResourceType,
        name: String,
        op: AclOperation,
        pattern: PatternType = PatternType.LITERAL
    ) =
      new AclBinding(
        new ResourcePattern(kind, name, pattern),
        new AccessControlEntry(s"User:$user", "*", op, AclPermissionType.ALLOW)
      )
    java.util.List.of(
      allow(ResourceType.TOPIC, CartCheckouts, AclOperation.READ),
      allow(ResourceType.TOPIC, CleanCheckouts, AclOperation.WRITE),
      allow(
        ResourceType.GROUP,
        s"ankka.machine.affiliates.$machine.",
        AclOperation.READ,
        PatternType.PREFIXED
      )
    )

  private def machineCluster(name: String, user: String, machine: String): String =
    s"""apiVersion: v1
       |kind: Secret
       |metadata: { name: kafka-cluster-$name, namespace: ankka-flow }
       |stringData:
       |  bootstrap.servers: "kafka.kafka-acl.svc:9092"
       |  machine: "affiliates/$machine"
       |  connection-config: |
       |    security.protocol=SASL_PLAINTEXT
       |    sasl.mechanism=PLAIN
       |    sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$user" password="${Users(
        user
      )}";
       |""".stripMargin

  private def checkoutsSpec(pipeline: String, cluster: String): AnkkaFlowSpec =
    AnkkaFlowSpec(
      pipeline = pipeline,
      version = "test",
      descriptorVersion = "2.0",
      streamlets = List(
        StreamletSpec(
          name = "cleaner",
          image = cleanerImage,
          engine = "pekko-streams",
          inlets = Map("in" -> "cart-checkouts"),
          outlets = Map("out" -> "clean-checkouts"),
          descriptor = Fixtures.descriptor("cart-cleaner")
        )
      ),
      topics = List(
        TopicSpec(
          id = "cart-checkouts",
          name = CartCheckouts,
          managed = false,
          ankka = Some(AnkkaRef("checkout", "cart-checkouts")),
          cluster = Some(cluster),
          consumerConfig = Map("auto.offset.reset" -> "earliest")
        ),
        TopicSpec(
          id = "clean-checkouts",
          name = CleanCheckouts,
          managed = false,
          ankka = Some(AnkkaRef("checkout", "clean-checkouts")),
          cluster = Some(cluster)
        )
      )
    )

  private def pipelineStatus(name: String): Option[AnkkaFlowStatus] =
    Option(flows.withName(name).get()).flatMap(r => Option(r.getStatus))
  private def conditionOf(name: String, kind: String): Option[Condition] =
    pipelineStatus(name).toList.flatMap(_.conditions).find(_.`type` == kind)

  private var aclReady = false
  private def aclBroker(): Unit =
    if !aclReady then
      flows.withName("carts").delete(): Unit
      load(aclKafkaManifest(k3s.getMappedPort(AclNodePort)))
      eventually(4.minutes, "the ACL broker to be reachable from outside")(
        assert(Try(aclAdmin(_.listTopics().names().get())).isSuccess)
      )
      aclAdmin { a =>
        a.createTopics(
          java.util.List.of(
            new NewTopic(CartCheckouts, 3, 1.toShort),
            new NewTopic(CleanCheckouts, 3, 1.toShort)
          )
        ).all()
          .get()
        a.createAcls(machineAcls("network", "network")).all().get()
        a.createAcls(
          java.util.List.of(
            new AclBinding(
              new ResourcePattern(ResourceType.TOPIC, CleanCheckouts, PatternType.LITERAL),
              new AccessControlEntry("User:reader", "*", AclOperation.READ, AclPermissionType.ALLOW)
            ),
            new AclBinding(
              new ResourcePattern(ResourceType.GROUP, "reader-", PatternType.PREFIXED),
              new AccessControlEntry("User:reader", "*", AclOperation.READ, AclPermissionType.ALLOW)
            )
          )
        ).all()
          .get()
      }
      aclReady = true

  test("a pipeline reads and writes ankka topics as the machine its cluster Secret names") {
    aclBroker()
    load(machineCluster("pipelines", "network", "network"))
    client
      .resource(AnkkaFlow(Namespace, "checkouts", checkoutsSpec("checkouts", "pipelines")))
      .fieldManager("test")
      .forceConflicts()
      .serverSideApply(): Unit
    eventually(6.minutes, s"checkouts to be Ready (status: ${pipelineStatus("checkouts")})")(
      assertEquals(pipelineStatus("checkouts").map(_.phase), Some(AnkkaFlowStatus.Ready))
    )
    assertEquals(
      pipelineStatus("checkouts").toList.flatMap(_.streamlets).flatMap(_.inlets).map(_.group),
      List("ankka.machine.affiliates.network.checkouts.cleaner.in")
    )
    assertEquals(conditionOf("checkouts", Condition.TopicNotGranted), None)

    val producer =
      new KafkaProducer(saslProps("admin"), new ByteArraySerializer, new ByteArraySerializer)
    try
      inputs.foreach((k, v) =>
        producer.send(new ProducerRecord(CartCheckouts, k.getBytes, v.getBytes)).get()
      )
    finally producer.close()

    val p = saslProps("reader")
    p.put(ConsumerConfig.GROUP_ID_CONFIG, s"reader-${UUID.randomUUID()}")
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    val reader = new KafkaConsumer(p, new ByteArrayDeserializer, new ByteArrayDeserializer)
    try
      reader.subscribe(java.util.List.of(CleanCheckouts))
      val got      = mutable.ArrayBuffer.empty[ConsumerRecord[Array[Byte], Array[Byte]]]
      val deadline = 3.minutes.fromNow
      while got.size < kept(10000).size && deadline.hasTimeLeft() do
        got ++= reader.poll(JDuration.ofMillis(500)).asScala
      assertEquals(seqs(got.toVector).toSet, kept(10000))
    finally reader.close()
  }

  test("a topic the machine holds no grant for is reported by name, and clears once it does") {
    aclBroker()
    load(machineCluster("strangers", "stranger", "stranger"))
    client
      .resource(AnkkaFlow(Namespace, "strangers", checkoutsSpec("strangers", "strangers")))
      .fieldManager("test")
      .forceConflicts()
      .serverSideApply(): Unit
    eventually(1.minute, s"TopicNotGranted (status: ${pipelineStatus("strangers")})") {
      val c = conditionOf("strangers", Condition.TopicNotGranted)
      assertEquals(c.map(_.reason), Some("Read"))
      assert(
        c.exists(
          _.message.contains(
            s"machine affiliates/stranger holds no grant to read topic $CartCheckouts"
          )
        ),
        c.toString
      )
      assertEquals(pipelineStatus("strangers").map(_.phase), Some(AnkkaFlowStatus.Degraded))
    }
    assert(events(Condition.TopicNotGranted, "strangers").nonEmpty)

    aclAdmin(_.createAcls(machineAcls("stranger", "stranger")).all().get())
    eventually(
      3.minutes,
      s"strangers Ready once granted (status: ${pipelineStatus("strangers")})"
    ) {
      assertEquals(conditionOf("strangers", Condition.TopicNotGranted), None)
      assertEquals(pipelineStatus("strangers").map(_.phase), Some(AnkkaFlowStatus.Ready))
    }

    // The grant revoked: the condition returns.
    aclAdmin(
      _.deleteAcls(machineAcls("stranger", "stranger").asScala.map(_.toFilter).asJava).all().get()
    )
    eventually(2.minutes, "TopicNotGranted again once revoked")(
      assert(conditionOf("strangers", Condition.TopicNotGranted).isDefined)
    )
  }
