package com.thinkmorestupidless.ankka.flow.sidecar

import java.time.Duration as JDuration
import java.util.{Properties, UUID}

import scala.collection.mutable
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.apache.pekko.actor.ActorSystem
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

/**
 * A real Kafka per suite (`apache/kafka`, KRaft, the image `-Dflow.kafka.image` names), plus the
 * plain-client helpers the suites use to check what reached the wire without trusting the sidecar.
 */
trait KafkaSuite extends munit.FunSuite:

  /**
   * Broker settings a suite needs beyond the image's defaults, as the image's `KAFKA_*` variables:
   * a suite that waits for compaction shortens the log cleaner's backoff here.
   */
  protected def kafkaEnv: Map[String, String] = Map.empty

  lazy val kafka: KafkaContainer =
    val container = new KafkaContainer(
      DockerImageName.parse(sys.props.getOrElse("flow.kafka.image", "apache/kafka:3.9.1"))
    )
    kafkaEnv.foreach((k, v) => container.withEnv(k, v))
    container
  given system: ActorSystem = ActorSystem("kafka-suite")

  override val munitTimeout: FiniteDuration = 3.minutes

  def bootstrap: String = kafka.getBootstrapServers

  override def beforeAll(): Unit = kafka.start()

  override def afterAll(): Unit =
    system.terminate(): Unit
    kafka.stop()

  def uniqueTopic(prefix: String): String = s"$prefix-${UUID.randomUUID().toString.take(8)}"

  def admin[A](f: Admin => A): A =
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    val a = Admin.create(props)
    try f(a)
    finally a.close()

  def createTopic(name: String, partitions: Int): String =
    admin(_.createTopics(java.util.List.of(new NewTopic(name, partitions, 1.toShort))).all().get())
    name

  /** A topic with its own settings, such as `cleanup.policy`. */
  def createTopic(name: String, partitions: Int, config: Map[String, String]): String =
    admin(
      _.createTopics(
        java.util.List.of(new NewTopic(name, partitions, 1.toShort).configs(config.asJava))
      ).all().get()
    )
    name

  /** Records with a key and no value: what compaction reads as "remove this key". */
  def publishMarkers(topic: String, keys: Seq[String]): Unit =
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    val producer = new KafkaProducer(props, new ByteArraySerializer, new ByteArraySerializer)
    try
      keys.foreach(k =>
        producer
          .send(new ProducerRecord[Array[Byte], Array[Byte]](topic, k.getBytes("UTF-8"), null))
          .get()
      )
    finally producer.close()

  def publish(
      topic: String,
      records: Seq[(Option[String], String, Seq[(String, Array[Byte])])]
  ): Unit =
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    val producer = new KafkaProducer(props, new ByteArraySerializer, new ByteArraySerializer)
    try
      records.foreach { (key, value, headers) =>
        val r = new ProducerRecord[Array[Byte], Array[Byte]](
          topic,
          null,
          key.map(_.getBytes("UTF-8")).orNull,
          value.getBytes("UTF-8"),
          headers
            .map((k, v) => new RecordHeader(k, v): org.apache.kafka.common.header.Header)
            .asJava
        )
        producer.send(r).get()
      }
    finally producer.close()

  def consumeAll(
      topic: String,
      count: Int,
      within: FiniteDuration = 30.seconds
  ): Vector[ConsumerRecord[Array[Byte], Array[Byte]]] =
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, s"verify-${UUID.randomUUID()}")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    val consumer = new KafkaConsumer(props, new ByteArrayDeserializer, new ByteArrayDeserializer)
    try
      consumer.subscribe(java.util.List.of(topic))
      val records  = mutable.ArrayBuffer.empty[ConsumerRecord[Array[Byte], Array[Byte]]]
      val deadline = within.fromNow
      while records.size < count && deadline.hasTimeLeft() do
        records ++= consumer.poll(JDuration.ofMillis(200)).asScala
      records.toVector
    finally consumer.close()

  /** The sum of a group's committed offsets across every partition. */
  def committed(group: String): Long =
    admin(
      _.listConsumerGroupOffsets(group)
        .partitionsToOffsetAndMetadata()
        .get()
        .asScala
        .values
        .map(_.offset)
        .sum
    )

  /** partition -> (committed, end offset), to see which partition a stuck group is stuck on. */
  def perPartition(group: String, topic: String): Map[Int, (Long, Long)] =
    admin { a =>
      val committed =
        a.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get().asScala
      val parts = a
        .describeTopics(java.util.List.of(topic))
        .allTopicNames()
        .get()
        .get(topic)
        .partitions()
        .asScala
        .map(_.partition)
      val tps = parts.map(p => new org.apache.kafka.common.TopicPartition(topic, p))
      val ends = a
        .listOffsets(
          tps.map(tp => tp -> org.apache.kafka.clients.admin.OffsetSpec.latest()).toMap.asJava
        )
        .all()
        .get()
        .asScala
      tps
        .map(tp =>
          tp.partition -> (committed.get(tp).map(_.offset).getOrElse(-1L), ends(tp).offset)
        )
        .toMap
    }

  def eventually[A](within: FiniteDuration = 30.seconds, every: FiniteDuration = 100.millis)(
      check: => A
  ): A =
    val deadline                = within.fromNow
    var last: Option[Throwable] = None
    var result: Option[A]       = None
    while result.isEmpty && deadline.hasTimeLeft() do
      try result = Some(check)
      catch
        case e: (AssertionError | Exception) =>
          last = Some(e)
          Thread.sleep(every.toMillis)
    result.getOrElse(throw last.getOrElse(new AssertionError("eventually: timed out")))

  def str(b: Array[Byte]): String = if b == null then null else new String(b, "UTF-8")
