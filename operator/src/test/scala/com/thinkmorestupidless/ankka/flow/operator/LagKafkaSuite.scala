package com.thinkmorestupidless.ankka.flow.operator

import java.util.Properties

import scala.jdk.CollectionConverters.*

import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

/** A group's lag on a topic, as the status reports it, against real Kafka. */
class LagKafkaSuite extends munit.FunSuite:

  private val kafka    = new KafkaContainer(DockerImageName.parse(sys.props("flow.kafka.image")))
  private val executor = new KafkaExecutor

  override def beforeAll(): Unit = kafka.start()
  override def afterAll(): Unit =
    executor.close()
    kafka.stop()

  private def topic(name: String, bootstrap: String = kafka.getBootstrapServers) =
    ResolvedTopic(
      name,
      name,
      true,
      bootstrap,
      Map.empty,
      Map.empty,
      Map.empty,
      Some(3),
      Some(1),
      Map.empty
    )

  private def withAdmin[A](f: Admin => A): A =
    val p = new Properties()
    p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers)
    val a = Admin.create(p)
    try f(a)
    finally a.close()

  /** A topic of three partitions holding `perPartition` records in each. */
  private def filled(name: String, perPartition: Int): Unit =
    withAdmin(_.createTopics(java.util.List.of(new NewTopic(name, 3, 1.toShort))).all().get())
    val p = new Properties()
    p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers)
    val producer = new KafkaProducer(p, new ByteArraySerializer, new ByteArraySerializer)
    try
      for partition <- 0 until 3; i <- 0 until perPartition do
        producer
          .send(new ProducerRecord(name, partition, s"k$i".getBytes, s"$i".getBytes))
          .get(): Unit
    finally producer.close()

  private def commit(group: String, name: String, offsets: (Int, Long)*): Unit =
    withAdmin(
      _.alterConsumerGroupOffsets(
        group,
        offsets.map((p, o) => new TopicPartition(name, p) -> new OffsetAndMetadata(o)).toMap.asJava
      ).all().get()
    ): Unit

  test("a group's lag is the sum over partitions of the end offset minus its committed offset") {
    filled("orders", 10)
    commit("g1", "orders", 0 -> 10L, 1 -> 4L, 2 -> 0L)
    assertEquals(executor.lag("g1", topic("orders")), Some(0L + 6L + 10L))
  }

  test(
    "a partition the group never committed counts whole; a group that committed nothing, the whole topic"
  ) {
    filled("payments", 5)
    commit("g2", "payments", 0 -> 5L)
    assertEquals(executor.lag("g2", topic("payments")), Some(10L))
    assertEquals(executor.lag("never", topic("payments")), Some(15L))
  }

  test("records already deleted from a partition are not lag") {
    filled("trimmed", 8)
    withAdmin(
      _.deleteRecords(
        Map(
          new TopicPartition("trimmed", 0) -> org.apache.kafka.clients.admin.RecordsToDelete
            .beforeOffset(6L)
        ).asJava
      ).all().get()
    )
    assertEquals(executor.lag("fresh", topic("trimmed")), Some(2L + 8L + 8L))
  }

  test("no lag when the broker cannot be asked") {
    assertEquals(executor.lag("g1", topic("orders", bootstrap = "127.0.0.1:1")), None)
  }
