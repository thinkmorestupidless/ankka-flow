/*
 * Copyright (C) 2016-2026 Lightbend Inc. <https://www.lightbend.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.thinkmorestupidless.ankka.flow.operator

import java.time.Duration as JDuration
import java.util.Properties

import scala.jdk.CollectionConverters.*
import scala.util.Try

import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

/**
 * Carried from Cloudflow's ConsumerGroupResetSpec
 * (core/cloudflow-operator/src/test/scala/cloudflow/operator/action/ConsumerGroupResetSpec.scala)
 * as munit, against real Kafka.
 */
class ConsumerGroupResetSuite extends munit.FunSuite:

  private val kafka = new KafkaContainer(
    DockerImageName.parse(sys.props.getOrElse("flow.kafka.image", "apache/kafka:3.9.1"))
  )

  override def beforeAll(): Unit = kafka.start()
  override def afterAll(): Unit  = kafka.stop()

  private def props(extra: (String, String)*) =
    val p = new Properties()
    p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers)
    extra.foreach((k, v) => p.put(k, v))
    p

  private def withAdmin[A](f: Admin => A): A =
    val a = Admin.create(props())
    try f(a)
    finally a.close()

  private def topicWith(name: String, records: Int): Unit =
    withAdmin(_.createTopics(java.util.List.of(new NewTopic(name, 2, 1.toShort))).all().get())
    val producer = new KafkaProducer(props(), new ByteArraySerializer, new ByteArraySerializer)
    try
      (0 until records).foreach(i =>
        producer.send(new ProducerRecord(name, s"k$i".getBytes, s"$i".getBytes)).get()
      )
    finally producer.close()

  private def consumer(group: String) =
    new KafkaConsumer(
      props(
        ConsumerConfig.GROUP_ID_CONFIG          -> group,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
      ),
      new ByteArrayDeserializer,
      new ByteArrayDeserializer
    )

  private def readAll(group: String, topic: String, n: Int): Unit =
    val c = consumer(group)
    try
      c.subscribe(java.util.List.of(topic))
      var seen     = 0
      val deadline = System.currentTimeMillis + 30000
      while seen < n && System.currentTimeMillis < deadline do
        seen += c.poll(JDuration.ofMillis(200)).count
      c.commitSync()
    finally c.close()

  private def committed(group: String): Long =
    withAdmin(
      _.listConsumerGroupOffsets(group)
        .partitionsToOffsetAndMetadata()
        .get()
        .asScala
        .values
        .map(_.offset)
        .sum
    )

  test("move a group's offsets back to the start of its topic") {
    topicWith("reset-a", 10)
    readAll("g-a", "reset-a", 10)
    assertEquals(committed("g-a"), 10L)
    assertEquals(withAdmin(ConsumerGroupReset.toEarliest(_, "g-a", "reset-a")), 2)
    assertEquals(committed("g-a"), 0L)
  }

  test("refuse a group with an active member, naming it and what to do") {
    topicWith("reset-b", 4)
    val c = consumer("g-b")
    try
      c.subscribe(java.util.List.of("reset-b"))
      val deadline = System.currentTimeMillis + 30000
      while c.assignment.isEmpty && System.currentTimeMillis < deadline do
        c.poll(JDuration.ofMillis(200)): Unit
      val refused = Try(withAdmin(ConsumerGroupReset.toEarliest(_, "g-b", "reset-b"))).failed.get
      assert(refused.isInstanceOf[ConsumerGroupReset.GroupHasActiveMembers], refused.toString)
      assert(
        refused.getMessage.contains("[g-b]") && refused.getMessage.contains(
          "scale its streamlet to 0"
        ),
        refused.getMessage
      )
    finally c.close()
  }

  test("give a group that never committed its partitions at the start") {
    topicWith("reset-c", 3)
    assertEquals(withAdmin(ConsumerGroupReset.toEarliest(_, "g-never", "reset-c")), 2)
    assertEquals(committed("g-never"), 0L)
  }
