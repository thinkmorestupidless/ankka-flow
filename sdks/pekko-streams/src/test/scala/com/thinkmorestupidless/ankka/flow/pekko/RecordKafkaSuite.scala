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

package com.thinkmorestupidless.ankka.flow.pekko

import scala.collection.immutable
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.apache.pekko.kafka.{CommitterSettings, ConsumerSettings, ProducerSettings, Subscriptions}
import org.apache.pekko.kafka.scaladsl.Consumer
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}

/**
 * Keys and headers across real Kafka: written by a producer, read by a committable source, written
 * on again by a relay adding a header and committing after the write, and what reaches the wire
 * checked with a plain Kafka consumer, so nothing here trusts the relay to report on itself.
 * Carried from Cloudflow's RecordKafkaSpec
 * (core/cloudflow-pekko-tests/.../scaladsl/RecordKafkaSpec.scala) as munit.
 */
class RecordKafkaSuite extends KafkaSuite:

  private val Keys   = 10
  private val PerKey = 5
  private val Binary = Array[Byte](0, -1, 42, 127, -128)

  private given ExecutionContext = system.dispatcher

  test(
    "carry every key and header across Kafka unchanged, keep each key on one partition, and keep its order"
  ) {
    val genTopic   = createTopic(uniqueTopic("gen"), 53)
    val relayTopic = createTopic(uniqueTopic("relay"), 53)

    val generated = for
      k <- 0 until Keys
      n <- 0 until PerKey
    yield
      val id = k * PerKey + n
      (
        Some(s"cart-$k"),
        s"$id",
        Seq(
          "ce_type" -> "ItemAdded".getBytes("UTF-8"),
          "ce_id"   -> id.toString.getBytes("UTF-8"),
          "raw"     -> Binary
        )
      )

    // The generator: one record at a time so each key's order is the send order.
    publish(genTopic, generated)

    // The relay: adds a `stage` header, writes to the relay topic, commits after the write.
    val producer = ProducerSettings(system, new ByteArraySerializer, new ByteArraySerializer)
      .withBootstrapServers(bootstrap)
      .createKafkaProducer()
    def write(batch: immutable.Seq[ProducerRecord[Array[Byte], Array[Byte]]]): Future[Unit] =
      Source(batch)
        .mapAsync(1)(r =>
          Future(producer.send(r).get()): Future[org.apache.kafka.clients.producer.RecordMetadata]
        )
        .runWith(Sink.ignore)
        .map(_ => ())
    val consumer = ConsumerSettings(system, new ByteArrayDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers(bootstrap)
      .withGroupId("record-app.relay.in")
      .withProperty("auto.offset.reset", "earliest")
      .withStopTimeout(1.second)
    val (control, _) = Consumer
      .committableSource(consumer, Subscriptions.topics(genTopic))
      .map { m =>
        val in = m.record
        val headers =
          in.headers.toArray.toSeq :+ new RecordHeader("stage", "relay".getBytes("UTF-8"))
        new ProducerRecord[Array[Byte], Array[Byte]](
          relayTopic,
          null,
          in.key,
          in.value,
          headers.asJava.asInstanceOf[java.lang.Iterable[org.apache.kafka.common.header.Header]]
        ) -> m.committableOffset
      }
      .toMat(
        CommitAfterWrite.sinkCommittingAfter(
          write,
          CommitAfterWrite.defaultCommitterSettings(CommitterSettings(system))
        )
      )(Keep.both)
      .run()

    val received = consumeAll(relayTopic, generated.size)
    Await.ready(control.shutdown(), 10.seconds)
    producer.close()

    assertEquals(received.size, generated.size)
    val byId = received.map(r => str(r.value).toInt -> r).toMap
    generated.foreach { (key, value, _) =>
      val id     = value.toInt
      val actual = byId(id)
      assertEquals(str(actual.key), key.get)
      assertEquals(
        actual.headers.toArray.map(_.key).toList,
        List("ce_type", "ce_id", "raw", "stage")
      )
      assertEquals(str(actual.headers.lastHeader("ce_type").value), "ItemAdded")
      assertEquals(str(actual.headers.lastHeader("ce_id").value), id.toString)
      assertEquals(actual.headers.lastHeader("raw").value.toList, Binary.toList)
      assertEquals(str(actual.headers.lastHeader("stage").value), "relay")
    }

    val byKey = received.groupBy(r => str(r.key))
    byKey.foreach { (key, records) =>
      assertEquals(records.map(_.partition).distinct.size, 1, key)
      assertEquals(
        records.sortBy(_.offset).map(r => str(r.value).toInt),
        records.map(r => str(r.value).toInt).sorted,
        key
      )
    }
    // The keys spread: nothing collapsed everything onto one partition (the topic has 53).
    assert(byKey.values.map(_.head.partition).toSet.size > 1)
  }
