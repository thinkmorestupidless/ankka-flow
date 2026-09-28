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

package com.thinkmorestupidless.ankka.flow.sidecar

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import ankka.flow.v1.payload.Header
import com.google.protobuf.ByteString
import org.apache.pekko.kafka.CommitterSettings

/**
 * Keys and headers across real Kafka: written by the sidecar's producers, read by an inlet graph,
 * written on again by a relay adding a header, and what reaches the wire checked with a plain Kafka
 * consumer, so nothing here trusts the sidecar to report on itself. Carried from Cloudflow's
 * RecordKafkaSpec (core/cloudflow-pekko-tests/.../scaladsl/RecordKafkaSpec.scala) as munit.
 */
class RecordKafkaSuite extends KafkaSuite:

  private val Keys   = 10
  private val PerKey = 5
  private val Binary = Array[Byte](0, -1, 42, 127, -128)

  private def outlet(name: String, topic: String) =
    OutletConfig(name, topic, s"record-app.$name.out", bootstrap, Map.empty, Map.empty)

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
      EmittedRecord(
        "out",
        TestSpecs
          .record(s"cart-$k", s"$id")
          .withHeaders(
            Seq(
              Header("ce_type", ByteString.copyFromUtf8("ItemAdded")),
              Header("ce_id", ByteString.copyFromUtf8(id.toString)),
              Header("raw", ByteString.copyFrom(Binary))
            )
          )
      )

    // The generator: the sidecar's producer, one record at a time so each key's order is the send order.
    val gen = new Producers(Map("out" -> outlet("out", genTopic)))
    generated.foreach(r => Await.result(gen.send(r), 10.seconds))
    Await.ready(gen.close(), 10.seconds)

    // The relay: an inlet graph whose processor adds a `stage` header and emits to the relay topic.
    val relayOut = new Producers(Map("out" -> outlet("out", relayTopic)))
    val relay = new BatchProcessor:
      def process(batch: InputBatch): Future[Outcome] =
        Future.successful(
          Outcome.Acked(batch.records.map { r =>
            val rec = r.getRecord
            EmittedRecord(
              "out",
              rec.withHeaders(rec.headers :+ Header("stage", ByteString.copyFromUtf8("relay")))
            )
          })
        )
      def revoke(inlet: String, partition: Int, generation: Long): Unit = ()
    val inlet = InletConfig(
      "in",
      genTopic,
      "record-app.relay.in",
      "record-app.relay.in",
      bootstrap,
      Map.empty,
      Map.empty,
      BatchSettings.Default
    )
    val running = new InletGraph(
      inlet,
      relay,
      relayOut,
      new Stalls(1.hour, new LogEventSink),
      CommitAfterWrite.defaultCommitterSettings(CommitterSettings(system))
    ).run()

    val received = consumeAll(relayTopic, generated.size)
    Await.ready(running.control.shutdown(), 10.seconds)
    Await.ready(relayOut.close(), 10.seconds)

    assertEquals(received.size, generated.size)
    val byId = received.map(r => str(r.value).toInt -> r).toMap
    generated.foreach { expected =>
      val id     = expected.record.value.toStringUtf8.toInt
      val actual = byId(id)
      assertEquals(str(actual.key), expected.record.getKey.toStringUtf8)
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
    val _ = received.asJava
  }
