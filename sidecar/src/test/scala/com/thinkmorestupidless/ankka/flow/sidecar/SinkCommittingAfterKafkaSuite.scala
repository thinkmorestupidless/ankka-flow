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

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.immutable
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.Try

import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.pekko.kafka.{CommitterSettings, ConsumerSettings, Subscriptions}
import org.apache.pekko.kafka.scaladsl.Consumer
import org.apache.pekko.stream.scaladsl.Keep

/**
 * What the commit-after-write sink commits, against real Kafka: never an offset whose record has
 * not been written, and after a failed write, a restart resumes from the last commit so that every
 * record is written, none skipped. Carried from Cloudflow's SinkCommittingAfterKafkaSpec
 * (core/cloudflow-pekko-tests/.../scaladsl/SinkCommittingAfterKafkaSpec.scala) as munit, over
 * `CommitAfterWrite` directly.
 *
 * `sbt mutationCheck` runs this suite with the commit moved ahead of the write and requires it to
 * fail (SC-006).
 */
class SinkCommittingAfterKafkaSuite extends KafkaSuite:

  private val Records       = 20
  private val FailingRecord = 12
  private val Group         = "items-app.writer.in"

  private given ExecutionContext = system.dispatcher

  test(
    "commit only what it has written, and after a failed write resume from there without skipping a record"
  ) {
    val topic = createTopic(uniqueTopic("items"), 1)
    // All records under one key, so on one partition, where each record's offset is its number.
    publish(topic, (0 until Records).map(n => (Some("one-key"), n.toString, Nil)))
    val written = new ConcurrentLinkedQueue[Int]()

    // First run: the write fails on the batch holding record 12, which stops the stream.
    val failOn = new AtomicInteger(FailingRecord)
    val first  = run(topic, written, failOn)
    eventually()(assertEquals(failOn.get, -1))
    Try(Await.ready(first._2, 10.seconds))
    Try(Await.ready(first._1.shutdown(), 10.seconds))

    val committedAfterFailure = committed(Group)
    val clue = s"committed $committedAfterFailure, written ${written.asScala.toList.sorted}"
    assert(committedAfterFailure <= FailingRecord.toLong, clue)
    assert((0L until committedAfterFailure).forall(n => written.contains(n.toInt)), clue)
    assert(!written.contains(FailingRecord), clue)

    // Second run, same consumer group: it resumes from the last commit and writes the rest.
    val second = run(topic, written, new AtomicInteger(-1))
    eventually()(assertEquals(written.asScala.toSet, (0 until Records).toSet))
    eventually()(assertEquals(committed(Group), Records.toLong))
    Await.ready(second._1.shutdown(), 10.seconds): Unit
  }

  /** Writes a batch, unless it holds the record `failOn` names: then it fails, once. */
  private def run(topic: String, written: ConcurrentLinkedQueue[Int], failOn: AtomicInteger) =
    def write(batch: immutable.Seq[Int]): Future[Unit] =
      if batch.contains(failOn.get) then
        failOn.set(-1)
        Future.failed(new RuntimeException(s"write failed on $batch"))
      else
        batch.foreach(n => written.add(n))
        Future.unit
    val settings = ConsumerSettings(system, new ByteArrayDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers(bootstrap)
      .withGroupId(Group)
      .withProperty("auto.offset.reset", "earliest")
      .withStopTimeout(1.second)
    Consumer
      .committableSource(settings, Subscriptions.topics(topic))
      .map(m => new String(m.record.value, "UTF-8").toInt -> m.committableOffset)
      .toMat(
        CommitAfterWrite.sinkCommittingAfter[Int](
          write,
          CommitAfterWrite.defaultCommitterSettings(CommitterSettings(system)),
          batchSize = 5,
          batchWithin = 200.millis
        )
      )(Keep.both)
      .run()
