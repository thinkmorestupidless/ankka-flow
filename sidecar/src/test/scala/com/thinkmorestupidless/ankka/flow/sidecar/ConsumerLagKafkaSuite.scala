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

import java.lang.management.ManagementFactory
import javax.management.ObjectName

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import org.apache.pekko.kafka.CommitterSettings

/**
 * Lag attributable to a streamlet's port from Kafka's own metrics: the consumer's `records-lag` and
 * the producer's send metrics appear under client ids `<pipeline>.<streamlet>.<port>`, which is
 * what the Prometheus rules label them by (FR-015, S4.4). Carried from Cloudflow's
 * ConsumerLagKafkaSpec (core/cloudflow-pekko-tests/.../scaladsl/ConsumerLagKafkaSpec.scala) as
 * munit, reading the platform MBean server as the JMX exporter does.
 */
class ConsumerLagKafkaSuite extends KafkaSuite:

  private val server = ManagementFactory.getPlatformMBeanServer

  private def beans(pattern: String) =
    server.queryNames(new ObjectName(pattern), null).asScala.toVector

  test("a reader's lag and a writer's send rate are registered under the streamlet's client ids") {
    val topic = createTopic(uniqueTopic("lag"), 2)
    val out   = createTopic(uniqueTopic("lag-out"), 2)

    val gen = new Producers(
      Map("out" -> OutletConfig("out", topic, "lag-app.gen.out", bootstrap, Map.empty, Map.empty))
    )
    (0 until 20).foreach(i =>
      Await.result(gen.send(EmittedRecord("out", TestSpecs.record(s"k$i", s"$i"))), 10.seconds)
    )

    val stalls  = new Stalls(1.hour, new LogEventSink)
    val metrics = new Metrics(stalls)
    val reader = new InletGraph(
      InletConfig(
        "in",
        topic,
        "lag-app.reader.in",
        "lag-app.reader.in",
        bootstrap,
        Map.empty,
        Map.empty,
        BatchSettings.Default
      ),
      new BatchProcessor:
        def process(batch: InputBatch): Future[Outcome] =
          Future.successful(Outcome.Acked(Vector.empty))
        def revoke(inlet: String, partition: Int, generation: Long): Unit = ()
      ,
      new Producers(
        Map(
          "out" -> OutletConfig("out", out, "lag-app.reader.out", bootstrap, Map.empty, Map.empty)
        )
      ),
      stalls,
      CommitAfterWrite.defaultCommitterSettings(CommitterSettings(system))
    ).run()
    try
      eventually()(assertEquals(committed("lag-app.reader.in"), 20L))

      val producer = beans("kafka.producer:type=producer-topic-metrics,client-id=lag-app.gen.out,*")
      assert(producer.nonEmpty, "no producer metrics for lag-app.gen.out")

      val lag = eventually() {
        val found = beans(
          "kafka.consumer:type=consumer-fetch-manager-metrics,client-id=lag-app.reader.in,topic=*,partition=*"
        )
        assert(found.nonEmpty, "no per-partition consumer metrics for lag-app.reader.in")
        found
      }
      eventually() {
        val max = lag.map(n => server.getAttribute(n, "records-lag").asInstanceOf[Double]).max
        assertEquals(max, 0.0)
      }

      metrics.refresh()
      val sidecarBeans = beans("ankka.flow:type=sidecar,inlet=in,*")
      assert(sidecarBeans.nonEmpty, "the sidecar registered no partition beans")
      sidecarBeans.foreach(n =>
        assertEquals(server.getAttribute(n, "StalledSeconds").asInstanceOf[Long], 0L)
      )
    finally
      Await.ready(reader.control.shutdown(), 10.seconds)
      Await.ready(gen.close(), 10.seconds): Unit
  }
