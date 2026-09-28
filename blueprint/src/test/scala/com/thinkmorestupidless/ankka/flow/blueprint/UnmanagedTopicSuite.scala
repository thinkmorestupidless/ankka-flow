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

package com.thinkmorestupidless.ankka.flow.blueprint

import Builders.*

/**
 * A blueprint consuming a topic another system owns (an ankka service's event topic), declared the
 * way a user writes it and followed to the settings the consuming streamlet runs with. Carried from
 * Cloudflow's UnmanagedTopicSpec
 * (core/cloudflow-blueprint/src/test/.../deployment/UnmanagedTopicSpec.scala) as munit; the
 * resource the operator runs is built by the CLI, so this follows the topic to its verified
 * settings.
 */
class UnmanagedTopicSuite extends munit.FunSuite:

  private val graphSink =
    descriptor(name = "graph-sink", inlets = Seq("in" -> json("nakka.cart-events.v1")))

  private val blueprintConf =
    """blueprint {
      |  streamlets {
      |    graph = graph-sink
      |  }
      |  topics {
      |    cart-events {
      |      topic.name = "nakka.cart-events.v1"
      |      managed = false
      |      bootstrap.servers = "nakka-kafka-bootstrap.kafka.svc:9092"
      |      consumer-config {
      |        auto.offset.reset = earliest
      |      }
      |      consumers = [graph.in]
      |    }
      |  }
      |}
      |""".stripMargin

  private lazy val blueprint = Blueprint.parseString(blueprintConf, Vector(graphSink))

  test("verify with consumers only: nothing in the blueprint produces to it") {
    assertEquals(blueprint.problems, Vector.empty)
  }

  test(
    "hand the consuming streamlet the external topic's name, its brokers and its consumer settings, marked unmanaged"
  ) {
    val topic = blueprint.verified.toOption.get.topics
      .find(_.consumers.exists(_.portPath.toString == "graph.in"))
      .get
    assertEquals(topic.id, "cart-events")
    assertEquals(topic.kafkaName("any-pipeline"), "nakka.cart-events.v1")
    assertEquals(topic.managed, false)
    val settings = TopicSettings.fromConfig(topic.kafkaConfig)
    assertEquals(settings.bootstrapServers, Some("nakka-kafka-bootstrap.kafka.svc:9092"))
    assertEquals(settings.consumerConfig, Map("auto.offset.reset" -> "earliest"))
  }

  test("an unmanaged topic with producers is refused (FR-005)") {
    val source = descriptor(name = "src", outlets = Seq("out" -> json("nakka.cart-events.v1")))
    val b = Blueprint.parseString(
      blueprintConf
        .replace("graph = graph-sink", "graph = graph-sink\n    src = src")
        .replace("consumers = [graph.in]", "consumers = [graph.in]\n      producers = [src.out]"),
      Vector(graphSink, source)
    )
    assert(b.problems.exists(_.isInstanceOf[UnmanagedTopicHasProducers]), b.problems.toString)
  }

  test("an unmanaged topic that names no brokers or cluster is refused") {
    val b = Blueprint.parseString(
      blueprintConf.replace("""bootstrap.servers = "nakka-kafka-bootstrap.kafka.svc:9092"""", ""),
      Vector(graphSink)
    )
    assertEquals(b.problems, Vector(UnmanagedTopicWithoutBrokers("cart-events")))
    val withCluster = Blueprint.parseString(
      blueprintConf.replace(
        """bootstrap.servers = "nakka-kafka-bootstrap.kafka.svc:9092"""",
        "cluster = shop"
      ),
      Vector(graphSink)
    )
    assertEquals(withCluster.problems, Vector.empty)
  }

  test("a managed topic's Kafka name defaults to <pipeline>.<id>") {
    val t =
      VerifiedTopic("valid-carts", Vector.empty, None, com.typesafe.config.ConfigFactory.empty())
    assertEquals(t.kafkaName("cart"), "cart.valid-carts")
  }
