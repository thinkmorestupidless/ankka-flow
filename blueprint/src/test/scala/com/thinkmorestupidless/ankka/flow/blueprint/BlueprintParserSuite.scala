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

/**
 * Carried from Cloudflow's BlueprintParserSpec
 * (core/cloudflow-blueprint/src/test/.../BlueprintParserSpec.scala) as munit.
 */
class BlueprintParserSuite extends munit.FunSuite:

  test("fail verification if streamlets and streamlet descriptors are empty") {
    val blueprint = Blueprint.parseString(
      """blueprint {
        |  streamlets {
        |  }
        |  topics {
        |  }
        |}
        |""".stripMargin,
      Vector.empty
    )
    assertEquals(
      blueprint.problems.toSet,
      Set[BlueprintProblem](EmptyStreamlets, EmptyStreamletDescriptors)
    )
  }

  test("read producers, consumers, cluster name, and extra config") {
    val blueprint = Blueprint.parseString(
      s"""blueprint {
         |  streamlets {
         |    metrics = "sample-metrics"
         |  }
         |  topics {
         |    metrics {
         |      producers = [metrics.out]
         |      consumers = [validation.in]
         |      cluster = "eastern-canada-1"
         |      extra = "data"
         |    }
         |  }
         |}
         |""".stripMargin,
      Vector.empty
    )
    val metricsTopic = blueprint.topics.head
    assertEquals(metricsTopic.id, "metrics")
    assertEquals(metricsTopic.name, "metrics")
    assertEquals(metricsTopic.producers, Vector("metrics.out"))
    assertEquals(metricsTopic.consumers, Vector("validation.in"))
    assertEquals(metricsTopic.cluster, Some("eastern-canada-1"))
    assertEquals(metricsTopic.kafkaConfig.getString("extra"), "data")
  }

  test("overwrite the topic name") {
    val blueprint = Blueprint.parseString(
      """blueprint {
        |  streamlets {
        |  }
        |  topics {
        |    metrics {
        |      topic.name = "ere"
        |    }
        |  }
        |}
        |""".stripMargin,
      Vector.empty
    )
    val metricsTopic = blueprint.topics.head
    assertEquals(metricsTopic.id, "metrics")
    assertEquals(metricsTopic.name, "ere")
  }

  test("keep topic config") {
    val blueprint = Blueprint.parseString(
      """blueprint {
        |  streamlets {
        |  }
        |  topics {
        |    metrics {
        |      topic {
        |        // See org.apache.kafka.common.config.TopicConfig
        |        // This sections uses Java properties, HOCON units (s, kb) are not available
        |        retention.ms = 3600000
        |        cleanup.policy = compact
        |      }
        |    }
        |  }
        |}
        |""".stripMargin,
      Vector.empty
    )
    val settings = TopicSettings.fromConfig(blueprint.topics.head.kafkaConfig)
    assertEquals(settings.topicConfig("retention.ms"), "3600000")
    assertEquals(settings.topicConfig("cleanup.policy"), "compact")
  }

  test("read the pipeline name, partitions, replicas and batching") {
    val blueprint = Blueprint.parseString(
      """blueprint {
        |  name = cart
        |  streamlets { }
        |  topics {
        |    t {
        |      partitions = 6
        |      replicas = 3
        |      consumer-config { auto.offset.reset = earliest, flow.batch { max-records = 10 } }
        |    }
        |  }
        |}
        |""".stripMargin,
      Vector.empty
    )
    assertEquals(blueprint.name, Some("cart"))
    val s = TopicSettings.fromConfig(blueprint.topics.head.kafkaConfig)
    assertEquals((s.partitions, s.replicas), (Some(6), Some(3)))
    assertEquals(s.consumerConfig, Map("auto.offset.reset" -> "earliest"))
    assertEquals(s.batch.maxRecords, 10)
  }

  test("an invalid format is a problem, not an exception") {
    val b = Blueprint.parseString("blueprint { streamlets = [ }", Vector.empty)
    assert(b.problems.head.isInstanceOf[BlueprintFormatError])
    val c = Blueprint.parseString("blueprint { streamlets {}, connections {} }", Vector.empty)
    assert(c.problems.head.isInstanceOf[BlueprintFormatError])
  }
