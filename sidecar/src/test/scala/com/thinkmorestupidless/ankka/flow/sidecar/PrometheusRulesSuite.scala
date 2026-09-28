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

import java.nio.file.Files

import scala.jdk.CollectionConverters.*
import scala.util.matching.Regex

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory

/**
 * The rules the Prometheus JMX agent runs with inside the sidecar image, against the JMX names the
 * sidecar's Kafka clients actually register: `<pipeline>.<streamlet>.<port>` as the client id. A
 * rule that does not match exports no metric, silently. Carried from Cloudflow's
 * PrometheusRulesSpec
 * (core/cloudflow-sbt-plugin/src/test/scala/cloudflow/sbt/PrometheusRulesSpec.scala) as munit, plus
 * the sidecar's own beans.
 */
class PrometheusRulesSuite extends munit.FunSuite:

  private val ClientId = "shop.processor.in"
  private val Topic    = "nakka.cart-events.v1"

  /**
   * As the JMX exporter 1.0 builds the string it matches from an MBean attribute:
   * `domain<properties><>Attribute: value`, the attribute spelled as the bean spells it. Checked
   * against the real agent in the sidecar image (research, verify-at-implementation item 5).
   */
  private def jmxName(domain: String, properties: (String, String)*)(attribute: String) =
    s"$domain<${properties.map((k, v) => s"$k=$v").mkString(", ")}><>$attribute: 0.0"

  /**
   * Parsed as YAML, as the agent parses it: a file the agent cannot read fails here, not in a pod.
   */
  private val patterns: List[Regex] =
    val file  = TestSpecs.repoRoot.resolve("sidecar/src/universal/agent/prometheus.yaml")
    val yaml  = new ObjectMapper(new YAMLFactory).readTree(Files.readAllBytes(file))
    val found = yaml.get("rules").elements.asScala.toList.map(_.get("pattern").asText.r)
    assert(found.nonEmpty)
    found

  private def firstMatch(name: String): Regex =
    patterns.find(_.findFirstIn(name).isDefined).getOrElse(fail(s"no rule matches $name"))

  test(
    "export a consumer's per-partition lag, labelled with the streamlet's client id, topic and partition"
  ) {
    val name = jmxName(
      "kafka.consumer",
      "type"      -> "consumer-fetch-manager-metrics",
      "client-id" -> ClientId,
      "topic"     -> Topic,
      "partition" -> "0"
    )("records-lag")
    val groups = firstMatch(name).findFirstMatchIn(name).get
    assertEquals(groups.group(1), ClientId)
    assertEquals(groups.group(2), Topic)
    assertEquals(groups.group(3), "0")
  }

  test(
    "export records-lag-max by its own rule, not the records-lag one, which its name also matches"
  ) {
    def lag(attr: String) = jmxName(
      "kafka.consumer",
      "type"      -> "consumer-fetch-manager-metrics",
      "client-id" -> ClientId,
      "topic"     -> Topic,
      "partition" -> "0"
    )(attr)
    // The exporter applies the first rule that matches, and `records-lag` is a prefix of
    // `records-lag-max`: the order in the file is what keeps them apart.
    assert(firstMatch(lag("records-lag-max")).regex.contains("records-lag-max"))
    assert(!firstMatch(lag("records-lag")).regex.contains("records-lag-max"))
  }

  test("records-lag-avg is not exported as records-lag: the patterns stop at the attribute name") {
    val avg = jmxName(
      "kafka.consumer",
      "type"      -> "consumer-fetch-manager-metrics",
      "client-id" -> ClientId,
      "topic"     -> Topic,
      "partition" -> "0"
    )("records-lag-avg")
    assert(!patterns.exists(_.findFirstIn(avg).isDefined), s"a rule matches $avg")
  }

  test("export a producer's send rate under the same client id scheme") {
    val name = jmxName(
      "kafka.producer",
      "type"      -> "producer-topic-metrics",
      "client-id" -> "shop.processor.out",
      "topic"     -> Topic
    )("record-send-rate")
    assertEquals(firstMatch(name).findFirstMatchIn(name).get.group(1), "shop.processor.out")
  }

  test("export the sidecar's own in-flight and stall beans by inlet and partition") {
    Seq("InFlight", "StalledSeconds").foreach { attr =>
      val name =
        jmxName("ankka.flow", "type" -> "sidecar", "inlet" -> "in", "partition" -> "3")(attr)
      val m = firstMatch(name).findFirstMatchIn(name).get
      assertEquals((m.group(1), m.group(2)), ("in", "3"))
    }
    // The bean is registered under exactly the properties the rule expects.
    assertEquals(
      Metrics.name("in", 3).getKeyPropertyListString,
      "type=sidecar,inlet=in,partition=3"
    )
  }
