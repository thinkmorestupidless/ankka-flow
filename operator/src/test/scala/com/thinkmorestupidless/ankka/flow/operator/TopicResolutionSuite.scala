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

import com.thinkmorestupidless.ankka.flow.crd.TopicSpec

import Fixtures.*

/**
 * Carried from Cloudflow's TopicActionsSpec
 * (core/cloudflow-operator/src/test/scala/cloudflow/operator/action/TopicActionsSpec.scala) as
 * munit, over `TopicResolution` and `Rendering`: create the topics a pipeline owns, never one it
 * does not, and size a topic from its named cluster. The savepoint case has no counterpart here.
 */
class TopicResolutionSuite extends munit.FunSuite:

  test("create topics when there is no previous deployment") {
    val r = Rendering.render(cart(), settings, observed, "t")
    assertEquals(
      r.actions.collect { case Action.EnsureTopic(t) => t.name }.toSet,
      Set("cart.valid-carts", "cart.review-carts")
    )
  }

  test("create no topic that the pipeline does not own, and still create the ones it does") {
    val r       = Rendering.render(cart(), settings, observed, "t")
    val created = r.actions.collect { case Action.EnsureTopic(t) => t.name }
    assert(!created.contains("shop.cart-events.v1"))
    // and the external topic still reaches the consuming streamlet, by its own name and brokers
    val external = r.resolved.find(_.id == "cart-events").get
    assertEquals(
      (external.name, external.managed, external.bootstrapServers),
      ("shop.cart-events.v1", false, "shop-kafka:9092")
    )
    assertEquals(
      (external.partitions, external.replicas),
      (None, None),
      "an unmanaged topic is never sized"
    )
  }

  test("create a topic for a named kafka cluster, sized by that cluster") {
    val big = KafkaCluster("big", "big-kafka:9092", partitions = Some(100), replicas = Some(3))
    val t = TopicResolution
      .resolve(TopicSpec(id = "t", name = "p.t", cluster = Some("big")), Map("big" -> big))
      .toOption
      .get
    assertEquals(
      (t.partitions, t.replicas, t.bootstrapServers),
      (Some(100), Some(3), "big-kafka:9092")
    )
  }

  test(
    "a managed topic with no partitions from anywhere is an error pointing at the cluster's defaults"
  ) {
    val bare = KafkaCluster("default", "k:9092")
    val e = TopicResolution
      .resolve(TopicSpec(id = "t", name = "p.t"), Map("default" -> bare))
      .left
      .toOption
      .get
    assert(e.contains("no partitions") && e.contains("kafka-cluster-default"), e)
  }

  test("an unmanaged topic with its own brokers needs no cluster") {
    val t = TopicResolution.resolve(
      TopicSpec(id = "x", name = "ext", managed = false, bootstrapServers = Some("ext:9092")),
      Map.empty
    )
    assertEquals(t.map(_.bootstrapServers), Right("ext:9092"))
  }

  test(
    "a cluster Secret is read from its data: bootstrap.servers required, configs as properties"
  ) {
    val c = KafkaCluster.fromData(
      "default",
      Map(
        "bootstrap.servers" -> "k:9092",
        "connection-config" -> "security.protocol=SSL\nssl.truststore.type=PEM\n",
        "partitions"        -> "3"
      )
    )
    assertEquals(
      c.map(_.connectionConfig),
      Right(Map("security.protocol" -> "SSL", "ssl.truststore.type" -> "PEM"))
    )
    assertEquals(c.map(_.partitions), Right(Some(3)))
    assert(KafkaCluster.fromData("x", Map.empty).isLeft)
  }
