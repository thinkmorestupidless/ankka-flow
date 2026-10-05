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

package com.thinkmorestupidless.ankka.flow.cli

import java.nio.file.{Files, Path}

import com.fasterxml.jackson.databind.ObjectMapper
import com.thinkmorestupidless.ankka.flow.crd.*
import io.fabric8.kubernetes.api.model.{ObjectMetaBuilder, PodBuilder}
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientBuilder}
import io.fabric8.kubernetes.client.server.mock.{KubernetesCrudDispatcher, KubernetesMockServer}
import io.fabric8.mockwebserver.Context
import io.fabric8.mockwebserver.MockWebServer

/**
 * `flow reset` against a mock API server, carrying the reset cases of Cloudflow's CliWorkflowSpec
 * (core/cloudflow-cli/src/test/scala/cloudflow/cli/CliWorkflowSpec.scala, lines 380-445) as munit.
 */
class CliResetSuite extends munit.FunSuite:

  private val server = new KubernetesMockServer(
    new Context(),
    new MockWebServer(),
    new java.util.HashMap(),
    new KubernetesCrudDispatcher(),
    false
  )
  private var client: KubernetesClient = scala.compiletime.uninitialized

  private def newClient(): KubernetesClient =
    new KubernetesClientBuilder()
      .withConfig(server.createClient().getConfiguration)
      .withKubernetesSerialization(FlowSerialization())
      .build()

  override def beforeAll(): Unit =
    server.init()
    client = newClient()

  override def afterAll(): Unit = server.destroy()

  private val node = new ObjectMapper().readTree("{}")

  private def pipeline(routerReplicas: Int, sinkReplicas: Int = 0): AnkkaFlow =
    AnkkaFlow(
      "shop",
      "cart",
      AnkkaFlowSpec(
        pipeline = "cart",
        streamlets = List(
          StreamletSpec("source", "i", 0, Map.empty, Map.empty, Map("out" -> "t0"), node),
          StreamletSpec(
            "router",
            "i",
            routerReplicas,
            Map.empty,
            Map("in"  -> "t0"),
            Map("out" -> "t1"),
            node
          ),
          StreamletSpec("sink", "i", sinkReplicas, Map.empty, Map("in" -> "t1"), Map.empty, node)
        )
      )
    )

  private def install(flow: AnkkaFlow): Unit =
    client
      .resources(classOf[AnkkaFlow])
      .inNamespace("shop")
      .resource(flow)
      .createOr(_.update()): Unit

  private def pod(streamlet: String): Unit =
    client.pods
      .inNamespace("shop")
      .resource(
        new PodBuilder()
          .withMetadata(
            new ObjectMetaBuilder()
              .withName(s"flow-cart-$streamlet-x")
              .withLabels(
                java.util.Map.of(
                  "flow.ankka.thinkmorestupidless.com/pipeline",
                  "cart",
                  "flow.ankka.thinkmorestupidless.com/streamlet",
                  streamlet
                )
              )
              .build()
          )
          .build()
      )
      .createOr(_.update()): Unit

  /**
   * A kubeconfig naming the mock server, so `flow` finds the cluster the way a user's does — in
   * this JVM through the `kubeconfig` property, as a binary through `KUBECONFIG` — rather than
   * through a client handed to it.
   */
  private lazy val kubeconfig: Path =
    val file = Files.createTempFile("flow-reset", ".kubeconfig")
    Files.writeString(
      file,
      s"""apiVersion: v1
         |kind: Config
         |clusters:
         |- name: mock
         |  cluster:
         |    server: ${server.createClient().getConfiguration.getMasterUrl.stripSuffix("/")}
         |contexts:
         |- name: flow-test
         |  context:
         |    cluster: mock
         |    user: nobody
         |current-context: flow-test
         |users:
         |- name: nobody
         |  user: {}
         |""".stripMargin
    )
    file

  private def reset(args: String*): (Int, String, String) =
    val result = CliFixtures.flowWith(kubeconfig, ("reset" +: args)*)
    (result.code, result.out, result.err)

  private def annotation: Option[ResetRequest.Request] =
    ResetRequest.request(
      client.resources(classOf[AnkkaFlow]).inNamespace("shop").withName("cart").get()
    )

  test("record a reset request for every streamlet that reads, once all are stopped") {
    install(pipeline(routerReplicas = 0))
    val (code, out, err) = reset("cart", "-n", "shop")
    assertEquals(code, 0, err)
    val request = annotation.get
    assert(out.contains(request.id), out)
    assertEquals(request.streamlets, Nil)
  }

  test("refuse while a target is not scaled to 0") {
    install(pipeline(routerReplicas = 2))
    val (code, _, err) = reset("cart", "-n", "shop", "--streamlet", "router")
    assertEquals(code, 1)
    assert(err.contains("[router] is not scaled to 0"), err)
  }

  test("refuse while a target still has pods") {
    install(pipeline(routerReplicas = 0))
    pod("router")
    val (code, _, err) = reset("cart", "-n", "shop", "--streamlet", "router")
    assertEquals(code, 1)
    assert(err.contains("[router] still has 1 pod(s)"), err)
  }

  test("refuse an unknown streamlet, and one with no inlets") {
    install(pipeline(routerReplicas = 0))
    val (code, _, err) = reset("cart", "-n", "shop", "--streamlet", "nope", "--streamlet", "source")
    assertEquals(code, 1)
    assert(
      err.contains("no streamlet [nope]") && err.contains("streamlet [source] has no inlets"),
      err
    )
  }

  test("reset one stopped streamlet while another runs: accepted, and only it is named") {
    // Rebuilding a graph resets the sink alone (feature 003): the streamlets in front of it may be
    // running, and only the targets must be stopped.
    install(pipeline(routerReplicas = 2, sinkReplicas = 0))
    pod("router")
    val (code, out, err) = reset("cart", "-n", "shop", "--streamlet", "sink")
    assertEquals(code, 0, err)
    val request = annotation.get
    assert(out.contains(request.id), out)
    assertEquals(request.streamlets, List("sink"))

    val (refused, _, why) = reset("cart", "-n", "shop", "--streamlet", "router")
    assertEquals(refused, 1)
    assert(why.contains("[router] is not scaled to 0"), why)
    assertEquals(annotation.map(_.id), Some(request.id), "a refused request must not replace one")
  }

  test("refuse a pipeline that does not exist") {
    val (code, _, err) = reset("ghost", "-n", "shop")
    assertEquals(code, 1)
    assert(err.contains("no pipeline 'ghost'"), err)
  }
