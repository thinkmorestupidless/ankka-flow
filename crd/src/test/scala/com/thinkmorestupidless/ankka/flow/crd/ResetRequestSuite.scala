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

package com.thinkmorestupidless.ankka.flow.crd

/**
 * Carried from Cloudflow's ResetOffsetsSpec
 * (core/cloudflow-operator/src/test/.../ResetOffsetsSpec.scala), the pure cases, as munit.
 */
class ResetRequestSuite extends munit.FunSuite:

  private def flow(annotations: (String, String)*): AnkkaFlow =
    val f = AnkkaFlow("shop", "cart", AnkkaFlowSpec(pipeline = "cart"))
    if annotations.nonEmpty then
      f.getMetadata.setAnnotations(new java.util.HashMap(annotations.toMap.asInstanceOfJava))
    f

  extension (m: Map[String, String])
    private def asInstanceOfJava: java.util.Map[String, String] =
      val h = new java.util.HashMap[String, String]()
      m.foreach((k, v) => h.put(k, v))
      h

  test("a request round-trips through its annotation") {
    val r = ResetRequest.Request("id-1", List("router"))
    assertEquals(ResetRequest.fromJson(ResetRequest.toJson(r)).toOption, Some(r))
    assertEquals(ResetRequest.request(ResetRequest.withRequest(flow(), r)), Some(r))
  }

  test("a request is pending until its id is marked done, and a new request is a new id") {
    val r    = ResetRequest.Request("id-1")
    val json = ResetRequest.toJson(r)
    assertEquals(ResetRequest.pending(flow(ResetRequest.RequestAnnotation -> json)), Some(r))
    assertEquals(
      ResetRequest.pending(
        flow(ResetRequest.RequestAnnotation -> json, ResetRequest.DoneAnnotation -> "id-1")
      ),
      None
    )
    val next = ResetRequest.toJson(ResetRequest.Request("id-2"))
    assert(
      ResetRequest
        .pending(
          flow(ResetRequest.RequestAnnotation -> next, ResetRequest.DoneAnnotation -> "id-1")
        )
        .isDefined
    )
  }

  test("an empty streamlet list includes every streamlet; a named one only itself") {
    assert(ResetRequest.Request("x").includes("anything"))
    assert(ResetRequest.Request("x", List("router")).includes("router"))
    assert(!ResetRequest.Request("x", List("router")).includes("sink"))
  }

  test("the group id is <pipeline>.<streamlet>.<inlet>, after a machine's prefix when given") {
    assertEquals(ResetRequest.groupId("cart", "router", "in"), "cart.router.in")
    assertEquals(
      ResetRequest.groupId("cart", "router", "in", Some("ankka.machine.affiliates.network.")),
      "ankka.machine.affiliates.network.cart.router.in"
    )
  }

  test("state epochs are absent, so 1, until one is advanced") {
    val f = flow()
    assertEquals(ResetRequest.stateEpochs(f), Map.empty[String, Int])
    assertEquals(ResetRequest.stateEpoch(f, "counter"), 1)
    val once = ResetRequest.withEpochAdvanced(f, "counter")
    assertEquals(ResetRequest.stateEpochs(once), Map("counter" -> 2))
    val twice =
      ResetRequest.withEpochAdvanced(ResetRequest.withEpochAdvanced(once, "counter"), "other")
    assertEquals(ResetRequest.stateEpochs(twice), Map("counter" -> 3, "other" -> 2))
    assertEquals(ResetRequest.stateEpoch(twice, "elsewhere"), 1)
  }

  test("state epochs round-trip through their annotation as a JSON object") {
    val f    = ResetRequest.withEpochAdvanced(flow(), "counter")
    val text = f.getMetadata.getAnnotations.get(ResetRequest.StateEpochsAnnotation)
    assertEquals(text, """{"counter":2}""")
    assertEquals(
      ResetRequest.stateEpochs(flow(ResetRequest.StateEpochsAnnotation -> """{"a":4,"b":1}""")),
      Map("a" -> 4, "b" -> 1)
    )
  }

  test("state epochs that do not parse are all 1, and advancing starts them again") {
    val f = flow(ResetRequest.StateEpochsAnnotation -> "nope")
    assertEquals(ResetRequest.stateEpochs(f), Map.empty[String, Int])
    assertEquals(ResetRequest.stateEpochs(ResetRequest.withEpochAdvanced(f, "c")), Map("c" -> 2))
  }

  test("an annotation that does not parse is no request") {
    assertEquals(ResetRequest.request(flow(ResetRequest.RequestAnnotation -> "not json")), None)
  }
