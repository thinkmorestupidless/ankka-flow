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

  test("the group id is <pipeline>.<streamlet>.<inlet>, as the sidecar reads with") {
    assertEquals(ResetRequest.groupId("cart", "router", "in"), "cart.router.in")
  }

  test("an annotation that does not parse is no request") {
    assertEquals(ResetRequest.request(flow(ResetRequest.RequestAnnotation -> "not json")), None)
  }
