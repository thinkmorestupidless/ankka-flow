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
 * Carried from Cloudflow's JsonSchemaVerificationSpec
 * (core/cloudflow-blueprint/src/test/.../JsonSchemaVerificationSpec.scala) as munit: a JSON
 * contract connects by schema name, and never across formats.
 */
class JsonContractSuite extends munit.FunSuite:

  private def connect(out: ankka.flow.v1.discovery.Contract, in: ankka.flow.v1.discovery.Contract) =
    val from = descriptor(outlets = Seq("out" -> out))
    val to   = descriptor(inlets = Seq("in" -> in))
    Draft().define(from, to).use("from", from).use("to", to).connect("events", "from.out", "to.in")

  test("verify when they name the same schema") {
    assertEquals(
      connect(json("nakka.cart-events.v1"), json("nakka.cart-events.v1")).problems,
      Vector.empty
    )
  }

  test(
    "fail verification when they name different schemas: a new version of a contract is a new name"
  ) {
    val problems = connect(json("cart-events.v1"), json("cart-events.v2")).problems.collect {
      case p: IncompatibleSchema => p
    }
    assertEquals(problems.size, 1)
    val message = BlueprintProblem.toMessage(problems.head)
    assert(message.contains("from.out") && message.contains("to.in"), message)
    assert(message.contains("cart-events.v1") && message.contains("cart-events.v2"), message)
  }

  test("never verify against another format, even with the same name") {
    val avro     = json("nakka.cart-events.v1").withFormat("avro")
    val problems = connect(json("nakka.cart-events.v1"), avro).problems
    assertEquals(problems.collect { case p: IncompatibleSchema => p }.size, 1)
    assertEquals(problems.collect { case p: UnsupportedFormat => p.format }, Vector("avro"))
  }
