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

import com.thinkmorestupidless.ankka.flow.descriptor.Contract

import Builders.*

/**
 * Carried from Cloudflow's JsonSchemaVerificationSpec
 * (core/cloudflow-blueprint/src/test/.../JsonSchemaVerificationSpec.scala) as munit: two contracts
 * connect by name and, where both state a fingerprint, by fingerprint.
 */
class JsonContractSuite extends munit.FunSuite:

  private def connect(out: Contract, in: Contract) =
    val from = descriptor(outlets = Seq("out" -> out))
    val to   = descriptor(inlets = Seq("in" -> in))
    Draft().define(from, to).use("from", from).use("to", to).connect("events", "from.out", "to.in")

  private def incompatible(out: Contract, in: Contract) =
    connect(out, in).problems.collect { case p: IncompatibleContract => p }

  test("verify when they name the same contract") {
    assertEquals(
      connect(json("nakka.cart-events.v1"), json("nakka.cart-events.v1")).problems,
      Vector.empty
    )
  }

  test(
    "fail verification when they name different contracts: a new version of a contract is a new name"
  ) {
    val problems = incompatible(json("cart-events.v1"), json("cart-events.v2"))
    assertEquals(problems.size, 1)
    val message = BlueprintProblem.toMessage(problems.head)
    assert(message.contains("from.out") && message.contains("to.in"), message)
    assert(message.contains("cart-events.v1") && message.contains("cart-events.v2"), message)
  }

  test("equal names connect when one of them states no fingerprint") {
    assertEquals(
      incompatible(fingerprinted("cart-events.v1", 'a'), json("cart-events.v1")),
      Vector.empty
    )
    assertEquals(
      incompatible(json("cart-events.v1"), fingerprinted("cart-events.v1", 'a')),
      Vector.empty
    )
  }

  test("equal names with equal fingerprints connect") {
    assertEquals(
      incompatible(fingerprinted("cart-events.v1", 'a'), fingerprinted("cart-events.v1", 'a')),
      Vector.empty
    )
  }

  test("equal names with differing fingerprints do not connect, and the message names both") {
    val problems =
      incompatible(fingerprinted("cart-events.v1", 'a'), fingerprinted("cart-events.v1", 'b'))
    assertEquals(problems.size, 1)
    val message = BlueprintProblem.toMessage(problems.head)
    assert(message.contains("from.out") && message.contains("to.in"), message)
    assert(
      message.contains("sha256:" + "a" * 64) && message.contains("sha256:" + "b" * 64),
      message
    )
  }
