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
 * Carried from Cloudflow's BlueprintSpec
 * (core/cloudflow-blueprint/src/test/scala/cloudflow/blueprint/BlueprintSpec.scala) as munit: the
 * cases that verify a blueprint. The editing cases (update, remove, disconnect) tested tooling this
 * project does not have; the config-parameter cases are the protocol's descriptor validation now
 * (DescriptorJsonSuite); volume mounts do not exist. An unconnected outlet is a note here, not a
 * problem (spec S2.2), so the expectations that listed UnconnectedOutlets as a problem now find it
 * among the notes.
 */
class BlueprintSuite extends munit.FunSuite:

  test("fail verification if streamlets and streamlet descriptors are empty") {
    assertEquals(
      Blueprint().verify.problems.toSet,
      Set[BlueprintProblem](EmptyStreamlets, EmptyStreamletDescriptors)
    )
  }

  test("fail verification if no streamlets are used") {
    assertEquals(Draft().define(ingress(), processor()).problems, Vector(EmptyStreamlets))
  }

  List("a", "abcd", "a-b", "ab--cd", "1ab2", "1ab", "1-2").foreach { name =>
    test(s"verify if it uses a streamlet with a valid name ('$name')") {
      val i = ingress()
      assertEquals(
        Draft().define(i).use(name, i).connect("out", s"$name.out").problems,
        Vector.empty
      )
    }
  }

  List("A", "aBcd", "9B", "-ab", "ab-", "a_b", "a/b", "a+b").foreach { name =>
    test(s"fail verification if it uses a streamlet with an invalid name ('$name')") {
      val i = ingress()
      assertEquals(
        Draft().define(i).use(name, i).connect("out", s"$name.out").problems,
        Vector(InvalidStreamletName(name))
      )
    }
  }

  test("a streamlet naming a descriptor that no descriptor declares is refused, naming both") {
    val i        = ingress()
    val problems = Draft().define(i).copy(uses = Vector("router" -> "cart-router")).problems
    assert(
      problems.contains(StreamletDescriptorNotFound("router", "cart-router")),
      problems.toString
    )
    assert(BlueprintProblem.toMessage(problems.head).contains("cart-router"))
  }

  test("be able to define, use and connect streamlets") {
    val i = ingress()
    val p = processor()
    val b = Draft()
      .define(i, p)
      .use("foo", i)
      .use("bar", p)
      .connect("foos", "foo.out", "bar.in")
      .connect("foos-processed", "bar.out")
      .blueprint
    assertEquals(b.problems, Vector.empty)
    assertEquals(b.streamlets(0).verified, Some(VerifiedStreamlet("foo", i)))
    assertEquals(b.streamlets(1).verified, Some(VerifiedStreamlet("bar", p)))
  }

  test(
    "be able to connect to the correct inlet using a full port path when the streamlet has more than one inlet"
  ) {
    val i = ingress()
    val m = merge("foo.v1", "bar.v1", "foo.v1")
    val b = Draft()
      .define(i, m)
      .use("foo", i)
      .use("bar", m)
      .connect("foos", "foo.out", "bar.in-0")
      .blueprint
    assertEquals(b.problems, Vector(UnconnectedInlets(Vector(UnconnectedPort("bar", m.inlets(1))))))
    assertEquals(b.notes, Vector(UnconnectedOutlets(Vector(UnconnectedPort("bar", m.outlets(0))))))
  }

  test("an unconnected inlet is refused; an unconnected outlet is allowed and noted (S2.2)") {
    val p = processor()
    val b = Draft().define(p).use("p", p).connect("t", "p.out").blueprint
    assertEquals(b.problems, Vector(UnconnectedInlets(Vector(UnconnectedPort("p", p.inlets(0))))))
    val onlyIn = Draft().define(p).use("p", p).connect("t", "p.in").blueprint
    assertEquals(onlyIn.problems, Vector.empty)
    assertEquals(onlyIn.notes.size, 1)
  }

  test(
    "not fail verification with UnconnectedInlets for already reported IncompatibleSchema problems"
  ) {
    val i = ingress("foo.v1")
    val p = processor("foo.v1", "bar.v1")
    val e = egress("bar.v1")
    val b = Draft()
      .define(i, p, e)
      .use("ingress", i)
      .use("p1", p)
      .use("p2", p)
      .use("e1", e)
      .use("e2", e)
      .connect("foos", "ingress.out", "p1.in")
      .connect("foos2", "ingress.out", "p2.in")
      .connect("bars", "p1.out", "e1.in")
      .connect("bars2", "p2.out", "e1.in")
      .connect("foobar", "ingress.out", "e2.in")
      .blueprint
    assertEquals(b.problems.collect { case u: UnconnectedInlets => u }.size, 0)
    val incompatible = b.problems.filter {
      case PortBoundToManyTopics(_, _) => false
      case _                           => true
    }
    assertEquals(incompatible.size, 1)
    val IncompatibleSchema(a, other, _, _) = incompatible.head: @unchecked
    assertEquals(Set(a.toString, other.toString), Set("ingress.out", "e2.in"))
  }

  test(
    "fail with InvalidPortPath and an unconnected inlet if the inlet/outlet part is missing in connections"
  ) {
    val i = ingress()
    val e = egress()
    val b = Draft()
      .define(i, e)
      .use("ingress", i)
      .use("egress", e)
      .connectTopic(Topic("foobars", producers = Vector("ingress"), consumers = Vector("egress")))
      .blueprint
    assertEquals(
      b.problems,
      Vector(
        UnconnectedInlets(Vector(UnconnectedPort("egress", e.inlets(0)))),
        InvalidPortPath("ingress"),
        InvalidPortPath("egress")
      )
    )
    assertEquals(
      b.notes,
      Vector(UnconnectedOutlets(Vector(UnconnectedPort("ingress", i.outlets(0)))))
    )
  }

  test("fail when a port is bound to more than one topic") {
    val i = ingress()
    val p = processor()
    val b = Draft()
      .define(i, p)
      .use("foo", i)
      .use("bar", p)
      .connect("foos", "foo.out", "bar.in")
      .connect("fooos", "bar.in")
      .connect("foos-processed", "bar.out")
      .connect("foos-processed2", "bar.out")
      .blueprint
    assertEquals(
      b.problems,
      Vector(
        PortBoundToManyTopics("bar.in", Vector("foos", "fooos")),
        PortBoundToManyTopics("bar.out", Vector("foos-processed", "foos-processed2"))
      )
    )
  }

  test(
    "a port path that names a port the descriptor does not declare is refused with suggestions"
  ) {
    val i     = ingress()
    val b     = Draft().define(i).use("foo", i).connect("t", "foo.output").blueprint
    val found = b.problems.collect { case p: PortPathNotFound => p }
    assertEquals(found.map(_.path), Vector("foo.output"))
    assert(BlueprintProblem.toMessage(found.head).contains("foo.out"))
  }

  test("every problem is reported in one pass") {
    val i = ingress("cart-events.v1")
    val e = egress("cart-events.v2")
    val b = Draft()
      .define(i, e)
      .use("Bad", i)
      .use("sink", e)
      .connect("t", "sink.in")
      .connect("x", "nobody.out")
      .blueprint
    assert(b.problems.size >= 2, b.problems.toString)
    assert(b.problems.exists(_.isInstanceOf[InvalidStreamletName]))
    assert(b.problems.exists(_.isInstanceOf[PortPathNotFound]))
  }

  test("duplicate streamlet names are refused") {
    val i = ingress()
    val b = Draft().define(i).use("a", i).use("a", i).connect("t", "a.out").blueprint
    assert(b.problems.exists(_.isInstanceOf[DuplicateStreamletNamesFound]), b.problems.toString)
  }
