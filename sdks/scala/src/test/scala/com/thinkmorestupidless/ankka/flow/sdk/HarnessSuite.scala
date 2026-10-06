package com.thinkmorestupidless.ankka.flow.sdk

import java.nio.charset.StandardCharsets.UTF_8

import scala.concurrent.duration.*

import com.thinkmorestupidless.ankka.flow.protocol.Json
import com.thinkmorestupidless.ankka.flow.sdk.testkit.Harness

/** The scenarios of features/sdk/scala-streamlet.feature, through the harness. */
class HarnessSuite extends munit.FunSuite:

  private def bytes(s: String)                = s.getBytes(UTF_8)
  private def event(cart: String, total: Int) = bytes(s"""{"cartId":"$cart","total":$total}""")

  /** The cart router: the Python one's declaration and process. */
  final class Router
      extends Streamlet("cart-router", "Routes cart events to the valid or review outlet."):
    val in        = inlet("in", schemaName = "cart-events.v1")
    val valid     = outlet("valid", schemaName = "cart-events.v1")
    val review    = outlet("review", schemaName = "cart-events.v1")
    val threshold = parameter.integer("review-threshold", default = 100)
    def process(batch: Batch): Iterable[Emit] =
      val limit = config(threshold)
      batch.records.map { record =>
        val total = Json.parse(record.valueString).toOption.flatMap(_.field("total")).collect {
          case Json.Num(n) => n
        }
        if total.exists(_ > limit) then review.emit(record) else valid.emit(record)
      }

  test("a streamlet declared in Scala routes a batch as the Python one does") {
    val h = Harness(new Router, Map("review-threshold" -> "50"))
    h.inlet("in")
      .put(event("cart-1", 10), Some(bytes("cart-1")), Seq("ce_type" -> bytes("ItemAdded")))
    h.inlet("in").put(event("cart-2", 99), Some(bytes("cart-2")))
    h.run()
    assertEquals(h.outlet("valid").records.flatMap(_.keyString), Vector("cart-1"))
    assertEquals(h.outlet("review").records.flatMap(_.keyString), Vector("cart-2"))
    assertEquals(
      h.outlet("valid").records.head.headers.map((k, v) => k -> new String(v, UTF_8)),
      Seq("ce_type" -> "ItemAdded")
    )
    assertEquals(h.outlet("valid").records.head.valueString, new String(event("cart-1", 10), UTF_8))
  }

  test("the harness batches and partitions records by key as the sidecar does") {
    val h = Harness(new Router)
    (0 until 50).foreach(i =>
      h.inlet("in").put(event(s"cart-${i % 10}", i), Some(bytes(s"cart-${i % 10}")))
    )
    h.run(partitions = Harness.hashPartitioner(3), maxRecords = Some(7))
    assertEquals(h.failures, Vector.empty)
    assertEquals(h.skipped, Vector.empty)
    assert(h.batches.forall(_.records.size <= 7))
    val place = Harness.hashPartitioner(3)
    h.batches.foreach(b => assert(b.records.forall(r => place(r.key) == b.partition), b.toString))
    val everything = h.outlet("valid").records ++ h.outlet("review").records
    assertEquals(everything.size, 50)
    everything.groupBy(_.keyString).values.foreach { ofOneCart =>
      val inOrder = h.batches
        .flatMap(_.records)
        .filter(r => ofOneCart.exists(_.keyString == r.keyString))
        .map(_.offset)
      assertEquals(inOrder, inOrder.sorted)
    }
  }

  test("a failing batch leaves no emit behind") {
    val failing = new Streamlet("failing"):
      inlet("in", schemaName = "x.v1")
      val out = outlet("out", schemaName = "x.v1")
      def process(batch: Batch): Iterable[Emit] =
        Iterator(out.emit(batch.records.head)) ++ Iterator(
          throw new RuntimeException("no")
        ) to Vector
    val h = Harness(failing)
    h.inlet("in").put(bytes("v"))
    h.run()
    assertEquals(h.failures.map(_.error.getMessage), Vector("no"))
    assertEquals(h.outlet("out").records, Vector.empty)
  }

  test("an emit to an undeclared outlet fails the batch") {
    val rogue = new Streamlet("rogue"):
      inlet("in", schemaName = "x.v1")
      val out = outlet("out", schemaName = "x.v1")
      def process(batch: Batch): Iterable[Emit] =
        Vector(out.emit(batch.records.head), Emit("nope", batch.records.head))
    val h = Harness(rogue)
    h.inlet("in").put(bytes("v"))
    h.run()
    assertEquals(h.failures.map(_.error.getMessage), Vector("emit to undeclared outlet 'nope'"))
    assertEquals(h.outlet("out").records, Vector.empty)
  }

  test("a parameter's value comes from the deploy-time configuration, or its default") {
    final class Configured extends Streamlet("configured"):
      inlet("in", schemaName = "x.v1")
      val limit                                 = parameter.integer("limit", default = 5)
      val pause                                 = parameter.duration("wait", default = 100.millis)
      val size                                  = parameter.memorySize("size", default = "1 MiB")
      val ratio                                 = parameter.double("ratio", default = 0.5)
      val enabled                               = parameter.boolean("enabled", default = false)
      def process(batch: Batch): Iterable[Emit] = Nil
    val defaults = new Configured
    Harness(defaults)
    assertEquals(defaults.config(defaults.limit), 5L)
    assertEquals(defaults.config(defaults.pause), 100.millis)
    assertEquals(defaults.config(defaults.size), 1048576L)
    assertEquals(defaults.config(defaults.ratio), 0.5)
    assertEquals(defaults.config(defaults.enabled), false)
    val set = new Configured
    Harness(
      set,
      Map("limit" -> "9", "wait" -> "2 s", "size" -> "2k", "ratio" -> "1.5", "enabled" -> "true")
    )
    assertEquals(set.config(set.limit), 9L)
    assertEquals(set.config(set.pause), 2.seconds)
    assertEquals(set.config(set.size), 2048L)
    assertEquals(set.config(set.ratio), 1.5)
    assertEquals(set.config(set.enabled), true)
    intercept[IllegalArgumentException](Harness(new Configured, Map("limit" -> "many")))
    intercept[IllegalArgumentException](Harness(new Configured, Map("undeclared" -> "1")))
  }

  test("the SDK decodes nothing and keeps nothing") {
    val echo = new Streamlet("echo"):
      inlet("in", schemaName = "x.v1")
      val out                                   = outlet("out", schemaName = "x.v1")
      def process(batch: Batch): Iterable[Emit] = batch.records.map(r => out.emit(r))
    val h   = Harness(echo)
    val raw = Array[Byte](0, -1, 127, -128, 10)
    h.inlet("in").put(raw)
    h.run()
    h.inlet("in").put(bytes("second"))
    h.run()
    assertEquals(
      h.outlet("out").records.map(_.value.toSeq),
      Vector(raw.toSeq, bytes("second").toSeq)
    )
    assertEquals(h.batches.map(_.records.size), Vector(1, 1))
    assertEquals(h.batches.map(_.records.head.offset), Vector(0L, 1L))
  }

  test("a record no emit was derived from is skipped") {
    val skipper = new Streamlet("skipper"):
      inlet("in", schemaName = "x.v1")
      val out = outlet("out", schemaName = "x.v1")
      def process(batch: Batch): Iterable[Emit] =
        batch.records.filter(_.valueString == "keep").map(r => out.emit(r))
    val h = Harness(skipper)
    h.inlet("in").put(bytes("keep"))
    h.inlet("in").put(bytes("drop"))
    h.run()
    assertEquals(h.skipped.map(_.valueString), Vector("drop"))
  }

  test("an inlet or outlet the streamlet does not declare is named") {
    val h = Harness(new Router)
    val e = intercept[NoSuchElementException](h.inlet("nope"))
    assert(e.getMessage.contains("declared: in"), e.getMessage)
  }
