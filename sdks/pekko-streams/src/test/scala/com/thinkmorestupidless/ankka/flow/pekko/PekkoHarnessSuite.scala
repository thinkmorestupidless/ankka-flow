package com.thinkmorestupidless.ankka.flow.pekko

import scala.concurrent.Future

/** The harness runs a streamlet's logic over records in memory: no broker, no cluster. */
class PekkoHarnessSuite extends munit.FunSuite:

  /**
   * Keeps even numbers, doubled; drops odd ones; fails on 13; sends multiples of 4 to `fours` too.
   */
  final class Evens extends PekkoStreamlet("evens"):
    val in    = inlet("in", Contract("numbers.v1"), Codec.string)
    val out   = outlet("out", Contract("numbers.v1"), Codec.string)
    val fours = outlet("fours", Contract("numbers.v1"), Codec.string)
    val by    = intParameter("by", default = 2)
    val label = stringParameter("label")

    def logic(ctx: Context): Logic = from(in) {
      _.map(r => r.withValue(r.value.toInt))
        .map { r =>
          if r.value == 13 then throw new IllegalStateException("thirteen") else r
        }
        .filter(_.value % 2 == 0)
        .map { r =>
          val doubled = s"${ctx(label)}${r.value * ctx(by)}"
          if r.value % 4 == 0 then Seq(out(r.withValue(doubled)), fours(r.key.get, doubled))
          else Seq(out(r.withValue(doubled)))
        }
    }

  private def records(values: Int*) = values.map(v => Record(Some(s"k$v"), v.toString))

  private val evens = new Evens

  test(
    "a filter-and-map streamlet returns each outlet's records in order and the committed positions"
  ) {
    val result = PekkoHarness(evens, "label" -> "#").run(evens.in -> records(1 to 10*))
    assertEquals(
      result(evens.out).map(r => r.key.get -> r.value),
      Vector("k2" -> "#4", "k4" -> "#8", "k6" -> "#12", "k8" -> "#16", "k10" -> "#20")
    )
    assertEquals(result.committed, Vector(1, 3, 5, 7, 9))
  }

  test("a dropped record emits nothing and is still covered by a later commit") {
    val result = PekkoHarness(evens, "label" -> "").run(evens.in -> records(1, 3, 2))
    assertEquals(result(evens.out).map(_.value), Vector("4"))
    assertEquals(result.committed, Vector(2))
    assert(result.committed.max > 1, "the dropped records at 0 and 1 are covered")
  }

  test("one record may emit to two outlets; the headers and key are kept") {
    val h      = Seq("trace" -> "t1".getBytes)
    val result = PekkoHarness(evens, "label" -> "").run(evens.in -> Seq(Record(Some("k"), "4", h)))
    assertEquals(result(evens.out), Vector(Record(Some("k"), "8", h)))
    assertEquals(result(evens.fours), Vector(Record(Some("k"), "8")))
    assertEquals(result.committed, Vector(0))
  }

  test("a failing element fails the run with its position, and nothing after it is committed") {
    val failure = intercept[PekkoHarness.Failed] {
      PekkoHarness(evens, "label" -> "").run(evens.in -> records(2, 4, 13, 6, 8))
    }
    assertEquals((failure.inlet, failure.position), ("in", 2))
    assertEquals(failure.committed, Vector(0, 1))
    assertEquals(failure.getCause.getMessage, "thirteen")
    assert(failure.getMessage.contains("position 2"), failure.getMessage)
  }

  test("a required parameter with no value fails naming it") {
    val e = intercept[IllegalArgumentException](PekkoHarness(evens).run(evens.in -> records(2)))
    assert(e.getMessage.contains("parameter 'label' has no default and no value"), e.getMessage)
  }

  test("a parameter value not of its type fails naming it") {
    val e = intercept[IllegalArgumentException](
      PekkoHarness(evens, "label" -> "", "by" -> "two").run(evens.in -> records(2))
    )
    assert(e.getMessage.contains("parameter 'by'"), e.getMessage)
  }

  test("a codec that cannot decode fails the run at that record") {
    val picky = new Codec[String]:
      def encode(value: String) = value.getBytes
      def decode(bytes: Array[Byte]) =
        val s = new String(bytes)
        if s == "bad" then throw new IllegalArgumentException("not a number") else s
    final class Picky extends PekkoStreamlet("picky"):
      val in                         = inlet("in", Contract("numbers.v1"), picky)
      val out                        = outlet("out", Contract("numbers.v1"), Codec.string)
      def logic(ctx: Context): Logic = from(in)(_.map(r => out(r)))
    val p = new Picky
    val failure = intercept[PekkoHarness.Failed](
      p.run(p.in -> (records(1).map(_.withValue("ok")) ++ Seq(Record(None, "bad"))))
    )
    assertEquals(failure.position, 1)
    assertEquals(failure.committed, Vector(0))
    assertEquals(failure.getCause.getMessage, "not a number")
  }

  extension (s: PekkoStreamlet)
    private def run[A](inputs: (Inlet[A], Seq[Record[A]])*) = PekkoHarness(s).run(inputs*)

  test("mapAsync, Option and Nil elements: Nil and None write nothing and still commit") {
    final class Async extends PekkoStreamlet("async"):
      val in  = inlet("in", Contract("numbers.v1"), Codec.string)
      val out = outlet("out", Contract("numbers.v1"), Codec.string)
      def logic(ctx: Context): Logic = from(in) {
        _.mapAsync(4)(r => Future.successful(r)).map { r =>
          r.value match
            case "none" => None
            case "nil"  => Nil
            case v      => Some(out(r.withValue(v.toUpperCase)))
        }
      }
    val a      = new Async
    val result = a.run(a.in -> Seq("a", "none", "nil", "b").map(v => Record(None, v)))
    assertEquals(result(a.out).map(_.value), Vector("A", "B"))
    assertEquals(result.committed, Vector(0, 1, 2, 3))
  }

  test(
    "a streamlet whose logic gives no stage for an inlet it is given records for fails naming it"
  ) {
    final class Idle extends PekkoStreamlet("idle"):
      val in                         = inlet("in", Contract("numbers.v1"), Codec.string)
      def logic(ctx: Context): Logic = Logic.empty
    val i = new Idle
    val e = intercept[IllegalArgumentException](i.run(i.in -> records(1)))
    assert(e.getMessage.contains("inlet 'in'"), e.getMessage)
  }
