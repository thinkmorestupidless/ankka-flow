package com.thinkmorestupidless.ankka.flow.sidecar

import java.time.{Clock, Instant, ZoneOffset}
import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.Await
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import ankka.flow.v1.discovery.Spec
import org.apache.pekko.actor.ActorSystem

/**
 * Discovery refusals end the sidecar before anything touches Kafka (S1.6), so this suite needs no
 * broker: every problem is named, reported to the process, and the exit code is 1.
 */
class SupervisorSuite extends munit.FunSuite:

  given system: ActorSystem = ActorSystem("supervisor-suite")

  override def afterAll(): Unit = system.terminate(): Unit

  private val deployed = TestSpecs.fixture("cart-router")
  private val conf =
    SidecarRun.conf(
      "cart",
      "router",
      "localhost:1",
      Seq("in"    -> "t"),
      Seq("valid" -> "v", "review" -> "r")
    )

  private def refused(discovered: Spec): (Int, Vector[String]) =
    val double = new ProcessDouble(discovered)
    val port   = double.start()
    try
      val run  = new SidecarRun(deployed, conf, port)
      val code = Await.result(run.exited, 30.seconds)
      assert(!run.ready)
      (code, double.reportedProblems.asScala.toVector)
    finally double.close()

  test("another major protocol version is refused naming both") {
    val (code, problems) = refused(deployed.withProtocolVersion("99.0"))
    assertEquals(code, 1)
    assert(problems.exists(p => p.contains("'99.0'") && p.contains("'1.0'")), problems.toString)
  }

  test("a process that describes a different streamlet is refused with every difference") {
    val d = deployed.getStreamlet
    val other = deployed.withStreamlet(
      d.withName("cart-auditor").withOutlets(d.outlets.filter(_.name == "valid"))
    )
    val (code, problems) = refused(other)
    assertEquals(code, 1)
    assert(
      problems.exists(
        _.contains("streamlet name: deployed 'cart-router', process declares 'cart-auditor'")
      ),
      problems.toString
    )
    assert(
      problems.exists(
        _.contains("outlet 'review' is deployed but the process does not declare it")
      ),
      problems.toString
    )
  }

  test("an outlet declared twice is refused") {
    val d = deployed.getStreamlet
    val (code, problems) =
      refused(deployed.withStreamlet(d.withOutlets(d.outlets :+ d.outlets.head)))
    assertEquals(code, 1)
    assert(problems.exists(_.contains("is declared 2 times")), problems.toString)
  }

  test("a stalled partition is warned once, after the threshold, and clears on commit") {
    val now = new AtomicLong(0L)
    val clock = new Clock:
      def getZone: java.time.ZoneId                     = ZoneOffset.UTC
      override def withZone(z: java.time.ZoneId): Clock = this
      override def instant: Instant                     = Instant.ofEpochMilli(now.get)
    val events = new CapturingEventSink
    val stalls = new Stalls(5.minutes, events, clock)
    stalls.sent("in", 2)
    now.set(4.minutes.toMillis)
    stalls.check((_, _) => "cart.router")
    assert(events.warnings.isEmpty)
    stalls.sent("in", 2) // a redelivery does not reset the clock
    now.set(5.minutes.toMillis + 1)
    stalls.lastError = "the record said fail"
    stalls.check((_, _) => "cart.router")
    stalls.check((_, _) => "cart.router")
    assertEquals(events.warnings.size, 1)
    val (reason, note) = events.warnings.peek()
    assertEquals(reason, "PartitionStalled")
    assert(note.contains("inlet 'in' partition 2") && note.contains("the record said fail"), note)
    assertEquals(stalls.stalledSeconds("in", 2), 300L)
    stalls.committed("in", 2)
    assertEquals(stalls.stalledSeconds("in", 2), 0L)
  }

class KubernetesEventSinkSuite extends munit.FunSuite:
  test("the event regards the sidecar's own pod, as a Warning with the reason and note") {
    val sink =
      new KubernetesEventSink(Settings.Pod("flow-cart-router-abc", "shop", "10.0.0.1", 443))
    val body = com.thinkmorestupidless.ankka.flow.protocol.Json
      .parse(
        sink.body(
          "PartitionStalled",
          "inlet 'in' partition 2 …",
          java.time.Instant.parse("2026-09-28T10:00:00Z")
        )
      )
      .toOption
      .get
    assertEquals(
      body.field("reason"),
      Some(com.thinkmorestupidless.ankka.flow.protocol.Json.Str("PartitionStalled"))
    )
    assertEquals(
      body.field("type"),
      Some(com.thinkmorestupidless.ankka.flow.protocol.Json.Str("Warning"))
    )
    assertEquals(
      body.field("regarding").flatMap(_.field("name")),
      Some(com.thinkmorestupidless.ankka.flow.protocol.Json.Str("flow-cart-router-abc"))
    )
    assertEquals(
      body.field("eventTime"),
      Some(com.thinkmorestupidless.ankka.flow.protocol.Json.Str("2026-09-28T10:00:00.000000Z"))
    )
  }
