package com.thinkmorestupidless.ankka.flow.sidecar

import scala.concurrent.duration.*

/**
 * The edge cases of restarts: the process restarted while the sidecar runs, and the sidecar
 * restarted while the process runs. Readiness follows the process; nothing is lost either way.
 */
class RestartKafkaSuite extends KafkaSuite:

  private def ids(topic: String, count: Int) =
    consumeAll(topic, count, 10.seconds).map(r => str(r.value).toInt)

  test(
    "not ready until the process answers; unready within 2 s of it going away; resumes with a new conversation"
  ) {
    val in     = createTopic(uniqueTopic("restart-in"), 2)
    val out    = createTopic(uniqueTopic("restart-out"), 2)
    val double = new ProcessDouble(TestSpecs.fixture("minimal"), ProcessDouble.keyed())
    val port   = double.start()
    double.stop() // the process is not running when the sidecar starts (S1.5)
    val sidecar = new SidecarRun(
      TestSpecs.fixture("minimal"),
      SidecarRun.conf("r", "s", bootstrap, Seq("in" -> in), Seq("out" -> out), maxRecords = 3),
      port
    )
    try
      Thread.sleep(2000)
      assert(!sidecar.ready, "ready before the process answered")
      double.start(port)
      eventually()(assert(sidecar.ready))

      publish(in, (0 until 30).map(i => (Some(s"k${i % 5}"), s"$i", Nil)))
      eventually()(assertEquals(committed("r.s.in"), 30L))

      double.stop()
      val gone = System.nanoTime
      eventually(5.seconds, 50.millis)(assert(!sidecar.ready, "still ready with the process gone"))
      assert((System.nanoTime - gone).nanos < 2.seconds, "took more than 2 s to become unready")

      publish(in, (30 until 60).map(i => (Some(s"k${i % 5}"), s"$i", Nil)))
      Thread.sleep(1000)
      double.start(port)
      eventually(30.seconds)(assert(sidecar.ready))
      eventually(30.seconds)(assertEquals(committed("r.s.in"), 60L))
      assertEquals(ids(out, 60).toSet, (0 until 60).toSet)
      assert(double.starts.map(_.conversationId).distinct.size >= 2)
    finally
      sidecar.stop(): Unit
      double.close()
  }

  test("an idle conversation outlives a process that keeps gRPC's default keepalive policy") {
    // gRPC servers allow a client one ping per five minutes by default and end the connection with
    // GOAWAY `too_many_pings` after a few more; the process double's server keeps grpc-java's
    // defaults, as Python's does. A sidecar that pinged a quiet stream every few seconds failed it
    // about every half a minute, and every failure revoked and redelivered the inlet's partitions.
    val in     = createTopic(uniqueTopic("idle-in"), 1)
    val out    = createTopic(uniqueTopic("idle-out"), 1)
    val double = new ProcessDouble(TestSpecs.fixture("minimal"), ProcessDouble.keyed())
    val port   = double.start()
    val sidecar = new SidecarRun(
      TestSpecs.fixture("minimal"),
      SidecarRun.conf("r", "i", bootstrap, Seq("in" -> in), Seq("out" -> out)),
      port
    )
    try
      eventually()(assert(sidecar.ready))
      Thread.sleep(45000) // three pings' worth at the old ten-second interval, and a strike more
      assert(sidecar.ready, "not ready after an idle spell")
      assertEquals(
        double.starts.map(_.conversationId).distinct.size,
        1,
        "the conversation was restarted"
      )
      publish(in, Seq((Some("k"), "0", Nil)))
      eventually()(assertEquals(committed("r.i.in"), 1L))
    finally
      sidecar.stop(): Unit
      double.close()
  }

  test("a sidecar restarted against the same group resumes from the last commit") {
    val in     = createTopic(uniqueTopic("resume-in"), 1)
    val out    = createTopic(uniqueTopic("resume-out"), 1)
    val double = new ProcessDouble(TestSpecs.fixture("minimal"), ProcessDouble.keyed())
    val port   = double.start()
    val conf   = SidecarRun.conf("r", "t", bootstrap, Seq("in" -> in), Seq("out" -> out))
    try
      val first = new SidecarRun(TestSpecs.fixture("minimal"), conf, port)
      eventually()(assert(first.ready))
      publish(in, (0 until 10).map(i => (Some("k"), s"$i", Nil)))
      eventually()(assertEquals(committed("r.t.in"), 10L))
      assertEquals(first.stop(), 0)
      assert(double.stops.nonEmpty, "the process was not told to stop")

      publish(in, (10 until 20).map(i => (Some("k"), s"$i", Nil)))
      val second = new SidecarRun(TestSpecs.fixture("minimal"), conf, port)
      eventually()(assertEquals(committed("r.t.in"), 20L))
      val seen = double.batches.flatMap(_.records.map(_.offset))
      // The second sidecar started from offset 10: nothing below it was sent again.
      assertEquals(seen.count(_ < 10), 10, s"offsets sent: $seen")
      assertEquals(ids(out, 20), (0 until 20).toVector)
      assertEquals(second.stop(), 0)
    finally double.close()
  }

  test("never ready while an inlet's topic does not exist; ready once it does") {
    val in     = uniqueTopic("missing-in") // not created yet
    val out    = createTopic(uniqueTopic("missing-out"), 1)
    val double = new ProcessDouble(TestSpecs.fixture("minimal"), ProcessDouble.keyed())
    val port   = double.start()
    val sidecar = new SidecarRun(
      TestSpecs.fixture("minimal"),
      SidecarRun.conf("r", "m", bootstrap, Seq("in" -> in), Seq("out" -> out)),
      port
    )
    try
      eventually()(assert(double.starts.nonEmpty, "the conversation never started"))
      Thread.sleep(4000)
      assert(!sidecar.ready, "ready with the input topic missing")
      createTopic(in, 1)
      eventually(60.seconds)(assert(sidecar.ready, "not ready after the topic was created"))
    finally
      sidecar.stop(): Unit
      double.close()
  }
