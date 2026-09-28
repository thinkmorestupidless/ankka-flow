package com.thinkmorestupidless.ankka.flow.sidecar

import java.util.concurrent.TimeUnit

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.*
import scala.util.Try

import io.grpc.ManagedChannel
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder

/** Every rule in contracts/protocol.md, against the double, with no Kafka. */
class ConversationSuite extends munit.FunSuite:

  import TestSpecs.*

  private val double                  = new ProcessDouble(fixture("conformance"))
  private var channel: ManagedChannel = scala.compiletime.uninitialized

  override def beforeAll(): Unit =
    val port = double.start()
    channel = NettyChannelBuilder.forAddress("127.0.0.1", port).usePlaintext().build()

  override def afterAll(): Unit =
    channel.shutdownNow()
    double.close()

  override def beforeEach(context: BeforeEach): Unit =
    double.behaviour = ProcessDouble.keyed()
    double.silent = false
    double.received.clear()

  private def open(config: String = """{"factor":1,"mode":"echo"}""") =
    Conversation.open(
      channel,
      "p",
      "s",
      config,
      Seq("in"    -> "t-in", "side"   -> "t-side"),
      Seq("other" -> "t-other", "out" -> "t-out")
    )

  private def await[A](f: Future[A]): A = Await.result(f, 5.seconds)

  private def batch(partition: Int, records: ankka.flow.v1.streamlet.InputRecord*) =
    InputBatch("in", partition, records.toVector)

  test("Start is the first message, carrying the conversation id, bindings and config") {
    val c     = open()
    val _     = await(c.process(batch(0, input(0, "echo"))))
    val first = double.received.peek()
    assert(first.message.isStart, first.toString)
    val s = first.getStart
    assertEquals(s.conversationId, c.start.conversationId)
    assertEquals(s.configJson, """{"factor":1,"mode":"echo"}""")
    assertEquals(s.inlets.map(_.port), Seq("in", "side"))
    assertEquals(s.maxMessageBytes, Conversation.MaxMessageBytes)
    c.stop("done")
  }

  test("an echo keeps key, header order, binary header values and the value bytes") {
    val c   = open()
    val raw = Array[Byte](0, -1, 42, 127, -128)
    val in =
      input(7, "echo", """{"total":3}""", Seq("ce_type" -> "ItemAdded".getBytes, "raw" -> raw))
    val Outcome.Acked(emits) = await(c.process(batch(0, in))): @unchecked
    assertEquals(emits.size, 1)
    assertEquals(emits.head.outlet, "out")
    val r = emits.head.record
    assertEquals(r.getKey.toStringUtf8, "echo")
    assertEquals(r.headers.map(_.key), Seq("ce_type", "raw"))
    assertEquals(r.headers(1).value.toByteArray.toSeq, raw.toSeq)
    assertEquals(r.value.toStringUtf8, """{"total":3}""")
    c.stop("done")
  }

  test("fan-out emits to both outlets, in order, before the ack") {
    val c                    = open()
    val Outcome.Acked(emits) = await(c.process(batch(0, input(0, "fan")))): @unchecked
    assertEquals(emits.map(_.outlet), Vector("out", "other"))
    c.stop("done")
  }

  test("skipping is acking without emitting") {
    val c = open()
    assertEquals(await(c.process(batch(0, input(0, "skip")))), Outcome.Acked(Vector.empty))
    assert(!c.failed.isCompleted)
    c.stop("done")
  }

  test("config reaches the process: multiply emits factor times") {
    val c                    = open("""{"factor":3,"mode":"echo"}""")
    val Outcome.Acked(emits) = await(c.process(batch(0, input(0, "multiply")))): @unchecked
    assertEquals(emits.size, 3)
    c.stop("done")
  }

  test("batches of different partitions interleave: a slow partition does not hold up another") {
    val c    = open()
    val slow = c.process(batch(0, input(0, "late")))
    val fast = c.process(batch(1, input(0, "echo")))
    await(fast): Unit
    assert(!slow.isCompleted, "the slow batch completed before the fast one")
    await(slow): Unit
    c.stop("done")
  }

  test("a Fail fails the conversation and every batch in flight") {
    val c       = open()
    val pending = c.process(batch(1, input(0, "late")))
    val failed  = c.process(batch(0, input(0, "fail")))
    assert(Try(await(failed)).failed.get.getMessage.contains("the record said fail"))
    assert(await(c.failed).getMessage.contains("failed batch"))
    assert(Try(await(pending)).isFailure)
    // nothing more can be sent on a failed conversation
    assert(Try(await(c.process(batch(2, input(0, "echo"))))).isFailure)
  }

  Seq(
    "rogue-outlet"   -> "emit to outlet 'nope'",
    "double-ack"     -> "not in flight",
    "emit-after-ack" -> "not in flight",
    "unknown-batch"  -> "never sent"
  ).foreach { (key, expected) =>
    test(s"violation: $key fails the stream") {
      val c = open()
      Try(await(c.process(batch(0, input(0, key))))): Unit
      val cause = await(c.failed).getMessage
      assert(cause.contains("protocol violation") && cause.contains(expected), cause)
    }
  }

  test("a revoked batch completes as Revoked, and its late ack is dropped silently") {
    val c    = open()
    val slow = c.process(batch(3, input(0, "late")))
    Thread.sleep(50)
    c.revoke("in", 3, generation = 9) // another substream of partition 3: untouched
    Thread.sleep(50)
    assert(!slow.isCompleted, "a revocation of another substream generation revoked this batch")
    c.revoke("in", 3, generation = 0)
    assertEquals(await(slow), Outcome.Revoked)
    Thread.sleep(400) // the late emit and ack arrive now
    assert(!c.failed.isCompleted, s"failed: ${c.failed.value}")
    assertEquals(await(c.process(batch(3, input(1, "echo")))).getClass, classOf[Outcome.Acked])
    c.stop("done")
  }

  test("Stop is delivered and the stream completes without a failure being logged as one") {
    val c = open()
    await(c.process(batch(0, input(0, "echo")))): Unit
    c.stop("test over")
    assert(await(c.failed).getMessage.contains("stopped"))
    Thread.sleep(200)
    assert(double.stops.map(_.reason).contains("test over"), double.stops.toString)
  }

  test("the process going away fails the conversation as unreachable") {
    val other = new ProcessDouble(fixture("conformance"))
    val port  = other.start()
    val ch    = NettyChannelBuilder.forAddress("127.0.0.1", port).usePlaintext().build()
    try
      val c = Conversation.open(ch, "p", "s", "{}", Seq("in" -> "t"), Seq("out" -> "u"))
      await(c.process(InputBatch("in", 0, Vector(input(0, "echo"))))): Unit
      other.stop()
      assert(await(c.failed).getMessage.contains("unreachable"))
    finally
      ch.shutdownNow().awaitTermination(2, TimeUnit.SECONDS): Unit
      other.close()
  }
