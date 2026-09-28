package com.thinkmorestupidless.ankka.flow.sidecar.conformance

import java.nio.file.Files
import java.util.Base64
import java.util.concurrent.TimeUnit

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.*
import scala.util.Try

import ankka.flow.v1.discovery.{DiscoveryGrpc, SidecarInfo}
import ankka.flow.v1.payload.{Header, Problem, Problems, Record}
import ankka.flow.v1.streamlet.InputRecord
import com.google.protobuf.ByteString
import com.thinkmorestupidless.ankka.flow.protocol.{DescriptorValidation, Json, ProtocolVersion}
import com.thinkmorestupidless.ankka.flow.sidecar.*
import io.grpc.ManagedChannel
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder

/**
 * What a compatible SDK is (FR-026, contracts/conformance.md): every conversation the protocol
 * defines, driven through the sidecar's own `Conversation` against a target, each case named so a
 * failing SDK learns exactly which conversation broke. The `violation.*` and `version.*` cases
 * prove the sidecar's side against the scriptable double; a correct SDK cannot produce them.
 */
class ConformanceSuite extends munit.FunSuite:

  override val munitTimeout: FiniteDuration = 1.minute

  private var target: ConformanceTarget = scala.compiletime.uninitialized
  private var channel: ManagedChannel   = scala.compiletime.uninitialized

  private val only = sys.props.get("flow.conformance.only").filter(_.nonEmpty)

  override def munitTests(): Seq[Test] =
    super.munitTests().filter(t => only.forall(o => t.name.startsWith(o)))

  override def beforeAll(): Unit =
    target = ConformanceTarget.fromProperties()
    println(s"conformance target: ${target.name}")
    channel = NettyChannelBuilder
      .forAddress(target.host, target.port)
      .usePlaintext()
      .maxInboundMessageSize(2 * Conversation.MaxMessageBytes)
      .build()

  override def afterAll(): Unit =
    channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS): Unit
    target.close()

  // ── fixtures ────────────────────────────────────────────────────────────────────────────

  private final case class Script(config: String, batches: Vector[InputBatch])

  private def script(name: String): Script =
    val text = new String(
      Files.readAllBytes(TestSpecs.repoRoot.resolve(s"protocol/fixtures/conversations/$name.json")),
      "UTF-8"
    )
    val json                    = Json.parse(text).fold(e => fail(s"$name: $e"), identity)
    def str(j: Json, k: String) = j.field(k).collect { case Json.Str(s) => s }
    def num(j: Json, k: String) = j.field(k).collect { case Json.Num(n) => n.toLong }.getOrElse(0L)
    def arr(j: Json, k: String) =
      j.field(k).collect { case Json.Arr(xs) => xs }.getOrElse(Vector.empty)
    val batches = arr(json, "batches").map { b =>
      InputBatch(
        str(b, "inlet").get,
        num(b, "partition").toInt,
        arr(b, "records").map { r =>
          val value = r.field("value_repeat") match
            case Some(rep) =>
              ByteString.copyFrom(
                Array.fill(num(rep, "count").toInt)(str(rep, "byte").get.head.toByte)
              )
            case None => ByteString.copyFrom(Base64.getDecoder.decode(str(r, "value_base64").get))
          InputRecord(
            num(r, "offset"),
            0L,
            Some(
              Record(
                key = str(r, "key").map(ByteString.copyFromUtf8),
                headers = arr(r, "headers").map(h =>
                  Header(
                    str(h, "key").get,
                    ByteString.copyFrom(Base64.getDecoder.decode(str(h, "value_base64").get))
                  )
                ),
                value = value
              )
            )
          )
        }
      )
    }
    Script(json.field("config").map(Json.compact).getOrElse("{}"), batches)

  private def open(config: String, ch: ManagedChannel = channel) =
    Conversation.open(
      ch,
      "conformance",
      "conformance",
      config,
      Seq("in"    -> "t-in", "side"   -> "t-side"),
      Seq("other" -> "t-other", "out" -> "t-out")
    )

  private def await[A](f: Future[A], within: FiniteDuration = 20.seconds): A =
    Await.result(f, within)

  /** Runs a scripted case's batches, one after another, and returns each outcome. */
  private def run(name: String): (Conversation, Vector[Outcome]) =
    val s = script(name)
    val c = open(s.config)
    val outcomes = s.batches.map(b =>
      Try(await(c.process(b))).fold(e => fail(s"$name: ${e.getMessage}"), identity)
    )
    (c, outcomes)

  private def emits(o: Outcome): Vector[EmittedRecord] = o match
    case Outcome.Acked(e) => e
    case other            => fail(s"expected an ack, got $other")

  private def settleThenHealthy(c: Conversation): Unit =
    Thread.sleep(300) // anything the process sends after the ack arrives now
    assert(!c.failed.isCompleted, s"the conversation failed: ${c.failed.value}")
    c.stop("case over")

  private def onlyAgainstDouble(): Unit =
    assume(!target.external, "skipped (double-only): a correct SDK cannot produce this")

  // ── discovery ───────────────────────────────────────────────────────────────────────────

  test("discovery.answers-spec") {
    val spec = DiscoveryGrpc
      .blockingStub(channel)
      .withDeadlineAfter(10, TimeUnit.SECONDS)
      .discover(SidecarInfo("1.0", "conformance"))
    assertEquals(ProtocolVersion.parse(spec.protocolVersion).map(_.major), Right(1))
    assertEquals(DescriptorValidation.validate(spec), Vector.empty)
    val expected = TestSpecs.fixture("conformance").getStreamlet
    assertEquals(DescriptorValidation.compare(expected, spec.getStreamlet), Vector.empty)
  }

  test("discovery.reports-error") {
    DiscoveryGrpc
      .blockingStub(channel)
      .withDeadlineAfter(10, TimeUnit.SECONDS)
      .reportError(
        Problems(Seq(Problem("conformance: first problem"), Problem("conformance: second problem")))
      ): Unit
  }

  // ── run ─────────────────────────────────────────────────────────────────────────────────

  test("run.start-then-silence") {
    val c = open("""{"factor":1,"mode":"echo"}""")
    Thread.sleep(500)
    assert(!c.failed.isCompleted, s"the process sent something unasked: ${c.failed.value}")
    c.stop("case over")
  }

  test("run.echo-preserves-record") {
    val (c, Vector(o)) = run("run.echo-preserves-record"): @unchecked
    val Vector(e)      = emits(o): @unchecked
    val in             = script("run.echo-preserves-record").batches.head.records.head.getRecord
    assertEquals(e.outlet, "out")
    assertEquals(e.record.key, in.key)
    assertEquals(e.record.headers, in.headers)
    assertEquals(e.record.value, in.value)
    settleThenHealthy(c)
  }

  test("run.fan-out") {
    val (c, Vector(o)) = run("run.fan-out"): @unchecked
    assertEquals(emits(o).map(_.outlet), Vector("out", "other"))
    settleThenHealthy(c)
  }

  test("run.skip-acks-without-emit") {
    val (c, Vector(o)) = run("run.skip-acks-without-emit"): @unchecked
    assertEquals(emits(o), Vector.empty)
    settleThenHealthy(c)
  }

  test("run.fail-sends-fail") {
    val s      = script("run.fail-sends-fail")
    val c      = open(s.config)
    val failed = Try(await(c.process(s.batches.head))).failed.toOption
    assert(failed.exists(_.getMessage.contains("failed batch")), s"expected a Fail, got $failed")
    assert(await(c.failed).getMessage.contains("failed batch"))
  }

  test("run.emits-precede-ack") {
    val (c, Vector(o)) = run("run.emits-precede-ack"): @unchecked
    assertEquals(emits(o).size, 5)
    settleThenHealthy(c)
  }

  test("run.two-partitions-interleave") {
    val s    = script("run.two-partitions-interleave")
    val c    = open(s.config)
    val slow = c.process(s.batches(0))
    val fast = c.process(s.batches(1))
    await(fast): Unit
    assert(!slow.isCompleted, "partition 0's slow batch held up partition 1")
    emits(await(slow)): Unit
    settleThenHealthy(c)
  }

  test("run.two-inlets") {
    val (c, outcomes) = run("run.two-inlets")
    assertEquals(outcomes.map(emits(_).size), Vector(1, 1))
    settleThenHealthy(c)
  }

  test("run.batch-order-within-partition") {
    val (c, outcomes) = run("run.batch-order-within-partition")
    assertEquals(
      outcomes.map(o => emits(o).head.record.value.toStringUtf8),
      Vector("""{"n":0}""", """{"n":1}""", """{"n":2}""")
    )
    settleThenHealthy(c)
  }

  test("run.config-applied") {
    val (c, Vector(o)) = run("run.config-applied"): @unchecked
    assertEquals(emits(o).size, 3)
    settleThenHealthy(c)
  }

  test("run.unkeyed-emit") {
    val (c, Vector(o)) = run("run.unkeyed-emit"): @unchecked
    assertEquals(
      emits(o).map(_.record.key),
      Vector(None),
      "a keyless emit carries no key, not an empty one"
    )
    settleThenHealthy(c)
  }

  test("run.header-order") {
    val (c, Vector(o)) = run("run.header-order"): @unchecked
    val in             = script("run.header-order").batches.head.records.head.getRecord.headers
    assertEquals(emits(o).head.record.headers, in.reverse)
    settleThenHealthy(c)
  }

  test("run.large-record") {
    val (c, Vector(o)) = run("run.large-record"): @unchecked
    assertEquals(emits(o).head.record.value.size, 3 * 1024 * 1024)
    settleThenHealthy(c)
  }

  test("run.rogue-outlet") {
    val s = script("run.rogue-outlet")
    val c = open(s.config)
    Try(await(c.process(s.batches.head))): Unit
    // Either the SDK refused the emit and failed the batch, or it emitted and the sidecar refused it.
    val cause = await(c.failed).getMessage
    assert(cause.contains("failed batch") || cause.contains("does not declare"), cause)
  }

  test("run.stop-completes") {
    val c = open("""{"factor":1,"mode":"echo"}""")
    emits(await(c.process(script("run.echo-preserves-record").batches.head))): Unit
    c.stop("case over")
    assert(await(c.failed).getMessage.contains("stopped"))
  }

  test("run.reconnect-new-conversation") {
    val first = open("""{"factor":1,"mode":"echo"}""")
    emits(await(first.process(script("run.fan-out").batches.head))): Unit
    first.fail(new StreamFailed("the sidecar reconnects"))
    val second = open("""{"factor":1,"mode":"echo"}""")
    assertNotEquals(second.start.conversationId, first.start.conversationId)
    assertEquals(
      emits(await(second.process(script("run.echo-preserves-record").batches.head))).size,
      1
    )
    settleThenHealthy(second)
  }

  // ── the sidecar's side, against the double ─────────────────────────────────────────────

  private def withDouble[A](f: (ProcessDouble, ManagedChannel) => A): A =
    val double = ConformanceReference.start()
    val ch     = NettyChannelBuilder.forAddress("127.0.0.1", double.port).usePlaintext().build()
    try f(double, ch)
    finally
      ch.shutdownNow()
      double.close()

  Seq(
    "double-ack"     -> "not in flight",
    "emit-after-ack" -> "not in flight",
    "unknown-batch"  -> "never sent"
  ).foreach { (key, expected) =>
    test(s"violation.${key}") {
      onlyAgainstDouble()
      withDouble { (_, ch) =>
        val c = open("{}", ch)
        Try(await(c.process(InputBatch("in", 0, Vector(TestSpecs.input(0, key)))))): Unit
        val cause = await(c.failed).getMessage
        assert(cause.contains("protocol violation") && cause.contains(expected), cause)
      }
    }
  }

  test("version.refuses-other-major") {
    onlyAgainstDouble()
    assert(
      ProtocolVersion
        .compatible(ProtocolVersion.Current, "2.0")
        .left
        .exists(m => m.contains("'2.0'") && m.contains("'1.0'"))
    )
  }

  test("version.accepts-earlier-minor") {
    onlyAgainstDouble()
    assertEquals(ProtocolVersion.compatible(ProtocolVersion(1, 1), "1.0"), Right(()))
  }
