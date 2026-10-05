package com.thinkmorestupidless.ankka.flow.sidecar

import java.nio.file.{Files, Path}

import scala.concurrent.Await
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import ch.qos.logback.classic.Logger as LogbackLogger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.thinkmorestupidless.ankka.flow.protocol.Builtins
import org.apache.pekko.actor.ActorSystem
import org.slf4j.LoggerFactory

/**
 * The merge stage alone against a real Neo4j, no Kafka (quickstart tier 2): every row of the state
 * table in data-model.md, folding, malformed records, value types, and the refusals at open.
 */
class Neo4jMergeSuite extends Neo4jSuite:

  given system: ActorSystem = ActorSystem("neo4j-merge-suite")

  override def afterAll(): Unit =
    system.terminate(): Unit
    super.afterAll()

  private val builtin = Descriptor(Builtins.neo4jMergeSink)

  private def credentials(password: String = AdminPassword): Path =
    val d = Files.createTempDirectory("neo4j-creds")
    Files.writeString(d.resolve("uri"), boltUri)
    Files.writeString(d.resolve("username"), "neo4j")
    Files.writeString(d.resolve("password"), password)
    d

  private def conf(dir: Path) =
    StreamletConfig
      .parseString(s"""
        flow {
          pipeline = graphs, streamlet = graph
          config { secret = neo4j-test, transaction-timeout = 10s }
          stage { name = neo4j-merge-sink, neo4j { credentials-dir = "$dir" } }
          inlets { in { topic = deltas, bootstrap.servers = "unused:9092" } }
        }""")
      .fold(e => throw new IllegalStateException(e.mkString), identity)

  private final case class Opened(
      stage: Neo4jMergeStage,
      metrics: StageMetrics,
      events: CapturingEventSink
  ):
    private val run = stage.open(() => false).get.toOption.get

    /** Each delta under its own element key, as a correct writer sends it. */
    private def records(values: Seq[String]) =
      values.toVector.zipWithIndex.map { (v, i) =>
        val key = Deltas.parse(i.toLong, v).toOption.map(Deltas.key).getOrElse(s"k$i")
        TestSpecs.keyed(i.toLong, Some(key), v)
      }
    def apply(values: String*): Unit =
      Await.result(run.processor.process(InputBatch("in", 0, records(values))), 30.seconds): Unit
    def attempt(values: String*): Either[Throwable, Unit] =
      attemptKeyed(records(values))

    /** Records exactly as given: any key, no key, or no value. */
    def attemptKeyed(
        records: Vector[ankka.flow.v1.streamlet.InputRecord]
    ): Either[Throwable, Unit] =
      scala.util
        .Try(Await.result(run.processor.process(InputBatch("in", 0, records)), 30.seconds))
        .toEither
        .map(_ => ())

  private def open(): Opened =
    clear()
    val metrics = new StageMetrics
    val events  = new CapturingEventSink
    Opened(new Neo4jMergeStage(builtin, conf(credentials()), events, metrics), metrics, events)

  private def node(id: String, v: Long, labels: Seq[String] = Seq("Cart"), props: String = "{}") =
    s"""{"kind":"node","id":"$id","version":$v,"labels":[${labels
        .map(l => s""""$l"""")
        .mkString(",")}],"properties":$props}"""

  private def edge(id: String, v: Long, from: String, to: String, props: String = "{}") =
    s"""{"kind":"edge","id":"$id","version":$v,"type":"LINKS","from":"$from","to":"$to","properties":$props}"""

  private def nodeTomb(id: String, v: Long) =
    s"""{"kind":"tombstone","element":"node","id":"$id","version":$v}"""

  private def edgeTomb(id: String, v: Long, from: String, to: String) =
    s"""{"kind":"tombstone","element":"edge","id":"$id","version":$v,"type":"LINKS","from":"$from","to":"$to"}"""

  private def nodeRow(id: String): Map[String, AnyRef] =
    query(
      "MATCH (n:Element {id: $id}) RETURN labels(n) AS labels, properties(n) AS props",
      Map("id" -> id)
    ).head

  private def labels(id: String) =
    nodeRow(id)("labels").asInstanceOf[java.util.List[String]].asScala.toSet

  private def props(id: String) =
    nodeRow(id)("props").asInstanceOf[java.util.Map[String, AnyRef]].asScala.toMap

  test("a node delta creates the node with its labels, properties and version") {
    val o = open()
    o(node("cart:1", 5, Seq("Cart", "Basket"), """{"cartId":"1","items":3}"""))
    assertEquals(labels("cart:1"), Set("Element", "Cart", "Basket"))
    assertEquals(
      props("cart:1"),
      Map[String, AnyRef](
        "id"       -> "cart:1",
        "_version" -> Long.box(5),
        "cartId"   -> "1",
        "items"    -> Long.box(3)
      )
    )
    assertEquals(o.metrics.of("in", 0).getDeltasWritten, 1L)
  }

  test("a higher version replaces labels and properties, removing what it no longer carries") {
    val o = open()
    o(node("cart:1", 5, Seq("Cart", "Basket"), """{"cartId":"1","items":3}"""))
    o(node("cart:1", 6, Seq("Order"), """{"cartId":"1"}"""))
    assertEquals(labels("cart:1"), Set("Element", "Order"))
    assertEquals(
      props("cart:1"),
      Map[String, AnyRef]("id" -> "cart:1", "_version" -> Long.box(6), "cartId" -> "1")
    )
  }

  test("an equal or lower version is stale and changes nothing") {
    val o = open()
    o(node("cart:1", 5, props = """{"v":"five"}"""))
    o(node("cart:1", 5, props = """{"v":"five again"}"""))
    o(node("cart:1", 4, props = """{"v":"four"}"""))
    assertEquals(props("cart:1")("v"), "five")
    assertEquals(o.metrics.of("in", 0).getDeltasWritten, 1L)
    assertEquals(o.metrics.of("in", 0).getDeltasStale, 2L)
  }

  test("an edge creates placeholder endpoints that the nodes' own deltas replace") {
    val o = open()
    o(edge("e:1", 1, "a", "b", """{"since":2026}"""))
    assertEquals(props("a"), Map[String, AnyRef]("id" -> "a", "_version" -> Long.box(-1)))
    assertEquals(labels("a"), Set("Element"))
    o(node("a", 1, Seq("Cart"), """{"name":"a"}"""))
    assertEquals(props("a")("name"), "a")
    val edges = query(
      "MATCH (:Element {id:'a'})-[r:LINKS]->(:Element {id:'b'}) RETURN r.id AS id, r.since AS since, r._version AS v"
    )
    assertEquals(
      edges,
      Vector(Map[String, AnyRef]("id" -> "e:1", "since" -> Long.box(2026), "v" -> Long.box(1)))
    )
  }

  test(
    "an edge is merged once however often it arrives, and a higher version replaces its properties"
  ) {
    val o = open()
    o(edge("e:1", 1, "a", "b", """{"w":1}"""))
    o(edge("e:1", 1, "a", "b", """{"w":1}"""), edge("e:1", 2, "a", "b", """{"w":2}"""))
    val edges = query("MATCH ()-[r:LINKS {id:'e:1'}]->() RETURN r.w AS w")
    assertEquals(edges, Vector(Map[String, AnyRef]("w" -> Long.box(2))))
  }

  test(
    "a tombstone marks the node and clears it; a lower merge after it is stale; a higher one revives it"
  ) {
    val o = open()
    o(node("cart:1", 1, props = """{"cartId":"1"}"""))
    o(nodeTomb("cart:1", 2))
    assertEquals(props("cart:1")("_deleted"), java.lang.Boolean.TRUE)
    assertEquals(props("cart:1").get("cartId"), None)
    o(node("cart:1", 2, props = """{"cartId":"stale"}"""))
    assertEquals(props("cart:1").get("cartId"), None)
    assertEquals(props("cart:1")("_deleted"), java.lang.Boolean.TRUE)
    o(node("cart:1", 3, props = """{"cartId":"1"}"""))
    assertEquals(props("cart:1").get("_deleted"), None)
    assertEquals(props("cart:1")("cartId"), "1")
  }

  test(
    "a tombstone for an element never seen creates it marked, so an older merge cannot revive it"
  ) {
    val o = open()
    o(nodeTomb("ghost", 10))
    o(node("ghost", 9, props = """{"late":true}"""))
    assertEquals(
      props("ghost"),
      Map[String, AnyRef](
        "id"       -> "ghost",
        "_version" -> Long.box(10),
        "_deleted" -> java.lang.Boolean.TRUE
      )
    )
  }

  test("an edge tombstone marks the edge and clears its properties") {
    val o = open()
    o(edge("e:1", 1, "a", "b", props = """{"since":1843}"""))
    o(edgeTomb("e:1", 2, "a", "b"))
    assertEquals(
      query("MATCH ()-[r:LINKS {id:'e:1'}]->() RETURN properties(r) AS p").head("p"),
      java.util.Map.of("id", "e:1", "_version", Long.box(2), "_deleted", java.lang.Boolean.TRUE)
    )
  }

  private val bareMarker = Map[String, AnyRef](
    "id"       -> "cart:9",
    "_version" -> Long.box(2),
    "_deleted" -> java.lang.Boolean.TRUE
  )

  test("a tombstone leaves the same bare marker whether it follows the node in one batch or two") {
    val two = open()
    two(node("cart:9", 1, labels = Seq("Cart"), props = """{"cartId":"9"}"""))
    two(nodeTomb("cart:9", 2))
    assertEquals(props("cart:9"), bareMarker)
    assertEquals(
      query("MATCH (n:Element {id:'cart:9'}) RETURN labels(n) AS l").head("l"),
      java.util.List.of("Element")
    )

    query("MATCH (n) DETACH DELETE n")
    val one = open()
    one(
      node("cart:9", 1, labels = Seq("Cart"), props = """{"cartId":"9"}"""),
      nodeTomb("cart:9", 2)
    )
    assertEquals(props("cart:9"), bareMarker)
    assertEquals(
      query("MATCH (n:Element {id:'cart:9'}) RETURN labels(n) AS l").head("l"),
      java.util.List.of("Element")
    )

    // A later merge brings it back whole, with nothing of the old state under it.
    one(node("cart:9", 3, labels = Seq("Cart"), props = """{"cartId":"9","again":true}"""))
    assertEquals(props("cart:9").get("_deleted"), None)
    assertEquals(props("cart:9")("again"), java.lang.Boolean.TRUE)
  }

  test("a batch is folded: two versions of one element in one batch write once") {
    val o = open()
    o(
      node("n", 1, props = """{"v":1}"""),
      node("n", 3, props = """{"v":3}"""),
      node("n", 2, props = """{"v":2}""")
    )
    assertEquals(props("n")("v"), Long.box(3))
    assertEquals(o.metrics.of("in", 0).getDeltasWritten, 1L)
    assertEquals(o.metrics.of("in", 0).getDeltasStale, 2L)
  }

  test("one malformed record fails the whole batch, naming its offset, and writes nothing") {
    val o       = open()
    val result  = o.attempt(node("n", 1), """{"kind":"nod","id":"m","version":1}""")
    val message = result.left.toOption.map(_.getMessage).getOrElse("")
    assert(message.contains("offset 1: unknown kind 'nod'"), message)
    assert(message.contains("inlet 'in' partition 0"), message)
    assertEquals(query("MATCH (n) RETURN count(n) AS c").head("c"), Long.box(0))
    assertEquals(o.metrics.of("in", 0).getBatchesFailed, 1L)
  }

  test("strings, integers, floats, booleans and arrays round-trip") {
    val o = open()
    o(
      node(
        "n",
        1,
        props = """{"s":"x","i":9007199254740993,"f":1.25,"b":false,"xs":["a","b"],"ns":[1,2]}"""
      )
    )
    val p = props("n")
    assertEquals(p("s"), "x")
    assertEquals(p("i"), Long.box(9007199254740993L))
    assertEquals(p("f"), Double.box(1.25))
    assertEquals(p("b"), java.lang.Boolean.FALSE)
    assertEquals(p("xs").asInstanceOf[java.util.List[AnyRef]].asScala.toList, List("a", "b"))
    assertEquals(
      p("ns").asInstanceOf[java.util.List[AnyRef]].asScala.toList,
      List(Long.box(1), Long.box(2))
    )
  }

  test("opening creates the element_id uniqueness constraint, idempotently") {
    open()
    open().stage.open(() => false): Unit
    val names = query("SHOW CONSTRAINTS YIELD name RETURN name").map(_("name"))
    assert(names.contains("element_id"), names.toString)
  }

  test("a wrong password never opens, and no log line or event carries it") {
    clear()
    val appender = new ListAppender[ILoggingEvent]
    appender.start()
    val logger = LoggerFactory.getLogger(classOf[Neo4jMergeStage]).asInstanceOf[LogbackLogger]
    logger.addAppender(appender)
    val events   = new CapturingEventSink
    val wrong    = "not-the-password-4711"
    val stage    = new Neo4jMergeStage(builtin, conf(credentials(wrong)), events)
    val deadline = System.nanoTime + 4.seconds.toNanos
    try
      assertEquals(stage.open(() => System.nanoTime > deadline), None)
      val lines = appender.list.asScala.map(_.getFormattedMessage)
      assert(lines.exists(_.contains("cannot open Neo4j")), lines.mkString("\n"))
      assert(!lines.exists(_.contains(wrong)), lines.mkString("\n"))
      assert(!events.warnings.asScala.exists(_._2.contains(wrong)))
    finally
      logger.detachAppender(appender): Unit
      stage.close()
  }

  test(
    "a missing credentials file refuses with exit 2; a descriptor that is not the built-in, exit 1"
  ) {
    val empty   = Files.createTempDirectory("neo4j-creds-empty")
    val noCreds = new Neo4jMergeStage(builtin, conf(empty), new CapturingEventSink)
    val refused = noCreds.open(() => false).get.left.toOption.get
    assertEquals(refused.exitCode, 2)
    assert(refused.problems.head.contains("has no 'uri'"), refused.problems.toString)

    val other    = Descriptor(TestSpecs.fixture("sink"))
    val wrong    = new Neo4jMergeStage(other, conf(credentials()), new CapturingEventSink)
    val refused1 = wrong.open(() => false).get.left.toOption.get
    assertEquals(refused1.exitCode, 1)
    assert(refused1.problems.exists(_.contains("streamlet name")), refused1.problems.toString)
  }

  test("the server version check accepts 5.26 and later, and calendar versions") {
    assert(Neo4jMergeStage.supported("Neo4j/5.26.0"))
    assert(Neo4jMergeStage.supported("Neo4j/5.27.1"))
    assert(Neo4jMergeStage.supported("Neo4j/2025.01.0"))
    assert(!Neo4jMergeStage.supported("Neo4j/5.24.2"))
    assert(!Neo4jMergeStage.supported("Neo4j/4.4.30"))
    assert(!Neo4jMergeStage.supported("Memgraph"))
  }

  test(
    "a delta under a key that is not its element's fails the batch, naming both keys, and writes nothing"
  ) {
    val o = open()
    val result = o.attemptKeyed(
      Vector(
        TestSpecs.keyed(0, Some("node:ok"), node("ok", 1)),
        TestSpecs.keyed(1, Some("cart-1"), node("cart:cart-1", 1))
      )
    )
    val message = result.left.toOption.map(_.getMessage).getOrElse("")
    assert(
      message.contains("offset 1: key 'cart-1' is not this delta's element key 'node:cart:cart-1'"),
      message
    )
    assertEquals(query("MATCH (n) RETURN count(n) AS c").head("c"), Long.box(0))
    assertEquals(o.metrics.of("in", 0).getBatchesFailed, 1L)
  }

  test("a delta with no key, or the right id under the wrong kind, is refused the same way") {
    val o       = open()
    val keyless = o.attemptKeyed(Vector(TestSpecs.keyed(0, None, node("n", 1))))
    assert(
      keyless.left.toOption
        .exists(_.getMessage.contains("offset 0: no key; this delta's element key is 'node:n'")),
      keyless.toString
    )
    val wrongKind = o.attemptKeyed(Vector(TestSpecs.keyed(0, Some("edge:n"), node("n", 1))))
    assert(
      wrongKind.left.toOption
        .exists(_.getMessage.contains("key 'edge:n' is not this delta's element key 'node:n'")),
      wrongKind.toString
    )
    assertEquals(query("MATCH (n) RETURN count(n) AS c").head("c"), Long.box(0))
  }

  test("a node and an edge with the same id are different elements under different keys") {
    val o = open()
    o(node("same", 1), edge("same", 1, "a", "b"))
    assertEquals(query("MATCH (n:Element {id:'same'}) RETURN count(n) AS c").head("c"), Long.box(1))
    assertEquals(
      query("MATCH ()-[r:LINKS {id:'same'}]->() RETURN count(r) AS c").head("c"),
      Long.box(1)
    )
  }

  test("a delete marker is passed over and counted: not written, not stale, not a failure") {
    val o = open()
    val result = o.attemptKeyed(
      Vector(
        TestSpecs.keyed(0, Some("node:a"), node("a", 1)),
        TestSpecs.keyed(1, Some("node:gone"), ""),
        TestSpecs.keyed(2, Some("node:b"), node("b", 1))
      )
    )
    assertEquals(result, Right(()))
    val m = o.metrics.of("in", 0)
    assertEquals(
      (m.getDeltasWritten, m.getDeltasStale, m.getDeleteMarkers, m.getBatchesFailed),
      (2L, 0L, 1L, 0L)
    )
  }

  test("a batch of delete markers alone is acknowledged; a marker changes nothing in the graph") {
    val o = open()
    o(node("a", 3, props = """{"v":3}"""))
    val before = props("a")
    val result = o.attemptKeyed(
      Vector(TestSpecs.keyed(0, Some("node:a"), ""), TestSpecs.keyed(1, None, ""))
    )
    assertEquals(result, Right(()))
    assertEquals(props("a"), before)
    assertEquals(o.metrics.of("in", 0).getDeleteMarkers, 2L)
    assertEquals(o.metrics.of("in", 0).getBatchesFailed, 0L)
  }
