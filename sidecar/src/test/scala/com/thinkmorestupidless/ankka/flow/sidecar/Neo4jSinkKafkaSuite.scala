package com.thinkmorestupidless.ankka.flow.sidecar

import java.nio.file.{Files, Path}

import scala.concurrent.Await
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import com.thinkmorestupidless.ankka.flow.protocol.Builtins
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

/**
 * The sidecar in stage mode, end to end, with Kafka and Neo4j (quickstart tier 3): readiness,
 * commit after the transaction, redelivery and replay leaving the same graph, and an outage.
 */
class Neo4jSinkKafkaSuite extends KafkaSuite with Neo4jSuite:

  override val munitTimeout: FiniteDuration = 5.minutes

  private val builtin = Builtins.neo4jMergeSink

  private def creds(password: String = AdminPassword): Path =
    SidecarRun.writeSecret(Files.createTempDirectory("neo4j-creds"), boltUri, "neo4j", password)

  private def node(id: String, v: Long) =
    s"""{"kind":"node","id":"$id","version":$v,"labels":["Thing"],"properties":{"v":$v}}"""
  private def edge(id: String, v: Long, from: String, to: String) =
    s"""{"kind":"edge","id":"$id","version":$v,"type":"LINKS","from":"$from","to":"$to","properties":{"v":$v}}"""
  private def tomb(id: String, v: Long) =
    s"""{"kind":"tombstone","element":"node","id":"$id","version":$v}"""

  /** Five nodes over four versions, four edges over two, one tombstone: keyed by element id. */
  private val sequence: Vector[(String, String)] =
    (for v <- 1L to 4L; n <- 0 to 4 yield s"n$n" -> node(s"n$n", v)).toVector ++
      (for v <- 1L to 2L; e <- 0 to 3
      yield s"e$e" -> edge(s"e$e", v, s"n$e", s"n${e + 1}")).toVector :+
      ("n0" -> tomb("n0", 10))

  private def publishAll(topic: String, records: Vector[(String, String)]): Unit =
    publish(topic, records.map((k, v) => (Some(k), v, Nil)))

  /** Every node and edge the sink wrote, as plain values, for comparing whole graphs. */
  private def graph(): (Map[String, Map[String, AnyRef]], Map[String, Map[String, AnyRef]]) =
    val nodes = query("MATCH (n:Element) RETURN n.id AS id, properties(n) AS p")
      .map(r =>
        r("id").toString -> r("p").asInstanceOf[java.util.Map[String, AnyRef]].asScala.toMap
      )
      .toMap
    val edges = query("MATCH ()-[r]->() RETURN r.id AS id, properties(r) AS p")
      .map(r =>
        r("id").toString -> r("p").asInstanceOf[java.util.Map[String, AnyRef]].asScala.toMap
      )
      .toMap
    (nodes, edges)

  private val expected: (Map[String, Map[String, AnyRef]], Map[String, Map[String, AnyRef]]) =
    val nodes = (0 to 4).map { n =>
      val base = Map[String, AnyRef]("id" -> s"n$n", "v" -> Long.box(4), "_version" -> Long.box(4))
      s"n$n" -> (if n == 0 then
                   base + ("_version" -> Long.box(10)) + ("_deleted" -> java.lang.Boolean.TRUE)
                 else base)
    }.toMap
    val edges = (0 to 3)
      .map(e =>
        s"e$e" -> Map[String, AnyRef]("id" -> s"e$e", "v" -> Long.box(2), "_version" -> Long.box(2))
      )
      .toMap
    (nodes, edges)

  private def sidecar(
      topic: String,
      streamlet: String,
      password: String = AdminPassword,
      stallAfter: FiniteDuration = 5.minutes,
      timeout: String = "30s"
  ) =
    new SidecarRun(
      builtin,
      SidecarRun.stageConf(
        "graphs",
        streamlet,
        bootstrap,
        topic,
        creds(password),
        maxRecords = 7,
        transactionTimeout = timeout
      ),
      stallAfter = stallAfter
    )

  private def group(streamlet: String) = s"graphs.$streamlet.in"

  test(
    "not ready while the credentials are wrong; ready once they are corrected, with no restart"
  ) {
    clear()
    val topic = createTopic(uniqueTopic("deltas-auth"), 1)
    val run   = sidecar(topic, "auth", password = "wrong-password")
    try
      Thread.sleep(4000)
      assert(!run.ready, "ready with a wrong password")
      Files.writeString(
        run.config.stage.get.neo4j.map(n => Path.of(n.credentialsDir)).get.resolve("password"),
        AdminPassword
      )
      eventually(40.seconds)(assert(run.ready))
    finally run.stop(): Unit
  }

  test(
    "the scripted sequence builds the expected graph; redelivery and a replay from the start change nothing"
  ) {
    clear()
    val topic = createTopic(uniqueTopic("deltas-seq"), 3)
    publishAll(topic, sequence)
    val first = sidecar(topic, "seq")
    try
      eventually(60.seconds)(assertEquals(committed(group("seq")), sequence.size.toLong))
      assertEquals(graph(), expected)
      val written = first.stageMetrics.seen.toVector
        .map((i, p) => first.stageMetrics.of(i, p).getDeltasWritten)
        .sum
      assert(written >= 10 && written <= sequence.size, s"written $written")

      // every delta again: committed, all stale, the graph untouched
      publishAll(topic, sequence)
      eventually(60.seconds)(assertEquals(committed(group("seq")), 2L * sequence.size))
      assertEquals(graph(), expected)
      val writtenAfter = first.stageMetrics.seen.toVector
        .map((i, p) => first.stageMetrics.of(i, p).getDeltasWritten)
        .sum
      assertEquals(writtenAfter, written)
    finally first.stop(): Unit

    // a replay from the start, as `flow reset` does: every batch applied again, found stale
    admin { a =>
      val partitions = (0 until 3).map(p => new TopicPartition(topic, p))
      val earliest = a
        .listOffsets(partitions.map(_ -> OffsetSpec.earliest()).toMap.asJava)
        .all()
        .get()
        .asScala
        .map((tp, info) => tp -> new OffsetAndMetadata(info.offset))
      a.alterConsumerGroupOffsets(group("seq"), earliest.asJava).all().get()
    }
    val replay = sidecar(topic, "seq")
    try
      eventually(60.seconds)(assertEquals(committed(group("seq")), 2L * sequence.size))
      eventually(10.seconds)(assert(replay.stageMetrics.seen.nonEmpty))
      val replayWritten = replay.stageMetrics.seen.toVector
        .map((i, p) => replay.stageMetrics.of(i, p).getDeltasWritten)
        .sum
      assertEquals(replayWritten, 0L)
      assertEquals(graph(), expected)
    finally replay.stop(): Unit
  }

  test(
    "Neo4j stops answering: not ready, nothing committed, a stall warning with the reason; then it drains"
  ) {
    clear()
    val topic = createTopic(uniqueTopic("deltas-outage"), 1)
    publishAll(topic, Vector("a" -> node("a", 1)))
    // The stall threshold is above the batch deadline (5 s + 5 s), as 5 min is above 35 s in a pod,
    // so the warning carries the error that stalled the partition.
    val run = sidecar(topic, "outage", stallAfter = 15.seconds, timeout = "5s")
    try
      eventually(60.seconds)(assertEquals(committed(group("outage")), 1L))
      eventually(10.seconds)(assert(run.ready))
      pause()
      try
        publishAll(topic, (2L to 6L).toVector.map(v => "a" -> node("a", v)))
        eventually(120.seconds)(assert(!run.ready, "still ready with Neo4j paused"))
        assertEquals(committed(group("outage")), 1L)
        eventually(60.seconds)(
          assert(
            run.events.warnings.asScala.exists(_._1 == "PartitionStalled"),
            run.events.warnings.toString
          )
        )
        val notes = run.events.warnings.asScala.filter(_._1 == "PartitionStalled").map(_._2)
        assert(notes.exists(_.contains("neo4j merge failed")), notes.mkString("\n"))
        assert(!notes.exists(_.contains(AdminPassword)), notes.mkString("\n"))
      finally unpause()
      eventually(120.seconds)(assertEquals(committed(group("outage")), 6L))
      eventually(30.seconds)(assert(run.ready))
      assertEquals(query("MATCH (n:Element {id:'a'}) RETURN n.v AS v").head("v"), Long.box(6))
      assert(run.stageMetrics.of("in", 0).getBatchesFailed > 0L)
    finally run.stop(): Unit
  }

  test(
    "a deployed descriptor that is not this sidecar's built-in: exit 1, naming the differences"
  ) {
    val topic = createTopic(uniqueTopic("deltas-desc"), 1)
    val run = new SidecarRun(
      TestSpecs.fixture("sink"),
      SidecarRun.stageConf("graphs", "desc", bootstrap, topic, creds()),
      stallAfter = 5.minutes
    )
    assertEquals(Await.result(run.exited, 30.seconds), 1)
  }
