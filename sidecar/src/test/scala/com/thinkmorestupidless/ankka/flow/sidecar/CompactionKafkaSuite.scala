package com.thinkmorestupidless.ankka.flow.sidecar

import java.nio.file.Files
import java.time.Duration as JDuration
import java.util.Properties

import scala.collection.mutable
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import com.thinkmorestupidless.ankka.flow.protocol.Builtins
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer

/**
 * Compaction actually run (feature 003, quickstart tier 3): a delta topic the broker has compacted
 * rebuilds the same graph into an emptied database, from a fraction of the records ever written; a
 * topic that was never compacted rebuilds the same graph too; and a delete marker after a tombstone
 * takes the element out of later rebuilds and nothing else.
 *
 * The broker's log cleaner wakes every half second here and the topics roll tiny segments, because
 * a real broker compacts on its own schedule and never the active segment.
 */
class CompactionKafkaSuite extends KafkaSuite with Neo4jSuite:

  override val munitTimeout: FiniteDuration = 8.minutes

  override protected def kafkaEnv: Map[String, String] =
    Map("KAFKA_LOG_CLEANER_BACKOFF_MS" -> "500")

  private val compacted = Map(
    "cleanup.policy"            -> "compact",
    "segment.bytes"             -> "65536",
    "segment.ms"                -> "500",
    "min.cleanable.dirty.ratio" -> "0.01",
    "min.compaction.lag.ms"     -> "0",
    "max.compaction.lag.ms"     -> "1000",
    "delete.retention.ms"       -> "500"
  )

  private def node(id: String, v: Long) =
    s"node:$id" -> s"""{"kind":"node","id":"$id","version":$v,"labels":["Thing"],"properties":{"v":$v,"name":"$id at $v"}}"""
  private def edge(id: String, v: Long, from: String, to: String) =
    s"edge:$id" -> s"""{"kind":"edge","id":"$id","version":$v,"type":"NEXT","from":"$from","to":"$to","properties":{"v":$v}}"""
  private def tombstone(id: String, v: Long) =
    s"node:$id" -> s"""{"kind":"tombstone","element":"node","id":"$id","version":$v}"""

  private def publishAll(topic: String, records: Seq[(String, String)]): Unit =
    publish(topic, records.map((k, v) => (Some(k), v, Nil)))

  private val tp = (topic: String) => new TopicPartition(topic, 0)

  private def endOffset(topic: String): Long =
    admin(
      _.listOffsets(Map(tp(topic) -> OffsetSpec.latest()).asJava).all().get().get(tp(topic)).offset
    )

  /** Every record still in the topic, from the start: its key and whether it has a value. */
  private def remaining(topic: String): Vector[(String, Boolean)] =
    val p = new Properties()
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    val consumer = new KafkaConsumer(p, new ByteArrayDeserializer, new ByteArrayDeserializer)
    try
      consumer.assign(java.util.List.of(tp(topic)))
      consumer.seekToBeginning(java.util.List.of(tp(topic)))
      val end = endOffset(topic)
      val out = mutable.ArrayBuffer.empty[(String, Boolean)]
      while consumer.position(tp(topic)) < end do
        consumer
          .poll(JDuration.ofMillis(500))
          .asScala
          .foreach(r => out += (new String(r.key, "UTF-8") -> (r.value != null)))
      out.toVector
    finally consumer.close()

  /** Rolls the active segment, which the cleaner never touches, by writing after `segment.ms`. */
  private def roll(topic: String): Unit =
    Thread.sleep(700)
    publishMarkers(topic, Seq("roll"))

  private var builds = 0

  /** Runs the sink over the whole topic under a group of its own and returns its counters. */
  private def build(topic: String): StageMetrics#Counters =
    builds += 1
    val creds =
      SidecarRun.writeSecret(
        Files.createTempDirectory("neo4j-creds"),
        boltUri,
        "neo4j",
        AdminPassword
      )
    val name = s"build$builds"
    val run = new SidecarRun(
      Builtins.neo4jMergeSink,
      SidecarRun.stageConf("compaction", name, bootstrap, topic, creds, maxRecords = 500)
    )
    try
      val end = endOffset(topic)
      eventually(3.minutes, 200.millis)(assertEquals(committed(s"compaction.$name.in"), end))
      run.stageMetrics.of("in", 0)
    finally run.stop(): Unit

  private val Elements = 1000
  private val Versions = 10

  private val history: Vector[(String, String)] =
    (for v <- 1L to Versions.toLong; n <- 0 until Elements yield node(s"n$n", v)).toVector ++
      (for v <- 1L to Versions.toLong; n <- 0 until Elements - 1
      yield edge(s"e$n", v, s"n$n", s"n${n + 1}")).toVector

  test(
    "a compacted delta topic rebuilds the same graph as its whole history, from a fraction of it"
  ) {
    // The whole history, kept: the graph as it was built when every record arrived.
    clear()
    val plain = createTopic(uniqueTopic("deltas-plain"), 1)
    publishAll(plain, history)
    val first    = build(plain)
    val original = graphSnapshot()
    assertEquals(original._1.size, Elements)
    assertEquals(original._2.size, Elements - 1)
    assertEquals(first.getDeltasWritten + first.getDeltasStale, history.size.toLong)

    // A topic that was never compacted rebuilds the same graph, reading all of it again.
    clear()
    val again = build(plain)
    assertEquals(graphSnapshot(), original)
    assertEquals(again.getDeltasWritten + again.getDeltasStale, history.size.toLong)

    // The same history on a compacted topic. The broker compacts in its own time; keep rolling
    // the active segment, which it never touches, until it has.
    val topic = createTopic(uniqueTopic("deltas-compacted"), 1, compacted)
    publishAll(topic, history)
    eventually(3.minutes, 2.seconds) {
      roll(topic)
      val left = remaining(topic)
      // A thousand nodes and nearly a thousand edges, ten versions each: about one record per
      // element once compacted, plus whatever is still in the active segment.
      assert(
        left.count(_._1.startsWith("node:")) < 2 * Elements && left.size < 3 * Elements,
        s"${left.size} records left, ${left.count(_._1.startsWith("node:"))} of them nodes"
      )
    }
    val left = remaining(topic).size

    clear()
    val rebuilt = build(topic)
    assertEquals(graphSnapshot(), original)
    val read = rebuilt.getDeltasWritten + rebuilt.getDeltasStale
    assert(read < history.size / 5, s"the rebuild read $read deltas of ${history.size} written")
    org.slf4j.LoggerFactory
      .getLogger(getClass)
      .info(
        "compaction: {} records written, {} left in the topic, the rebuild read {} deltas",
        history.size,
        left,
        read
      )
  }

  test(
    "a tombstone stays as its element's last record; a delete marker after it removes the element from later rebuilds, and nothing else"
  ) {
    clear()
    val records = (0 until 20).map(n => node(s"t$n", 1)) :+ tombstone("t0", 2)
    val topic   = createTopic(uniqueTopic("deltas-tombstone"), 1, compacted)
    val plain   = createTopic(uniqueTopic("deltas-tombstone-plain"), 1)
    publishAll(topic, records)
    publishAll(plain, records)

    build(topic): Unit
    val original = graphSnapshot()
    assertEquals(original._1("t0").get("_deleted"), Some(java.lang.Boolean.TRUE))
    val live = original._1 - "t0"

    // Before any marker, however long it has been: the tombstone is still there, and a rebuild
    // has the element marked deleted. The platform wrote nothing to the topic.
    roll(topic)
    Thread.sleep(3000)
    assert(remaining(topic).contains("node:t0" -> true), "the tombstone left the topic on its own")
    clear()
    build(topic): Unit
    assertEquals(graphSnapshot()._1("t0").get("_deleted"), Some(java.lang.Boolean.TRUE))
    assertEquals(
      endOffset(plain),
      records.size.toLong,
      "something other than the test wrote to the topic"
    )

    // The writer removes it: a marker under the element's key, after its tombstone.
    publishMarkers(topic, Seq("node:t0"))
    publishMarkers(plain, Seq("node:t0"))
    eventually(3.minutes, 2.seconds) {
      roll(topic)
      assert(
        !remaining(topic).contains("node:t0" -> true),
        "node:t0 still has a record with a value"
      )
    }
    clear()
    val rebuilt = build(topic)
    assertEquals(graphSnapshot()._1, live)
    assertEquals(rebuilt.getBatchesFailed, 0L)

    // Not yet compacted, the same history rebuilds the element marked deleted: the live graph is
    // the same either way.
    clear()
    val uncompacted = build(plain)
    val (nodes, _)  = graphSnapshot()
    assertEquals(nodes("t0").get("_deleted"), Some(java.lang.Boolean.TRUE))
    assertEquals(nodes - "t0", live)
    assertEquals(uncompacted.getDeleteMarkers, 1L)
  }
