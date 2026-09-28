package com.thinkmorestupidless.ankka.flow.sidecar

import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration.*

import org.apache.pekko.kafka.CommitterSettings

/**
 * User Story 1's independent test with the Scala double standing in for Python: fifty cart events
 * over ten carts, the process killed part way through and restarted, and every event found on
 * exactly its outlet, each cart in order on one partition, headers intact (S1.1-S1.4). Then a
 * partition revoked while its batch is in flight commits nothing, and its records are read again.
 */
class InletGraphKafkaSuite extends KafkaSuite:

  private def total(r: ankka.flow.v1.streamlet.InputRecord): Int =
    """"total":(\d+)""".r.findFirstMatchIn(r.getRecord.value.toStringUtf8).get.group(1).toInt

  test(
    "fifty events over ten carts survive a process kill: each on exactly its outlet, in order per cart, headers intact"
  ) {
    val in     = createTopic(uniqueTopic("cart-events"), 3)
    val valid  = createTopic(uniqueTopic("valid"), 3)
    val review = createTopic(uniqueTopic("review"), 3)
    val events = (0 until 50).map { i =>
      val cart  = s"cart-${i % 10}"
      val total = (i * 37) % 200
      (
        Some(cart),
        s"""{"id":$i,"cart":"$cart","total":$total}""",
        Seq(
          "ce_type"   -> "CartUpdated".getBytes,
          "ce_id"     -> s"$i".getBytes,
          "ce_source" -> "test".getBytes
        )
      )
    }
    def expectedOutlet(i: Int) = if (i * 37) % 200 > 100 then review else valid

    val double = new ProcessDouble(
      TestSpecs.fixture("cart-router"),
      ProcessDouble.keyed(r => Vector(if total(r) > 100 then "review" else "valid"))
    )
    val port   = double.start()
    val killed = new AtomicBoolean(false)
    double.onAck = n =>
      if n == 8 && killed.compareAndSet(false, true) then
        new Thread(() => {
          double.stop()
          Thread.sleep(2000)
          double.restart()
        }).start()

    val sidecar = new SidecarRun(
      TestSpecs.fixture("cart-router"),
      SidecarRun.conf(
        "cart",
        "router",
        bootstrap,
        Seq("in"    -> in),
        Seq("valid" -> valid, "review" -> review),
        maxRecords = 2
      ),
      port
    )
    try
      eventually()(assert(sidecar.ready, "the sidecar never became ready"))
      publish(in, events)
      eventually(60.seconds)(assertEquals(committed("cart.router.in"), 50L))
      assert(killed.get, "the process was never killed")
      assert(
        double.starts.map(_.conversationId).distinct.size >= 2,
        "no second conversation after the restart"
      )

      val onValid  = consumeAll(valid, 50, 5.seconds)
      val onReview = consumeAll(review, 50, 5.seconds)
      val all      = onValid ++ onReview
      val ids = all.map(r => """"id":(\d+)""".r.findFirstMatchIn(str(r.value)).get.group(1).toInt)
      assertEquals(ids.toSet, (0 until 50).toSet, "every event arrived")
      (onValid.map(r => r -> valid) ++ onReview.map(r => r -> review)).foreach { (r, topic) =>
        val id = """"id":(\d+)""".r.findFirstMatchIn(str(r.value)).get.group(1).toInt
        assertEquals(topic, expectedOutlet(id), s"event $id on the wrong outlet")
        assertEquals(r.headers.toArray.map(_.key).toList, List("ce_type", "ce_id", "ce_source"))
        assertEquals(str(r.headers.lastHeader("ce_id").value), id.toString)
      }
      all.groupBy(r => (str(r.key), r.topic)).foreach { case ((cart, topic), records) =>
        assertEquals(records.map(_.partition).distinct.size, 1, s"$cart on $topic spans partitions")
        val sequence = records
          .sortBy(_.offset)
          .map(r => """"id":(\d+)""".r.findFirstMatchIn(str(r.value)).get.group(1).toInt)
        val firsts = sequence.distinct
        assertEquals(firsts, firsts.sorted, s"$cart on $topic out of order: $sequence")
      }
    finally
      assertEquals(sidecar.stop(), 0)
      double.close()
  }

  test(
    "a partition revoked while its batch is in flight commits nothing for it, and the batch is read again"
  ) {
    val in  = createTopic(uniqueTopic("revoke-in"), 4)
    val out = createTopic(uniqueTopic("revoke-out"), 4)
    publish(in, (0 until 40).map(i => (Some(s"k-$i"), s"$i", Nil)))
    val group = "revoke.app.in"

    // A processor that echoes, except that the first batch it sees is held: until its substream
    // ends (the partition went elsewhere: Revoked), or for 8 s, like a slow process (the same
    // rebalance handed the partition back, and Pekko kept the substream).
    final class Gated extends BatchProcessor:
      private val gate                   = Promise[Outcome]()
      private val held                   = new AtomicBoolean(false)
      @volatile var heldPartition: Int   = -1
      @volatile var heldGeneration: Long = -1
      def process(batch: InputBatch): Future[Outcome] =
        if held.compareAndSet(false, true) then
          heldPartition = batch.partition
          heldGeneration = batch.generation
          val acked = Outcome.Acked(batch.records.map(r => EmittedRecord("out", r.getRecord)))
          system.scheduler.scheduleOnce(8.seconds)(gate.trySuccess(acked): Unit)(using
            system.dispatcher
          )
          gate.future
        else
          Future.successful(
            Outcome.Acked(batch.records.map(r => EmittedRecord("out", r.getRecord)))
          )
      def revoke(inlet: String, partition: Int, generation: Long): Unit =
        if partition == heldPartition && generation == heldGeneration then
          gate.trySuccess(Outcome.Revoked): Unit
      def released: Boolean = gate.isCompleted

    def graph(processor: BatchProcessor) =
      val inlet = InletConfig(
        "in",
        in,
        group,
        s"$group.${processor.hashCode}",
        bootstrap,
        Map.empty,
        Map.empty,
        BatchSettings(5, 1024 * 1024)
      )
      new InletGraph(
        inlet,
        processor,
        new Producers(
          Map("out" -> OutletConfig("out", out, "revoke.app.out", bootstrap, Map.empty, Map.empty))
        ),
        new Stalls(1.hour, new LogEventSink),
        CommitAfterWrite.defaultCommitterSettings(CommitterSettings(system))
      ).run()

    val gated = new Gated
    val a     = graph(gated)
    eventually()(assert(gated.heldPartition >= 0, "the first batch never arrived"))
    val echo = new BatchProcessor:
      def process(batch: InputBatch): Future[Outcome] =
        Future.successful(Outcome.Acked(batch.records.map(r => EmittedRecord("out", r.getRecord))))
      def revoke(inlet: String, partition: Int, generation: Long): Unit = ()
    val b = graph(echo)
    eventually(20.seconds)(assert(gated.released, "the held batch was never released"))
    // Every record, the revoked batch's included, is eventually written and committed by someone.
    eventually(60.seconds)(
      assertEquals(
        committed(group),
        40L,
        s"held ${gated.heldPartition}; per partition ${perPartition(group, in)}"
      )
    )
    assert(
      !a.done.isCompleted && !b.done.isCompleted,
      s"a graph stopped: a ${a.done.value}, b ${b.done.value}"
    )
    val written = consumeAll(out, 40, 10.seconds).map(r => str(r.value).toInt).toSet
    assertEquals(written, (0 until 40).toSet)
    Await.ready(a.control.shutdown(), 10.seconds)
    Await.ready(b.control.shutdown(), 10.seconds): Unit
  }
