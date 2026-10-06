package com.thinkmorestupidless.ankka.flow.sdk.testkit

import java.util.zip.CRC32

import scala.collection.mutable
import scala.util.control.NonFatal

import com.thinkmorestupidless.ankka.flow.sdk.*

/** A batch that failed, and why. */
final case class Failure(batch: Batch, error: Throwable)

/** Records put on one inlet, waiting for `run`. */
final class InletQueue private[testkit] (val name: String):
  private[testkit] val pending =
    mutable.ArrayBuffer.empty[(Option[Array[Byte]], Array[Byte], Seq[(String, Array[Byte])])]

  def put(
      value: Array[Byte],
      key: Option[Array[Byte]] = None,
      headers: Seq[(String, Array[Byte])] = Nil
  ): Unit =
    pending += ((key, value, headers))

/** What a streamlet emitted to one outlet, in order, from the batches that succeeded. */
final class OutletRecords private[testkit] (val name: String):
  private[testkit] val buffer = mutable.ArrayBuffer.empty[Record]
  def records: Vector[Record] = buffer.toVector

/**
 * Run a streamlet over in-memory inlets and outlets: no Kafka, no sidecar, no network.
 *
 * The harness applies the protocol's rules: records on one partition in offset order, emits
 * recorded only when their batch succeeds, and an exception or an emit to an undeclared outlet
 * fails the batch and discards its emits, as the sidecar would. A record no emit was derived from
 * is recorded as skipped.
 */
final class Harness(val streamlet: Streamlet, config: Map[String, String] = Map.empty):
  streamlet.configure(config)
  Streamlet.refuse(Descriptor.validate(streamlet))

  private val inlets  = streamlet.inlets.map(i => i.name -> new InletQueue(i.name)).toMap
  private val outlets = streamlet.outlets.map(o => o.name -> new OutletRecords(o.name)).toMap
  private val offsets = mutable.Map.empty[(String, Int), Long]
  private val failed  = mutable.ArrayBuffer.empty[Failure]
  private val skips   = mutable.ArrayBuffer.empty[Record]
  private val ran     = mutable.ArrayBuffer.empty[Batch]

  def inlet(name: String): InletQueue =
    inlets.getOrElse(
      name,
      throw new NoSuchElementException(
        s"no inlet '$name'; declared: ${inlets.keys.toVector.sorted.mkString(", ")}"
      )
    )

  def outlet(name: String): OutletRecords =
    outlets.getOrElse(
      name,
      throw new NoSuchElementException(
        s"no outlet '$name'; declared: ${outlets.keys.toVector.sorted.mkString(", ")}"
      )
    )

  def failures: Vector[Failure] = failed.toVector
  def skipped: Vector[Record]   = skips.toVector
  def batches: Vector[Batch]    = ran.toVector

  /**
   * Process everything put so far: per inlet, per partition, batches in offset order. `partitions`
   * places a key on a partition (default: everything on partition 0); `maxRecords` bounds a batch
   * (default: one batch per partition).
   */
  def run(
      partitions: Option[Array[Byte]] => Int = Harness.singlePartition,
      maxRecords: Option[Int] = None
  ): Unit =
    streamlet.inlets.foreach { declared =>
      val queue       = inlets(declared.name)
      val byPartition = mutable.TreeMap.empty[Int, mutable.ArrayBuffer[Record]]
      queue.pending.foreach { (key, value, headers) =>
        val partition = partitions(key)
        val offset    = offsets.getOrElse((queue.name, partition), 0L)
        offsets((queue.name, partition)) = offset + 1
        byPartition.getOrElseUpdate(partition, mutable.ArrayBuffer.empty) += Record(
          value,
          key,
          headers,
          offset
        )
      }
      queue.pending.clear()
      byPartition.foreach { (partition, records) =>
        val size = maxRecords.getOrElse(records.size).max(1)
        records.grouped(size).foreach(group => runOne(Batch(queue.name, partition, group.toVector)))
      }
    }

  private def runOne(batch: Batch): Unit =
    ran += batch
    try
      val emits   = Streamlet.runBatch(streamlet, batch).toVector
      val derived = emits.map(_.record.offset).toSet
      skips ++= batch.records.filterNot(r => derived(r.offset))
      emits.foreach(e => outlets(e.outlet).buffer += e.record)
    catch case NonFatal(e) => failed += Failure(batch, e)

object Harness:

  /** Every record on partition 0. */
  val singlePartition: Option[Array[Byte]] => Int = _ => 0

  /**
   * A stable key-to-partition function: the CRC32 of the key modulo `partitions`, as the Python
   * harness.
   */
  def hashPartitioner(partitions: Int): Option[Array[Byte]] => Int =
    case None => 0
    case Some(key) =>
      val crc = new CRC32
      crc.update(key)
      (crc.getValue % partitions).toInt
