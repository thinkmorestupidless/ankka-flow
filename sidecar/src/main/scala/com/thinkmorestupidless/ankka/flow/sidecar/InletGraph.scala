package com.thinkmorestupidless.ankka.flow.sidecar

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters.*

import ankka.flow.v1.payload.{Header, Record}
import ankka.flow.v1.streamlet.InputRecord
import com.google.protobuf.ByteString
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.pekko.Done
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.kafka.{
  CommitterSettings,
  ConsumerSettings,
  RestrictedConsumer,
  Subscriptions
}
import org.apache.pekko.kafka.ConsumerMessage.{
  CommittableMessage,
  CommittableOffset,
  CommittableOffsetBatch
}
import org.apache.pekko.kafka.scaladsl.{Consumer, PartitionAssignmentHandler}
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.slf4j.LoggerFactory

/**
 * One inlet: a committable source per assigned partition → batches bounded by count, bytes and time
 * → the processor, one batch in flight per partition → produce the emits → commit (research R8).
 */
final class InletGraph(
    inlet: InletConfig,
    processor: BatchProcessor,
    producers: Producers,
    stalls: Stalls,
    committerSettings: CommitterSettings
)(using system: ActorSystem):

  import InletGraph.*

  private val log = LoggerFactory.getLogger(classOf[InletGraph])

  private given ExecutionContext = system.dispatcher

  private val subscribedPromise = Promise[Unit]()
  private val generations       = new AtomicLong(0L)

  /**
   * Set before the sidecar shuts this graph down itself (a failed stream, or stopping). Its
   * substreams then end without the partitions having moved anywhere, so their stalls are kept: the
   * batch will be sent again after the reconnect and the stall is measured from its first try.
   */
  private val tearingDown = new AtomicBoolean(false)

  /** Completes when the consumer has joined its group and been given its (possibly empty) share. */
  def subscribed: Future[Unit] = subscribedPromise.future

  private val assignment = new PartitionAssignmentHandler:
    // Revocation is not acted on here: Pekko keeps a partition's substream when the same rebalance
    // hands the partition back, so the substream ending is the only reliable "this is no longer
    // ours" (see `partition`).
    def onRevoke(revokedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit =
      if revokedTps.nonEmpty then
        log.info(
          "inlet '{}': partitions revoked {}",
          inlet.name,
          revokedTps.map(_.partition).toSeq.sorted
        )
    def onAssign(assignedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit =
      log.info(
        "inlet '{}': partitions assigned {}",
        inlet.name,
        assignedTps.map(_.partition).toSeq.sorted
      )
      assignedTps.foreach(tp => stalls.assigned(inlet.name, tp.partition))
      // A partition that went to another pod while this sidecar was reconnecting is not stalled here.
      stalls.retain(inlet.name, assignedTps.map(_.partition))
      // An empty assignment means "subscribed, nothing for this pod" only when the topic exists.
      // A topic that does not exist also assigns nothing, and must never make the pod ready.
      val exists = assignedTps.nonEmpty ||
        scala.util
          .Try(consumer.beginningOffsets(java.util.List.of(new TopicPartition(inlet.topic, 0))))
          .toOption
          .exists(offsets => !offsets.isEmpty)
      if exists then subscribedPromise.trySuccess(()): Unit
      else
        log.warn(
          "inlet '{}': topic '{}' does not exist; not ready until it does",
          inlet.name,
          inlet.topic
        )
    def onLost(lostTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit =
      log.warn("inlet '{}': partitions lost {}", inlet.name, lostTps.map(_.partition).toSeq.sorted)
    def onStop(currentTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = ()

  def consumerSettings: ConsumerSettings[Array[Byte], Array[Byte]] =
    ConsumerSettings(system, new ByteArrayDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers(inlet.bootstrapServers)
      .withGroupId(inlet.group)
      .withClientId(inlet.clientId)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withProperties(inlet.connectionConfig ++ inlet.consumerConfig)
      // The platform never creates a topic by reading it (FR-005): a broker that auto-creates
      // topics would otherwise create an unmanaged topic the moment the sidecar subscribed.
      .withProperty(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false")

  def run(): Running =
    val (control, done) = Consumer
      .committablePartitionedSource(
        consumerSettings,
        Subscriptions.topics(inlet.topic).withPartitionAssignmentHandler(assignment)
      )
      .mapAsyncUnordered(MaxPartitions)((tp, source) => partition(tp, source))
      .toMat(Sink.ignore)(Keep.both)
      .run()
    Running(control, done, subscribed, () => tearingDown.set(true))

  private def partition(
      tp: TopicPartition,
      source: Source[CommittableMessage[Array[Byte], Array[Byte]], ?]
  ): Future[Done] =
    val revoked    = new AtomicBoolean(false)
    val generation = generations.incrementAndGet()
    val p          = tp.partition
    source
      .watchTermination() { (_, terminated) =>
        // The substream ended: the partition is no longer ours. Its batch in flight, if any, is
        // discarded and nothing more is committed for it; the next owner reads it again.
        terminated.onComplete { ended =>
          revoked.set(true)
          processor.revoke(inlet.name, p, generation)
          // Only a partition taken away is no longer stalled here. A substream that failed (its
          // batch failed) or that the sidecar is shutting down will be read again from the same
          // offset after the reconnect, and its stall is measured from the first attempt.
          if ended.isSuccess && !tearingDown.get then stalls.forget(inlet.name, p)
        }
      }
      .map { msg =>
        val record = msg.record
        val size   = weight(record)
        if size > Conversation.MaxRecordBytes then
          throw new StreamFailed(
            s"record at ${record.topic}/${record.partition}@${record.offset} is $size bytes, over the ${Conversation.MaxRecordBytes}-byte limit"
          )
        (toInput(record), size, msg.committableOffset)
      }
      // Natural batching: a batch is whatever arrived while the previous one was with the process,
      // capped at max-records and max-bytes. A quiet stream sends each record at once; a busy one
      // sends full batches. Both caps are held by one weight: each record costs the larger of
      // bytes x max-records and max-bytes, so the batch's total stays under max-records x max-bytes
      // exactly when it holds at most max-records records of at most max-bytes in all (SC-008).
      .batchWeighted(
        inlet.batch.maxRecords.toLong * inlet.batch.maxBytes,
        (r: (InputRecord, Long, CommittableOffset)) =>
          math.max(r._2 * inlet.batch.maxRecords, inlet.batch.maxBytes),
        (r: (InputRecord, Long, CommittableOffset)) => Vector(r)
      )(_ :+ _)
      .mapAsync(1) { group =>
        if revoked.get then Future.successful(None)
        else
          stalls.sent(inlet.name, p)
          processor
            .process(InputBatch(inlet.name, p, group.map(_._1).toVector, generation))
            .map {
              case Outcome.Acked(emits) if !revoked.get =>
                Some(emits -> CommittableOffsetBatch(group.map(_._3)))
              case _ => None
            }
      }
      // A revoked batch ends the substream: nothing after it may be committed, or its records
      // would be skipped.
      .takeWhile(_.isDefined)
      .collect { case Some(acked) => acked }
      .runWith(
        CommitAfterWrite.sinkCommittingAfter[Vector[EmittedRecord]](
          write = batches =>
            producers.sendAll(batches.flatten.toVector).map(_ => stalls.committed(inlet.name, p)),
          committerSettings = committerSettings
        )
      )

object InletGraph:

  /** Upper bound on partitions of one inlet a single pod runs at once. */
  val MaxPartitions = 1024

  final case class Running(
      control: Consumer.Control,
      done: Future[Done],
      subscribed: Future[Unit],
      /** Call before shutting the graph down on purpose; see `tearingDown`. */
      tearingDown: () => Unit = () => ()
  )

  def weight(r: ConsumerRecord[Array[Byte], Array[Byte]]): Long =
    val k = Option(r.key).fold(0)(_.length)
    val v = Option(r.value).fold(0)(_.length)
    val h =
      r.headers.toArray.iterator.map(h => h.key.length + Option(h.value).fold(0)(_.length)).sum
    (k + v + h).toLong

  def toInput(r: ConsumerRecord[Array[Byte], Array[Byte]]): InputRecord =
    InputRecord(
      offset = r.offset,
      timestampMs = r.timestamp,
      record = Some(
        Record(
          key = Option(r.key).map(ByteString.copyFrom),
          headers = r.headers.toArray.toSeq.map(h =>
            Header(h.key, Option(h.value).fold(ByteString.EMPTY)(ByteString.copyFrom))
          ),
          value = Option(r.value).fold(ByteString.EMPTY)(ByteString.copyFrom)
        )
      )
    )

  def headersOf(r: ConsumerRecord[?, ?]): Vector[(String, Array[Byte])] =
    r.headers.asScala.toVector.map(h => h.key -> h.value)
