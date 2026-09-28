package com.thinkmorestupidless.ankka.flow.sidecar

import java.util.UUID

import scala.collection.mutable
import scala.concurrent.{Future, Promise}

import ankka.flow.v1.streamlet.*
import io.grpc.{ManagedChannel, Status}
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory

/**
 * One `Streamlet.Run` conversation (contracts/protocol.md). Correlates batches by id, buffers each
 * batch's emits until its ack, and fails the whole conversation on any violation, on `Fail`, or
 * when the process goes away. Thread-safe: batches are sent from stream threads, answers arrive on
 * gRPC's.
 */
final class Conversation private (
    channel: ManagedChannel,
    val start: Start
) extends BatchProcessor:

  private val log = LoggerFactory.getLogger(classOf[Conversation])

  private final case class Pending(
      id: Long,
      inlet: String,
      partition: Int,
      generation: Long,
      emits: mutable.ArrayBuffer[EmittedRecord],
      promise: Promise[Outcome]
  )

  private val outlets                             = start.outlets.map(_.port).toSet
  private val inFlight                            = mutable.LongMap.empty[Pending]
  private val revokedIds                          = mutable.Set.empty[Long]
  private var nextId                              = 1L
  private var stopping                            = false
  private val failure                             = Promise[Throwable]()
  private var requests: StreamObserver[ToProcess] = scala.compiletime.uninitialized

  /** Completes once, with the reason, when the conversation can no longer be used. */
  def failed: Future[Throwable] = failure.future

  private val responses = new StreamObserver[FromProcess]:
    def onNext(msg: FromProcess): Unit = receive(msg)
    def onError(t: Throwable): Unit =
      fail(new StreamFailed(s"the process is unreachable: ${Status.fromThrowable(t)}", t))
    def onCompleted(): Unit =
      val expected = Conversation.this.synchronized(stopping)
      if !expected then fail(new StreamFailed("the process closed the stream"))

  private def open(): Unit =
    requests = StreamletGrpc.stub(channel).run(responses)
    send(ToProcess(ToProcess.Message.Start(start)))

  private def send(msg: ToProcess): Unit = synchronized {
    if !failure.isCompleted then requests.onNext(msg)
  }

  def process(batch: InputBatch): Future[Outcome] =
    val promise = Promise[Outcome]()
    synchronized {
      failure.future.value match
        case Some(scala.util.Success(t)) => promise.failure(t)
        case _ =>
          val id = nextId
          nextId += 1
          inFlight(id) = Pending(
            id,
            batch.inlet,
            batch.partition,
            batch.generation,
            mutable.ArrayBuffer.empty,
            promise
          )
          requests.onNext(
            ToProcess(
              ToProcess.Message.Batch(
                Batch(id, batch.inlet, batch.partition, batch.records)
              )
            )
          )
    }
    promise.future

  def revoke(inlet: String, partition: Int, generation: Long): Unit = synchronized {
    val ids = inFlight.values
      .filter(p => p.inlet == inlet && p.partition == partition && p.generation == generation)
      .map(_.id)
      .toVector
    ids.foreach { id =>
      val p = inFlight.remove(id).get
      revokedIds += id
      p.promise.trySuccess(Outcome.Revoked)
    }
  }

  private def receive(msg: FromProcess): Unit =
    def violation(v: String) = Some(new StreamFailed(s"protocol violation: $v"))
    val failure: Option[StreamFailed] = synchronized {
      msg.message match
        case FromProcess.Message.Emit(e) =>
          if revokedIds(e.batchId) then None
          else
            inFlight.get(e.batchId) match
              case None => violation(s"emit for ${describe(e.batchId)}")
              case Some(_) if !outlets(e.outlet) =>
                violation(s"emit to outlet '${e.outlet}', which the streamlet does not declare")
              case Some(p) =>
                p.emits += EmittedRecord(e.outlet, e.getRecord)
                None
        case FromProcess.Message.Ack(a) =>
          if revokedIds.remove(a.batchId) then None
          else
            inFlight.remove(a.batchId) match
              case None => violation(s"ack for ${describe(a.batchId)}")
              case Some(p) =>
                p.promise.trySuccess(Outcome.Acked(p.emits.toVector))
                None
        case FromProcess.Message.Fail(f) =>
          if revokedIds.remove(f.batchId) then None
          else
            inFlight.get(f.batchId) match
              case None => violation(s"fail for ${describe(f.batchId)}")
              case Some(p) =>
                Some(
                  new StreamFailed(
                    s"the process failed batch ${f.batchId} (inlet '${p.inlet}', partition ${p.partition}): ${f.getError.message}"
                  )
                )
        case FromProcess.Message.Empty => violation("an empty message")
    }
    failure.foreach(fail)

  private def describe(id: Long): String =
    if id >= nextId || id <= 0 then s"batch $id, which was never sent"
    else s"batch $id, which is not in flight (already acknowledged or failed)"

  /** Fails the conversation once: every batch in flight fails, and the stream is cancelled. */
  def fail(cause: Throwable): Unit = end(cause, cancel = true)

  private def end(cause: Throwable, cancel: Boolean): Unit =
    val pending = synchronized {
      if failure.isCompleted then None
      else
        failure.success(cause)
        val ps = inFlight.values.toVector
        inFlight.clear()
        Some(ps)
    }
    pending.foreach { ps =>
      if cancel then
        log.info(
          "conversation {} failed: {}; voiding {} batch(es) in flight",
          start.conversationId,
          cause.getMessage,
          ps.size
        )
      ps.foreach(_.promise.tryFailure(cause))
      if cancel then
        try
          requests.onError(Status.CANCELLED.withDescription(cause.getMessage).asRuntimeException())
        catch case _: IllegalStateException => ()
    }

  /** Sends `Stop` and half-closes. The process completes its side; nothing is cancelled. */
  def stop(reason: String): Unit =
    synchronized { stopping = true }
    try
      send(ToProcess(ToProcess.Message.Stop(Stop(reason))))
      synchronized(if !failure.isCompleted then requests.onCompleted())
    catch case _: IllegalStateException => ()
    end(new StreamFailed(s"stopped: $reason"), cancel = false)

object Conversation:

  /** The largest single message a batch may be; one record above it fails the stream (R10). */
  val MaxMessageBytes: Int = 4 * 1024 * 1024

  /** What a single input record may weigh: the limit less framing. */
  val MaxRecordBytes: Int = MaxMessageBytes - 64 * 1024

  def open(
      channel: ManagedChannel,
      pipeline: String,
      streamlet: String,
      configJson: String,
      inlets: Seq[(String, String)],
      outlets: Seq[(String, String)]
  ): Conversation =
    val start = Start(
      conversationId = UUID.randomUUID().toString,
      pipeline = pipeline,
      streamlet = streamlet,
      configJson = configJson,
      inlets = inlets.map((p, t) => PortBinding(p, t)),
      outlets = outlets.map((p, t) => PortBinding(p, t)),
      maxMessageBytes = MaxMessageBytes
    )
    val c = new Conversation(channel, start)
    c.open()
    c
