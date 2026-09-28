package com.thinkmorestupidless.ankka.flow.sidecar

import scala.concurrent.Future

import ankka.flow.v1.payload.Record
import ankka.flow.v1.streamlet.InputRecord

/**
 * One batch as the sidecar assembled it: one inlet, one partition, records in offset order.
 * `generation` names the partition substream it came from, so a revocation of one substream can
 * never touch a batch of a later substream of the same partition.
 */
final case class InputBatch(
    inlet: String,
    partition: Int,
    records: Vector[InputRecord],
    generation: Long = 0L
)

/** A record the process emitted, to be produced to an outlet's topic. */
final case class EmittedRecord(outlet: String, record: Record)

enum Outcome:
  /** Acknowledged: produce these, then commit the batch. */
  case Acked(emits: Vector[EmittedRecord])

  /** The partition was revoked while the batch was in flight: produce nothing, commit nothing. */
  case Revoked

/**
 * Given a batch, the emits and the acknowledgement. A failure fails the stream.
 *
 * The process across the protocol is one implementation (`RemoteProcessor`). A stage built into the
 * sidecar's own image would be another (FR-018); version one ships none, and nothing here assumes
 * the process is the only kind.
 */
trait BatchProcessor:
  def process(batch: InputBatch): Future[Outcome]

  /** A partition's substream ended: its batch in flight, if any, completes as `Revoked`. */
  def revoke(inlet: String, partition: Int, generation: Long): Unit

/** Why a stream failed. The message is what the log and a stall warning say. */
final class StreamFailed(message: String, cause: Throwable = null)
    extends RuntimeException(message, cause)
