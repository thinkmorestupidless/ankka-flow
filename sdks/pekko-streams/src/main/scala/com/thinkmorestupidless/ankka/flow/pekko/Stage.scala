package com.thinkmorestupidless.ankka.flow.pekko

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.FlowWithContext

/**
 * Where a record came from: its partition and offset. The logic carries it through untouched (the
 * stage's context); the SDK commits it once everything made from the record is confirmed.
 */
final class Offset private[pekko] (
    val partition: Int,
    val offset: Long,
    private[pekko] val committable: AnyRef | Null
):
  override def toString: String = s"Offset($partition, $offset)"

/**
 * An inlet's records with their offsets: `map`, `filter`, `collect`, `mapConcat`, `mapAsync` and
 * `via` keep each element with the offset of the record it came from, and nothing reorders or drops
 * the offsets except with the element they belong to.
 */
type Stage[A] = FlowWithContext[Record[A], Offset, Record[A], Offset, NotUsed]

object Stage:

  /**
   * A stage whose elements are what the logic writes: an `Emit`, a `Seq` of them, or an `Option`.
   */
  type Out = FlowWithContext[Nothing, Offset, Emit | Seq[Emit] | Option[Emit], Offset, Any]

  /** An element of `Out` as the emits it stands for. */
  private[pekko] def emits(element: Emit | Seq[Emit] | Option[Emit]): Seq[Emit] = element match
    case e: Emit      => Seq(e)
    case s: Seq[?]    => s.asInstanceOf[Seq[Emit]]
    case o: Option[?] => o.asInstanceOf[Option[Emit]].toSeq

/** What a streamlet does with its inlets: one stage per inlet, joined with `and`. */
final class Logic private[pekko] (private[pekko] val parts: Vector[Logic.Part[?]]):
  def and(other: Logic): Logic = new Logic(parts ++ other.parts)

  /** The stage of an inlet, when the logic gives one. */
  private[pekko] def part(inlet: Inlet[?]): Option[Logic.Part[?]] = parts.find(_.inlet eq inlet)

object Logic:

  /** No stage at all: a streamlet that declares ports and does nothing with them. */
  val empty: Logic = new Logic(Vector.empty)

  private[pekko] final case class Part[A](
      inlet: Inlet[A],
      flow: FlowWithContext[Record[A], Offset, Seq[Emit], Offset, NotUsed]
  )
