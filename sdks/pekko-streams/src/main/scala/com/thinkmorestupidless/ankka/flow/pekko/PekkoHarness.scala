package com.thinkmorestupidless.ankka.flow.pekko

import scala.concurrent.Await
import scala.concurrent.duration.*
import scala.util.control.NonFatal

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.stream.scaladsl.{Sink, Source}

/**
 * Runs a streamlet's logic over records in memory, with no broker and no cluster: the same stages
 * the runner gives the records of a topic.
 *
 * {{{
 * val result = PekkoHarness(Cleaner, "max-total" -> "250").run(Cleaner.in -> records)
 * result(Cleaner.out)   // what was written to `out`, in order
 * result.committed      // the positions whose offsets would be committed
 * }}}
 *
 * Each record is encoded and decoded with its inlet's codec on the way in, and each emit encoded
 * with its outlet's on the way out, so a codec that cannot do either fails the run as it would fail
 * the stream. A position is committed when its record's stage produced an element, even one that
 * writes nothing (`Nil`, `None`); a record the stage drops is covered by the next commit.
 */
final class PekkoHarness private (streamlet: PekkoStreamlet, parameters: Map[String, String]):

  def run[A](inputs: (Inlet[A], Seq[Record[A]])*): PekkoHarness.Result =
    val system = ActorSystem(Behaviors.empty[Nothing], "pekko-harness")
    try
      val ctx   = Context.resolve(streamlet.parameters, parameters, system)
      val logic = streamlet.logic(ctx)
      val runs = inputs.toVector.map { (inlet, records) =>
        val part = logic
          .part(inlet)
          .getOrElse(
            throw new IllegalArgumentException(
              s"streamlet '${streamlet.name}' gives no stage for inlet '${inlet.name}'"
            )
          )
          .asInstanceOf[Logic.Part[A]]
        inlet -> PekkoHarness.runInlet(part, records, system)
      }
      PekkoHarness.Result(
        runs.flatMap(_._2._1).groupMap(_.outlet.name)(_.record),
        runs.map((i, r) => i.name -> r._2).toMap
      )
    finally system.terminate()

object PekkoHarness:

  def apply(streamlet: PekkoStreamlet, parameters: (String, String)*): PekkoHarness =
    new PekkoHarness(streamlet, parameters.toMap)

  /** What a run wrote, per outlet, and the positions it committed, per inlet. */
  final class Result private[pekko] (
      written: Map[String, Vector[Record[?]]],
      commits: Map[String, Vector[Int]]
  ):
    def apply[B](outlet: Outlet[B]): Vector[Record[B]] =
      written.getOrElse(outlet.name, Vector.empty).asInstanceOf[Vector[Record[B]]]

    /** The committed positions of the one inlet run; `committedOf` for one of several. */
    def committed: Vector[Int] = commits.values.flatten.toVector.sorted

    def committedOf(inlet: Inlet[?]): Vector[Int] = commits.getOrElse(inlet.name, Vector.empty)

  /** The run failed at `position` of `inlet`; `committed` is what was committed before it. */
  final class Failed private[pekko] (
      val inlet: String,
      val position: Int,
      val committed: Vector[Int],
      cause: Throwable
  ) extends RuntimeException(
        s"inlet '$inlet' failed at position $position: ${cause.getClass.getSimpleName}: ${cause.getMessage}",
        cause
      )

  private val Timeout = 30.seconds

  private def runInlet[A](
      part: Logic.Part[A],
      records: Seq[Record[A]],
      system: ActorSystem[?]
  ): (Vector[Emit], Vector[Int]) =
    given ActorSystem[?] = system
    val inlet            = part.inlet
    records.zipWithIndex.foldLeft((Vector.empty[Emit], Vector.empty[Int])) {
      case ((written, committed), (record, position)) =>
        def fail(e: Throwable) = throw new Failed(inlet.name, position, committed, e)
        val decoded =
          try record.withValue(inlet.codec.decode(inlet.codec.encode(record.value)))
          catch case NonFatal(e) => fail(e)
        val elements =
          try
            Await.result(
              Source
                .single(decoded -> new Offset(0, position.toLong, null))
                .via(part.flow.asFlow)
                .map(_._1)
                .runWith(Sink.seq),
              Timeout
            )
          catch case NonFatal(e) => fail(e)
        val emits = elements.flatten.toVector
        try emits.foreach(_.bytes)
        catch case NonFatal(e) => fail(e)
        (written ++ emits, if elements.nonEmpty then committed :+ position else committed)
    }
