package com.thinkmorestupidless.ankka.flow.pekko

import java.util.concurrent.ConcurrentLinkedQueue

import scala.concurrent.Await
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.Try

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Sink, Source}

/**
 * What the runner hands its committer: each partition's offset once a later one of it is confirmed,
 * the rest when the stream completes, and nothing it holds when the stream fails.
 */
class HoldUntilNextSuite extends munit.FunSuite:

  private given system: ActorSystem = ActorSystem("hold")
  override def afterAll(): Unit     = Await.ready(system.terminate(), 10.seconds): Unit

  /** (partition, offset); two elements at one offset are one record made into two. */
  private def released(upstream: Source[(Int, Long), ?]): (Vector[(Int, Long)], Try[Any]) =
    val seen = new ConcurrentLinkedQueue[(Int, Long)]()
    val done = upstream
      .via(CommitAfterWrite.holdUntilNext[Int, (Int, Long)](_._1, _._2))
      .runWith(Sink.foreach(_.foreach(seen.add)))
    Try(Await.result(done, 10.seconds)).pipe(r => (seen.asScala.toVector, r))

  extension [A](a: A) private def pipe[B](f: A => B): B = f(a)

  test("an offset is released by a later one of its partition, and the rest on completion") {
    val (out, result) =
      released(Source(List(0 -> 0L, 1 -> 0L, 0 -> 1L, 0 -> 1L, 0 -> 2L, 1 -> 1L)))
    assert(result.isSuccess)
    assertEquals(out.take(2), Vector(0 -> 0L, 0 -> 1L))
    assertEquals(out.drop(2).toSet, Set(1 -> 0L, 0 -> 2L, 1 -> 1L))
  }

  test(
    "on failure nothing held is released: a record whose second write failed stays uncommitted"
  ) {
    // Offset 1's first element was confirmed; its second failed, so the stream fails.
    val (out, result) =
      released(
        Source(List(0 -> 0L, 0 -> 1L, 0 -> -1L))
          .map(e => if e._2 < 0 then throw new RuntimeException("write") else e)
      )
    assert(result.isFailure)
    assertEquals(out, Vector(0 -> 0L))
  }
