package com.thinkmorestupidless.ankka.flow.sidecar

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.*
import scala.util.Try

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.kafka.CommitterSettings
import org.slf4j.LoggerFactory

/**
 * The sidecar's loop (data-model.md, *Conversation lifecycle*): open the stage, run, and on any
 * failure tear everything down, back off, and open again. Never skips; never gives up. Exits only
 * when the stage refuses to open (its refusal's code) or it is asked to stop (0).
 */
final class Supervisor(
    settings: Settings,
    config: StreamletConfig,
    probes: Probes,
    stalls: Stalls,
    stage: Stage
)(using system: ActorSystem):

  private val log = LoggerFactory.getLogger(classOf[Supervisor])

  private given ExecutionContext = system.dispatcher

  private val stopSignal = Promise[Unit]()

  def requestStop(): Unit      = stopSignal.trySuccess(()): Unit
  private def stopped: Boolean = stopSignal.isCompleted

  /** The number of runs (conversations, for a process) started so far; a test hook. */
  @volatile var conversations: Int = 0

  private val committerSettings =
    CommitAfterWrite
      .defaultCommitterSettings(CommitterSettings(system))
      .withMaxBatch(1000)
      .withMaxInterval(200.millis)

  def run(): Int =
    try loop()
    finally stage.close()

  private def loop(): Int =
    var backoff           = 500.millis
    var exit: Option[Int] = None
    while exit.isEmpty do
      probes.notReady()
      if stopped then exit = Some(0)
      else
        stage.open(() => stopped) match
          case None => exit = Some(0)
          case Some(Left(refusal)) =>
            exit = Some(refusal.exitCode)
          case Some(Right(opened)) =>
            val session = Session.start(opened)
            val started = System.nanoTime
            val ended = Await.result(
              Future.firstCompletedOf(
                Seq(session.failed.map(Left(_)), stopSignal.future.map(Right(_)))
              ),
              Duration.Inf
            )
            ended match
              case Right(()) =>
                session.stop("the sidecar is shutting down")
                exit = Some(0)
              case Left(cause) =>
                stalls.lastError = cause.getMessage
                log.warn("stream failed: {}", cause.getMessage)
                session.teardown()
                if (System.nanoTime - started).nanos > settings.reconnectMaxBackoff * 2 then
                  backoff = 500.millis
                log.info("reconnecting in {}", backoff)
                Try(Await.ready(stopSignal.future, backoff))
                backoff = (backoff * 2).min(settings.reconnectMaxBackoff)
    exit.get

  /** One run of the stage and the Kafka graphs feeding it. */
  private final class Session(
      opened: Stage.Run,
      producers: Producers,
      graphs: Vector[InletGraph.Running]
  ):
    val failed: Future[Throwable] =
      Future.firstCompletedOf(
        opened.failed +: graphs.map(g =>
          g.done.transform(t =>
            scala.util.Success(
              t.fold(
                identity,
                _ => new StreamFailed("an inlet's consumer stopped")
              )
            )
          )
        )
      )

    Future.sequence(graphs.map(_.subscribed)).foreach { _ =>
      if !failed.isCompleted then
        log.info("ready: every inlet is subscribed")
        probes.ready()
    }

    def teardown(): Unit =
      probes.notReady()
      graphs.foreach(_.tearingDown())
      opened.fail(new StreamFailed("the session is being torn down"))
      val shutdowns = graphs.map(_.control.shutdown())
      Try(Await.ready(Future.sequence(shutdowns), 15.seconds))
      Try(Await.ready(producers.close(), 10.seconds)): Unit

    def stop(reason: String): Unit =
      probes.notReady()
      graphs.foreach(_.tearingDown())
      val stops = graphs.map(_.control.stop())
      Try(Await.ready(Future.sequence(stops), 5.seconds))
      opened.stop(reason)
      val shutdowns = graphs.map(_.control.shutdown())
      Try(Await.ready(Future.sequence(shutdowns), 10.seconds))
      Try(Await.ready(producers.close(), 10.seconds)): Unit

  private object Session:
    def start(opened: Stage.Run): Session =
      conversations += 1
      val producers = new Producers(config.outlets)
      val graphs = config.inlets.values.toVector.sortBy(_.name).map { i =>
        new InletGraph(i, opened.processor, producers, stalls, committerSettings).run()
      }
      new Session(opened, producers, graphs)
