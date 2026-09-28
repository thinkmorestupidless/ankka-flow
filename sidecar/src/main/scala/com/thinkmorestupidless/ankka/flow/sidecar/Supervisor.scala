package com.thinkmorestupidless.ankka.flow.sidecar

import java.util.concurrent.TimeUnit

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.*
import scala.util.Try

import io.grpc.ManagedChannel
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.kafka.CommitterSettings
import org.slf4j.LoggerFactory

/**
 * The sidecar's loop (data-model.md, *Conversation lifecycle*): discover, run, and on any failure
 * tear everything down, back off, and start again from discovery. Never skips; never gives up.
 * Exits only when discovery is refused (1) or it is asked to stop (0).
 */
final class Supervisor(
    settings: Settings,
    descriptor: Descriptor,
    config: StreamletConfig,
    probes: Probes,
    stalls: Stalls
)(using system: ActorSystem):

  private val log = LoggerFactory.getLogger(classOf[Supervisor])

  private given ExecutionContext = system.dispatcher

  private val stopSignal = Promise[Unit]()

  def requestStop(): Unit      = stopSignal.trySuccess(()): Unit
  private def stopped: Boolean = stopSignal.isCompleted

  /** The number of conversations started so far; a test hook. */
  @volatile var conversations: Int = 0

  private val committerSettings =
    CommitAfterWrite
      .defaultCommitterSettings(CommitterSettings(system))
      .withMaxBatch(1000)
      .withMaxInterval(200.millis)

  def newChannel(): ManagedChannel =
    NettyChannelBuilder
      .forAddress(settings.processHost, settings.processPort)
      .usePlaintext()
      .maxInboundMessageSize(2 * Conversation.MaxMessageBytes)
      // No keepalive pings. gRPC servers allow one per five minutes by default and answer more with
      // GOAWAY `too_many_pings`, which failed a quiet conversation every half a minute against a
      // process on default settings. On loopback they buy nothing: a process that dies resets the
      // connection at once, and one that hangs still answers pings from its HTTP/2 layer.
      .build()

  def run(): Int =
    val channel = newChannel()
    try loop(channel)
    finally
      channel.shutdownNow()
      channel.awaitTermination(5, TimeUnit.SECONDS): Unit

  private def loop(channel: ManagedChannel): Int =
    val discovery         = new Discovery(channel, settings, descriptor)
    var backoff           = 500.millis
    var exit: Option[Int] = None
    while exit.isEmpty do
      probes.notReady()
      if stopped then exit = Some(0)
      else
        channel.resetConnectBackoff()
        discovery.run(() => stopped) match
          case None => exit = Some(0)
          case Some(Left(problems)) =>
            log.error("refusing to start: the process does not match the deployed streamlet")
            problems.foreach(p => log.error("  {}", p))
            discovery.reportError(problems)
            exit = Some(1)
          case Some(Right(_)) =>
            val session = Session.start(channel)
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

  /** One conversation and the Kafka graphs feeding it. */
  private final class Session(
      conversation: Conversation,
      producers: Producers,
      graphs: Vector[InletGraph.Running]
  ):
    val failed: Future[Throwable] =
      Future.firstCompletedOf(
        conversation.failed +: graphs.map(g =>
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
      conversation.fail(new StreamFailed("the session is being torn down"))
      val shutdowns = graphs.map(_.control.shutdown())
      Try(Await.ready(Future.sequence(shutdowns), 15.seconds))
      Try(Await.ready(producers.close(), 10.seconds)): Unit

    def stop(reason: String): Unit =
      probes.notReady()
      val stops = graphs.map(_.control.stop())
      Try(Await.ready(Future.sequence(stops), 5.seconds))
      conversation.stop(reason)
      val shutdowns = graphs.map(_.control.shutdown())
      Try(Await.ready(Future.sequence(shutdowns), 10.seconds))
      Try(Await.ready(producers.close(), 10.seconds)): Unit

  private object Session:
    def start(channel: ManagedChannel): Session =
      conversations += 1
      val streamlet = descriptor.streamlet
      val configJson =
        config
          .configJson(streamlet)
          .fold(ps => throw new IllegalStateException(ps.mkString("; ")), identity)
      val conversation = Conversation.open(
        channel,
        config.pipeline,
        config.streamlet,
        configJson,
        config.inlets.values.toVector.sortBy(_.name).map(i => i.name -> i.topic),
        config.outlets.values.toVector.sortBy(_.name).map(o => o.name -> o.topic)
      )
      log.info("conversation {} started", conversation.start.conversationId)
      val producers = new Producers(config.outlets)
      val graphs = config.inlets.values.toVector.sortBy(_.name).map { i =>
        new InletGraph(i, conversation, producers, stalls, committerSettings).run()
      }
      new Session(conversation, producers, graphs)
