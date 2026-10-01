package com.thinkmorestupidless.ankka.flow.sidecar

import java.util.concurrent.TimeUnit

import scala.concurrent.Future

import io.grpc.ManagedChannel
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import org.slf4j.LoggerFactory

/**
 * What the supervisor's loop runs batches through. A streamlet's process behind the protocol is one
 * stage; a stage built into the sidecar (feature 002) is another. The Kafka side — inlet graphs,
 * commit after the write, stalls, readiness — is the same for both.
 */
trait Stage:

  /**
   * Blocks until the stage can take batches, retrying what a retry can fix, or `stopped` returns
   * true (`None`). `Left` is a refusal no retry can fix, with the exit code the sidecar ends with.
   */
  def open(stopped: () => Boolean): Option[Either[Stage.Refusal, Stage.Run]]

  /** Releases whatever `open` acquired; called once, when the sidecar stops. */
  def close(): Unit

object Stage:

  final case class Refusal(problems: Vector[String], exitCode: Int)

  /** One opened run of a stage: the processor every inlet graph feeds, until it fails or stops. */
  trait Run:
    def processor: BatchProcessor

    /** Completes when the run fails on its own, apart from any batch. */
    def failed: Future[Throwable]

    /** Fails the run: every batch in flight fails, nothing more is processed. */
    def fail(cause: Throwable): Unit

    /** Ends the run cleanly after what is in flight. */
    def stop(reason: String): Unit

/**
 * The streamlet's process, behind the protocol: discovery until it answers and matches the deployed
 * descriptor, then one `Run` conversation per opened run.
 */
final class ProcessStage(settings: Settings, deployed: Descriptor, config: StreamletConfig)
    extends Stage:

  private val log = LoggerFactory.getLogger(classOf[ProcessStage])

  val channel: ManagedChannel =
    NettyChannelBuilder
      .forAddress(settings.processHost, settings.processPort)
      .usePlaintext()
      .maxInboundMessageSize(2 * Conversation.MaxMessageBytes)
      // No keepalive pings. gRPC servers allow one per five minutes by default and answer more with
      // GOAWAY `too_many_pings`, which failed a quiet conversation every half a minute against a
      // process on default settings. On loopback they buy nothing: a process that dies resets the
      // connection at once, and one that hangs still answers pings from its HTTP/2 layer.
      .build()

  private val discovery = new Discovery(channel, settings, deployed)

  def open(stopped: () => Boolean): Option[Either[Stage.Refusal, Stage.Run]] =
    channel.resetConnectBackoff()
    discovery.run(stopped).map {
      case Left(problems) =>
        log.error("refusing to start: the process does not match the deployed streamlet")
        problems.foreach(p => log.error("  {}", p))
        discovery.reportError(problems)
        Left(Stage.Refusal(problems, 1))
      case Right(_) =>
        val configJson =
          config
            .configJson(deployed.streamlet)
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
        Right(new Stage.Run:
          def processor: BatchProcessor    = conversation
          def failed: Future[Throwable]    = conversation.failed
          def fail(cause: Throwable): Unit = conversation.fail(cause)
          def stop(reason: String): Unit   = conversation.stop(reason))
    }

  def close(): Unit =
    channel.shutdownNow()
    channel.awaitTermination(5, TimeUnit.SECONDS): Unit
