package com.thinkmorestupidless.ankka.flow.sdk

import java.net.{InetSocketAddress, SocketAddress}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import java.util.concurrent.{
  CompletableFuture,
  ConcurrentHashMap,
  ExecutorService,
  Executors,
  ThreadFactory,
  TimeUnit
}

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

import ankka.flow.v1.discovery.{DiscoveryGrpc, SidecarInfo, Spec}
import ankka.flow.v1.payload.{Empty, Error, Header, Problems, Record as RecordProto}
import ankka.flow.v1.streamlet.{
  Ack,
  Batch as BatchProto,
  Emit as EmitProto,
  Fail,
  FromProcess,
  InputRecord,
  Start,
  StreamletGrpc,
  ToProcess
}
import com.google.protobuf.ByteString
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory

/**
 * The process side of the protocol: `Discovery` and `Streamlet.Run`, on 127.0.0.1 only. The sidecar
 * in the pod dials `127.0.0.1:$FLOW_PROCESS_PORT`; nothing else can reach the process.
 */
object Serve:
  val DefaultPort     = 9010
  val Loopback        = "127.0.0.1"
  val MaxMessageBytes = 16 * 1024 * 1024

  private val log = LoggerFactory.getLogger("ankka.flow")

  /** The port the platform gives the process: `FLOW_PROCESS_PORT`, else 9010. */
  def port(env: Map[String, String] = sys.env): Int =
    env.get("FLOW_PROCESS_PORT").filter(_.nonEmpty).map(_.trim.toInt).getOrElse(DefaultPort)

  /**
   * Serve `streamlet` on `127.0.0.1:port` (0 for any free port) and return at once. A declaration
   * the protocol refuses is refused here, before anything is bound.
   */
  def start(streamlet: Streamlet, port: Int = 0): Server =
    Streamlet.refuse(Descriptor.validate(streamlet))
    val workers            = Executors.newCachedThreadPool(daemon("batch"))
    val grpcPool           = Executors.newCachedThreadPool(daemon("grpc"))
    given ExecutionContext = ExecutionContext.fromExecutor(grpcPool)
    val servicer           = new Servicer(streamlet, workers)
    val server = NettyServerBuilder
      .forAddress(new InetSocketAddress(Loopback, port))
      .executor(grpcPool)
      .addService(DiscoveryGrpc.bindService(servicer.discovery, summon[ExecutionContext]))
      .addService(StreamletGrpc.bindService(servicer.streamlet, summon[ExecutionContext]))
      .maxInboundMessageSize(MaxMessageBytes)
      .build()
      .start()
    log.info(s"streamlet ${streamlet.name} serving on $Loopback:${server.getPort}")
    new Server(server, Vector(workers, grpcPool))

  /** Serve `streamlet` on `127.0.0.1:$FLOW_PROCESS_PORT` until the process is stopped. */
  def run(streamlet: Streamlet): Unit =
    val server = start(streamlet, port())
    Runtime.getRuntime.addShutdownHook(new Thread(() => server.close(), "flow-shutdown"))
    server.awaitTermination()

  /** `<streamlet class>`: construct the streamlet with no arguments and serve it. */
  def main(args: Array[String]): Unit =
    args.toList match
      case className :: Nil => run(Descriptor.instantiate(className))
      case _ =>
        System.err.println("usage: Serve <streamlet class>")
        sys.exit(2)

  private def daemon(prefix: String): ThreadFactory =
    val n = new AtomicInteger(0)
    r =>
      val t = new Thread(r, s"$prefix-${n.incrementAndGet()}")
      t.setDaemon(true)
      t

  // ── the services ──────────────────────────────────────────────────────────────────────────────

  private final class Servicer(target: Streamlet, workers: ExecutorService):
    private val spec: Spec = Descriptor.spec(target)
    private val current    = new AtomicReference[Conversation | Null](null)

    object discovery extends DiscoveryGrpc.Discovery:
      def discover(request: SidecarInfo): Future[Spec] =
        log.info(
          s"discovery from sidecar ${request.sidecarVersion} (protocol ${request.protocolVersion})"
        )
        Future.successful(spec)

      def reportError(request: Problems): Future[Empty] =
        request.problems.foreach(p => log.error(s"the sidecar refused this process: ${p.message}"))
        Future.successful(Empty())

    object streamlet extends StreamletGrpc.Streamlet:
      def run(out: StreamObserver[FromProcess]): StreamObserver[ToProcess] =
        val conversation = new Conversation(target, workers, out)
        val previous     = current.getAndSet(conversation)
        if previous != null then
          log.info("a new conversation supersedes the previous one")
          previous.end()
        conversation

  /** One `Run` stream. Batches run on worker threads; messages out are sent under one lock. */
  private final class Conversation(
      streamlet: Streamlet,
      workers: ExecutorService,
      out: StreamObserver[FromProcess]
  ) extends StreamObserver[ToProcess]:
    private val ended    = new AtomicBoolean(false)
    private val started  = new AtomicBoolean(false)
    private val inFlight = ConcurrentHashMap.newKeySet[CompletableFuture[Unit]]()

    def end(): Unit =
      if ended.compareAndSet(false, true) then
        out.synchronized {
          try out.onCompleted()
          catch case NonFatal(_) => ()
        }

    private def send(message: FromProcess.Message): Unit =
      if !ended.get then
        out.synchronized {
          if !ended.get then out.onNext(FromProcess(message))
        }

    def onNext(message: ToProcess): Unit =
      if !ended.get then
        message.message match
          case ToProcess.Message.Start(s) => start(s)
          case ToProcess.Message.Batch(b) =>
            if started.get then submit(b)
            else
              log.error("a batch arrived before Start; ending the conversation")
              end()
          case ToProcess.Message.Stop(s) =>
            log.info(s"stop: ${if s.reason.isEmpty then "no reason given" else s.reason}")
            drainThenEnd()
          case ToProcess.Message.Empty => ()

    def onError(t: Throwable): Unit = drainThenEnd()
    def onCompleted(): Unit         = drainThenEnd()

    private def start(s: Start): Unit =
      try
        streamlet.configure(Config.fromJson(s.configJson))
        started.set(true)
        log.info(
          s"conversation ${s.conversationId} started: pipeline ${s.pipeline}, streamlet ${s.streamlet}"
        )
      catch
        case NonFatal(e) =>
          log.error(s"the start's configuration was refused: ${e.getMessage}")
          end()

    private def submit(b: BatchProto): Unit =
      val f = CompletableFuture.runAsync(() => runBatch(b), workers).thenApply(_ => ())
      inFlight.add(f)
      f.whenComplete((_, _) => inFlight.remove(f): Unit): Unit

    private def runBatch(b: BatchProto): Unit =
      val batch = Batch(b.inlet, b.partition, b.records.iterator.map(fromProto).toVector)
      try
        Streamlet.runBatch(streamlet, batch).foreach { e =>
          send(FromProcess.Message.Emit(EmitProto(b.batchId, e.outlet, Some(toProto(e.record)))))
        }
        send(FromProcess.Message.Ack(Ack(b.batchId)))
      catch
        case NonFatal(e) =>
          val message = Option(e.getMessage).filter(_.nonEmpty).getOrElse(e.getClass.getSimpleName)
          log.warn(s"batch ${b.batchId} on ${b.inlet}/${b.partition} failed: $message")
          send(FromProcess.Message.Fail(Fail(b.batchId, Some(Error(message)))))

    private def drainThenEnd(): Unit =
      val pending = inFlight.asScala.toVector
      CompletableFuture
        .allOf(pending*)
        .whenCompleteAsync((_, _) => end(), workers): Unit

  private def fromProto(r: InputRecord): Record =
    val rec = r.getRecord
    Record(
      value = rec.value.toByteArray,
      key = rec.key.map(_.toByteArray),
      headers = rec.headers.map(h => h.key -> h.value.toByteArray),
      offset = r.offset,
      timestampMs = r.timestampMs
    )

  private def toProto(r: Record): RecordProto =
    RecordProto(
      key = r.key.map(ByteString.copyFrom),
      headers = r.headers.map((k, v) => Header(k, ByteString.copyFrom(v))),
      value = ByteString.copyFrom(r.value)
    )

/** A running process: `port` is the bound port; `close()` stops it. */
final class Server private[sdk] (underlying: io.grpc.Server, pools: Vector[ExecutorService]):
  def port: Int                     = underlying.getPort
  def addresses: Seq[SocketAddress] = underlying.getListenSockets.asScala.toVector

  def close(): Unit =
    underlying.shutdown()
    if !underlying.awaitTermination(2, TimeUnit.SECONDS) then underlying.shutdownNow(): Unit
    pools.foreach(_.shutdownNow())

  def awaitTermination(): Unit = underlying.awaitTermination()
