package com.thinkmorestupidless.ankka.flow.sidecar

import java.net.InetSocketAddress
import java.util.concurrent.{ConcurrentLinkedQueue, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

import ankka.flow.v1.discovery.{DiscoveryGrpc, SidecarInfo, Spec}
import ankka.flow.v1.payload.{Empty, Error, Header, Problems, Record}
import ankka.flow.v1.streamlet.*
import com.google.protobuf.ByteString
import io.grpc.Server
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import io.grpc.stub.StreamObserver

/**
 * A Scala process speaking the protocol, made scriptable: the only way to prove the conversation
 * before an SDK exists, and afterwards the only way to produce misbehaviour a correct SDK cannot.
 *
 * What it does with each batch is `behaviour(start, batch)`: a list of steps it sends in order.
 * `ProcessDouble.keyed` scripts behaviour from each record's key, which is how the suites drive it.
 */
final class ProcessDouble(
    @volatile var spec: Spec,
    @volatile var behaviour: ProcessDouble.Behaviour = ProcessDouble.keyed()
):
  import ProcessDouble.*

  val received                     = new ConcurrentLinkedQueue[ToProcess]()
  val reportedProblems             = new ConcurrentLinkedQueue[String]()
  val acks                         = new AtomicInteger(0)
  val discoveries                  = new AtomicInteger(0)
  @volatile var onAck: Int => Unit = _ => ()
  @volatile var silent: Boolean    = false

  private val pool                     = Executors.newCachedThreadPool()
  private given ExecutionContext       = ExecutionContext.fromExecutor(pool)
  @volatile private var server: Server = scala.compiletime.uninitialized
  @volatile var port: Int              = 0

  def starts: Vector[Start] =
    received.asScala.toVector.collect { case ToProcess(ToProcess.Message.Start(s), _) => s }

  def batches: Vector[Batch] =
    received.asScala.toVector.collect { case ToProcess(ToProcess.Message.Batch(b), _) => b }

  def stops: Vector[Stop] =
    received.asScala.toVector.collect { case ToProcess(ToProcess.Message.Stop(s), _) => s }

  private object discovery extends DiscoveryGrpc.Discovery:
    def discover(request: SidecarInfo): Future[Spec] =
      discoveries.incrementAndGet()
      Future.successful(spec)
    def reportError(request: Problems): Future[Empty] =
      request.problems.foreach(p => reportedProblems.add(p.message))
      Future.successful(Empty())

  private object streamlet extends StreamletGrpc.Streamlet:
    def run(out: StreamObserver[FromProcess]): StreamObserver[ToProcess] =
      @volatile var start: Start             = Start()
      def send(m: FromProcess.Message): Unit = out.synchronized(out.onNext(FromProcess(m)))
      new StreamObserver[ToProcess]:
        def onNext(msg: ToProcess): Unit =
          received.add(msg)
          msg.message match
            case ToProcess.Message.Start(s) => start = s
            case ToProcess.Message.Batch(b) =>
              if !silent then
                val s = start
                Future {
                  behaviour(s, b).foreach {
                    case Step.Emit(outlet, record) =>
                      send(FromProcess.Message.Emit(Emit(b.batchId, outlet, Some(record))))
                    case Step.EmitFor(id, outlet, record) =>
                      send(FromProcess.Message.Emit(Emit(id, outlet, Some(record))))
                    case Step.Ack        => send(FromProcess.Message.Ack(Ack(b.batchId)))
                    case Step.AckFor(id) => send(FromProcess.Message.Ack(Ack(id)))
                    case Step.Fail(m) =>
                      send(FromProcess.Message.Fail(Fail(b.batchId, Some(Error(m)))))
                    case Step.Sleep(ms) => Thread.sleep(ms)
                  }
                  if behaviour(s, b).contains(Step.Ack) then onAck(acks.incrementAndGet())
                }: Unit
            case ToProcess.Message.Stop(_) => out.synchronized(out.onCompleted())
            case ToProcess.Message.Empty   => ()
        def onError(t: Throwable): Unit = ()
        def onCompleted(): Unit         = ()

  /** Starts on `port` (0: any free one) on loopback, and returns the port. */
  def start(port: Int = 0): Int =
    server = NettyServerBuilder
      .forAddress(new InetSocketAddress("127.0.0.1", port))
      .addService(DiscoveryGrpc.bindService(discovery, summon[ExecutionContext]))
      .addService(StreamletGrpc.bindService(streamlet, summon[ExecutionContext]))
      .maxInboundMessageSize(16 * 1024 * 1024)
      .build()
      .start()
    this.port = server.getPort
    this.port

  /** Kills every connection, as a process dying does. */
  def stop(): Unit =
    Option(server).foreach { s =>
      s.shutdownNow()
      s.awaitTermination(5, TimeUnit.SECONDS): Unit
    }

  def restart(): Unit =
    stop()
    start(port): Unit

  def close(): Unit =
    stop()
    pool.shutdownNow(): Unit

object ProcessDouble:

  type Behaviour = (Start, Batch) => Vector[Step]

  enum Step:
    case Emit(outlet: String, record: Record)
    case EmitFor(batchId: Long, outlet: String, record: Record)
    case Ack
    case AckFor(batchId: Long)
    case Fail(message: String)
    case Sleep(millis: Long)

  def key(r: InputRecord): String = r.getRecord.key.fold("")(_.toStringUtf8)

  /**
   * Behaviour from each record's key, as the conformance reference: `echo`, `fan`, `skip`, `fail`,
   * `late`, `rogue-outlet`, `double-ack`, `emit-after-ack`, `unknown-batch`, `multiply`, `unkeyed`,
   * `header-echo`. Anything else is routed by `route` (default: to `out`).
   */
  def keyed(route: InputRecord => Vector[String] = _ => Vector("out")): Behaviour =
    (start, batch) =>
      val factor =
        """"factor":(\d+)""".r.findFirstMatchIn(start.configJson).map(_.group(1).toInt).getOrElse(1)
      val steps = batch.records.toVector.flatMap { r =>
        val rec = r.getRecord
        key(r) match
          case "echo"           => Vector(Step.Emit("out", rec))
          case "fan"            => Vector(Step.Emit("out", rec), Step.Emit("other", rec))
          case "skip"           => Vector.empty
          case "fail"           => Vector(Step.Fail("the record said fail"))
          case "late"           => Vector(Step.Sleep(300), Step.Emit("out", rec))
          case "rogue-outlet"   => Vector(Step.Emit("nope", rec))
          case "double-ack"     => Vector(Step.AckFor(batch.batchId))
          case "emit-after-ack" => Vector(Step.AckFor(batch.batchId), Step.Emit("out", rec))
          case "unknown-batch"  => Vector(Step.AckFor(batch.batchId + 1000))
          case "multiply" =>
            (1 to factor).toVector.map(i =>
              Step.Emit(
                "out",
                rec.withHeaders(rec.headers :+ Header("n", ByteString.copyFromUtf8(i.toString)))
              )
            )
          case "unkeyed"     => Vector(Step.Emit("out", rec.clearKey))
          case "header-echo" => Vector(Step.Emit("out", rec.withHeaders(rec.headers.reverse)))
          case _             => route(r).map(o => Step.Emit(o, rec))
      }
      if steps.exists(_.isInstanceOf[Step.Fail]) then
        steps.filter(_.isInstanceOf[Step.Fail]).take(1)
      else if steps.contains(Step.AckFor(batch.batchId)) && !steps.exists(_.isInstanceOf[Step.Emit])
      then steps :+ Step.Ack // double-ack: the scripted AckFor, then the normal one
      else steps :+ Step.Ack
